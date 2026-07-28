#include "nats_source.hpp"

#include <algorithm>
#include <chrono>

namespace duckdb {

NatsMessageEnvelope::NatsMessageEnvelope(natsMsg *message) : message_(message) {
    if (message_ == nullptr) {
        throw std::runtime_error("Message envelope requires a message");
    }

    subject_ = natsMsg_GetSubject(message_);
    data_ = natsMsg_GetData(message_);
    data_length_ = natsMsg_GetDataLength(message_);
    sequence_ = natsMsg_GetSequence(message_);
}

void NatsMessageEnvelope::LoadMetadata() const {
    if (metadata_loaded_) {
        return;
    }
    metadata_loaded_ = true;
    jsMsgMetaData *metadata = nullptr;
    if (natsMsg_GetMetaData(&metadata, message_) == NATS_OK && metadata != nullptr) {
        if (sequence_ == 0) {
            sequence_ = metadata->Sequence.Stream;
        }
        timestamp_ns_ = metadata->Timestamp;
        jsMsgMetaData_Destroy(metadata);
    }
    if (timestamp_ns_ == 0) {
        timestamp_ns_ = natsMsg_GetTime(message_);
    }
}

natsMsg *NatsMessageEnvelope::Message() const {
    return message_;
}

const char *NatsMessageEnvelope::Subject() const {
    return subject_;
}

uint64_t NatsMessageEnvelope::Sequence() const {
    if (sequence_ == 0) {
        LoadMetadata();
    }
    return sequence_;
}

int64_t NatsMessageEnvelope::TimestampNs() const {
    LoadMetadata();
    return timestamp_ns_;
}

const char *NatsMessageEnvelope::Data() const {
    return data_;
}

int NatsMessageEnvelope::DataLength() const {
    return data_length_;
}

NatsJetStreamBatchSource::NatsJetStreamBatchSource(natsSubscription *subscription) : subscription_(subscription) {
    if (subscription_ == nullptr) {
        throw std::runtime_error("JetStream batch source requires a subscription");
    }
}

NatsJetStreamBatchSource::~NatsJetStreamBatchSource() {
    if (prefetch_thread_.joinable()) {
        {
            std::lock_guard<std::mutex> guard(prefetch_mutex_);
            prefetch_stop_ = true;
        }
        prefetch_cv_.notify_all();
        prefetch_thread_.join();
    }
    ClearBatch();
    for (auto &batch : prefetch_batches_) {
        natsMsgList_Destroy(&batch);
    }
    prefetch_batches_.clear();
}

void NatsJetStreamBatchSource::ClearBatch() {
    natsMsgList_Destroy(&messages_);
    messages_ = {nullptr, 0};
    next_index_ = 0;
}

void NatsJetStreamBatchSource::StartPrefetch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name) {
    if (batch_size == 0) {
        throw std::runtime_error("JetStream batch source requires a non-zero prefetch batch size");
    }
    if (fetch_timeout_ms < 1) {
        throw std::runtime_error("JetStream batch source requires a positive prefetch timeout");
    }
    if (prefetch_thread_.joinable()) {
        throw std::runtime_error("JetStream batch source prefetch is already running");
    }

    prefetch_batch_size_ = batch_size;
    prefetch_timeout_ms_ = fetch_timeout_ms;
    prefetch_stream_name_ = stream_name;
    prefetch_enabled_ = true;
    prefetch_stop_ = false;
    prefetch_thread_ = std::thread([this]() { PrefetchLoop(); });
}

bool NatsJetStreamBatchSource::Fetch(natsMsgList &target, uint64_t batch_size, int64_t fetch_timeout_ms,
                                     const string &stream_name, bool no_wait) {
    if (batch_size == 0) {
        throw std::runtime_error("JetStream batch source requires a non-zero batch size");
    }
    if (fetch_timeout_ms < 1) {
        throw std::runtime_error("JetStream batch source requires a positive fetch timeout");
    }

    jsFetchRequest request;
    natsStatus status = jsFetchRequest_Init(&request);
    if (status != NATS_OK) {
        throw std::runtime_error(std::string("Failed to initialize JetStream fetch request: ") +
                                 natsStatus_GetText(status));
    }

    request.Batch = static_cast<int>(std::min<uint64_t>(batch_size, 65536));
    request.Expires = fetch_timeout_ms * 1000LL * 1000LL;
    request.NoWait = no_wait;

    status = natsSubscription_FetchRequest(&target, subscription_, &request);
    if (status == NATS_TIMEOUT || status == NATS_NOT_FOUND) {
        natsMsgList_Destroy(&target);
        target = {nullptr, 0};
        return false;
    }
    if (status != NATS_OK) {
        throw std::runtime_error(std::string("Failed to fetch JetStream message batch from '") +
                                 stream_name + "': " + natsStatus_GetText(status));
    }
    if (target.Count == 0) {
        natsMsgList_Destroy(&target);
        target = {nullptr, 0};
        return false;
    }
    return true;
}

bool NatsJetStreamBatchSource::FetchBatch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name,
                                          bool no_wait) {
    ClearBatch();
    if (!prefetch_enabled_) {
        return Fetch(messages_, batch_size, fetch_timeout_ms, stream_name, no_wait);
    }

    std::unique_lock<std::mutex> lock(prefetch_mutex_);
    prefetch_cv_.wait_for(lock, std::chrono::milliseconds(fetch_timeout_ms), [&]() {
        return !prefetch_batches_.empty() || prefetch_error_ != nullptr || prefetch_stop_;
    });
    if (prefetch_error_) {
        std::rethrow_exception(prefetch_error_);
    }
    if (prefetch_batches_.empty()) {
        return false;
    }
    messages_ = prefetch_batches_.front();
    prefetch_batches_.pop_front();
    next_index_ = 0;
    lock.unlock();
    prefetch_cv_.notify_all();
    return true;
}

bool NatsJetStreamBatchSource::HasBufferedMessages() const {
    return next_index_ < messages_.Count;
}

bool NatsJetStreamBatchSource::Next(natsMsg **message, uint64_t batch_size, int64_t fetch_timeout_ms,
                                    const string &stream_name) {
    if (message == nullptr) {
        throw std::runtime_error("JetStream batch source requires a message output pointer");
    }
    *message = nullptr;

    if (next_index_ >= messages_.Count) {
        ClearBatch();
        if (!Fetch(messages_, batch_size, fetch_timeout_ms, stream_name, true)) {
            return false;
        }
    }

    *message = messages_.Msgs[next_index_];
    messages_.Msgs[next_index_] = nullptr;
    next_index_++;
    return *message != nullptr;
}

void NatsJetStreamBatchSource::PrefetchLoop() {
    while (true) {
        {
            std::unique_lock<std::mutex> lock(prefetch_mutex_);
            prefetch_cv_.wait(lock, [&]() { return prefetch_stop_ || prefetch_batches_.empty(); });
            if (prefetch_stop_) {
                return;
            }
        }

        natsMsgList batch {nullptr, 0};
        bool fetched = false;
        try {
            fetched = Fetch(batch, prefetch_batch_size_, prefetch_timeout_ms_, prefetch_stream_name_, false);
        } catch (...) {
            std::lock_guard<std::mutex> guard(prefetch_mutex_);
            prefetch_error_ = std::current_exception();
            prefetch_cv_.notify_all();
            return;
        }
        if (!fetched) {
            continue;
        }

        std::lock_guard<std::mutex> guard(prefetch_mutex_);
        if (prefetch_stop_) {
            natsMsgList_Destroy(&batch);
            return;
        }
        prefetch_batches_.push_back(batch);
        prefetch_cv_.notify_all();
    }
}

} // namespace duckdb
