#include "nats_source.hpp"

#include <algorithm>

namespace duckdb {

NatsJetStreamBatchSource::NatsJetStreamBatchSource(natsSubscription *subscription) : subscription_(subscription) {
    if (subscription_ == nullptr) {
        throw std::runtime_error("JetStream batch source requires a subscription");
    }
}

NatsJetStreamBatchSource::~NatsJetStreamBatchSource() {
    ClearBatch();
}

void NatsJetStreamBatchSource::ClearBatch() {
    natsMsgList_Destroy(&messages_);
    messages_ = {nullptr, 0};
    next_index_ = 0;
}

bool NatsJetStreamBatchSource::Fetch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name) {
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
    request.NoWait = true;

    status = natsSubscription_FetchRequest(&messages_, subscription_, &request);
    if (status == NATS_TIMEOUT || status == NATS_NOT_FOUND) {
        ClearBatch();
        return false;
    }
    if (status != NATS_OK) {
        throw std::runtime_error(std::string("Failed to fetch JetStream message batch from '") +
                                 stream_name + "': " + natsStatus_GetText(status));
    }
    if (messages_.Count == 0) {
        ClearBatch();
        return false;
    }
    return true;
}

bool NatsJetStreamBatchSource::Next(natsMsg **message, uint64_t batch_size, int64_t fetch_timeout_ms,
                                    const string &stream_name) {
    if (message == nullptr) {
        throw std::runtime_error("JetStream batch source requires a message output pointer");
    }
    *message = nullptr;

    if (next_index_ >= messages_.Count) {
        ClearBatch();
        if (!Fetch(batch_size, fetch_timeout_ms, stream_name)) {
            return false;
        }
    }

    *message = messages_.Msgs[next_index_];
    messages_.Msgs[next_index_] = nullptr;
    next_index_++;
    return *message != nullptr;
}

} // namespace duckdb
