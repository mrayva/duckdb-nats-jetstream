#pragma once

#include "duckdb.hpp"
#include <nats/nats.h>

namespace duckdb {

// Caches the message metadata and data pointers while the caller retains the
// underlying natsMsg. It does not own or acknowledge the message.
class NatsMessageEnvelope {
public:
    explicit NatsMessageEnvelope(natsMsg *message);

    natsMsg *Message() const;
    const char *Subject() const;
    uint64_t Sequence() const;
    int64_t TimestampNs() const;
    const char *Data() const;
    int DataLength() const;

private:
    void LoadMetadata() const;

    natsMsg *message_ = nullptr;
    const char *subject_ = nullptr;
    const char *data_ = nullptr;
    int data_length_ = 0;
    mutable uint64_t sequence_ = 0;
    mutable int64_t timestamp_ns_ = 0;
    mutable bool metadata_loaded_ = false;
};

// Owns one buffered JetStream pull batch and transfers individual messages to
// the caller. The caller owns a returned message and must destroy it.
class NatsJetStreamBatchSource {
public:
    explicit NatsJetStreamBatchSource(natsSubscription *subscription);
    ~NatsJetStreamBatchSource();

    NatsJetStreamBatchSource(const NatsJetStreamBatchSource &) = delete;
    NatsJetStreamBatchSource &operator=(const NatsJetStreamBatchSource &) = delete;

    // Fetches one complete batch. Existing buffered messages are discarded.
    bool FetchBatch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name, bool no_wait = true);
    bool HasBufferedMessages() const;

    // Returns false when the source is exhausted for this bounded read.
    bool Next(natsMsg **message, uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name);

private:
    bool Fetch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name, bool no_wait);
    void ClearBatch();

    natsSubscription *subscription_ = nullptr;
    natsMsgList messages_ {nullptr, 0};
    int next_index_ = 0;
};

} // namespace duckdb
