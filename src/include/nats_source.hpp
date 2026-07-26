#pragma once

#include "duckdb.hpp"
#include <nats/nats.h>

namespace duckdb {

// Owns one buffered JetStream pull batch and transfers individual messages to
// the caller. The caller owns a returned message and must destroy it.
class NatsJetStreamBatchSource {
public:
    explicit NatsJetStreamBatchSource(natsSubscription *subscription);
    ~NatsJetStreamBatchSource();

    NatsJetStreamBatchSource(const NatsJetStreamBatchSource &) = delete;
    NatsJetStreamBatchSource &operator=(const NatsJetStreamBatchSource &) = delete;

    // Returns false when the source is exhausted for this bounded read.
    bool Next(natsMsg **message, uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name);

private:
    bool Fetch(uint64_t batch_size, int64_t fetch_timeout_ms, const string &stream_name);
    void ClearBatch();

    natsSubscription *subscription_ = nullptr;
    natsMsgList messages_ {nullptr, 0};
    int next_index_ = 0;
};

} // namespace duckdb
