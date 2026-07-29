#pragma once

#include "duckdb.hpp"
#include "duckdb/function/table_function.hpp"
#include <nats/nats.h>

namespace duckdb {

struct NatsConnectionConfig {
    string url = "nats://localhost:4222";
    string credentials_file;
    string tls_ca_file;
    string tls_cert_file;
    string tls_key_file;
    string tls_server_name;
    bool tls_skip_verify = false;
};

struct NatsConnectionCallbacks {
    natsConnectionHandler disconnected = nullptr;
    natsConnectionHandler reconnected = nullptr;
    natsConnectionHandler closed = nullptr;
    void *closure = nullptr;
};

bool ParseNatsConnectionParameter(NatsConnectionConfig &config, const string &name, const Value &value);
void ValidateNatsConnectionConfig(const NatsConnectionConfig &config);
void RegisterNatsConnectionParameters(TableFunction &function);
void ConfigureNatsOptions(natsOptions *options, const NatsConnectionConfig &config,
                          const NatsConnectionCallbacks *callbacks = nullptr);
void ConnectNats(const NatsConnectionConfig &config, natsConnection **connection, idx_t max_attempts = 1,
                 const NatsConnectionCallbacks *callbacks = nullptr);
void ConnectJetStream(const NatsConnectionConfig &config, natsConnection **connection, jsCtx **jetstream);

} // namespace duckdb
