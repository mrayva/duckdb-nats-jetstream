#include "nats_connection.hpp"
#include <chrono>
#include <filesystem>
#include <thread>

namespace duckdb {

static void CheckNatsStatus(natsStatus status, const string &action) {
    if (status != NATS_OK) {
        throw std::runtime_error(action + ": " + natsStatus_GetText(status));
    }
}

bool ParseNatsConnectionParameter(NatsConnectionConfig &config, const string &name, const Value &value) {
    if (name == "url") {
        config.url = StringValue::Get(value);
    } else if (name == "credentials_file") {
        config.credentials_file = StringValue::Get(value);
    } else if (name == "tls_ca_file") {
        config.tls_ca_file = StringValue::Get(value);
    } else if (name == "tls_cert_file") {
        config.tls_cert_file = StringValue::Get(value);
    } else if (name == "tls_key_file") {
        config.tls_key_file = StringValue::Get(value);
    } else if (name == "tls_server_name") {
        config.tls_server_name = StringValue::Get(value);
    } else if (name == "tls_skip_verify") {
        config.tls_skip_verify = BooleanValue::Get(value);
    } else {
        return false;
    }
    return true;
}

void ValidateNatsConnectionConfig(const NatsConnectionConfig &config) {
    if (config.url.empty()) {
        throw std::runtime_error("url must not be empty");
    }
    if (config.tls_cert_file.empty() != config.tls_key_file.empty()) {
        throw std::runtime_error("tls_cert_file and tls_key_file must be specified together");
    }
    for (const auto &file : {pair<const char *, const string *>("credentials_file", &config.credentials_file),
                             pair<const char *, const string *>("tls_ca_file", &config.tls_ca_file),
                             pair<const char *, const string *>("tls_cert_file", &config.tls_cert_file),
                             pair<const char *, const string *>("tls_key_file", &config.tls_key_file)}) {
        if (!file.second->empty() && !std::filesystem::exists(*file.second)) {
            throw std::runtime_error(string(file.first) + " does not exist: " + *file.second);
        }
    }
}

void RegisterNatsConnectionParameters(TableFunction &function) {
    function.named_parameters["url"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["credentials_file"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["tls_ca_file"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["tls_cert_file"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["tls_key_file"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["tls_server_name"] = LogicalType(LogicalTypeId::VARCHAR);
    function.named_parameters["tls_skip_verify"] = LogicalType(LogicalTypeId::BOOLEAN);
}

void ConfigureNatsOptions(natsOptions *options, const NatsConnectionConfig &config,
                          const NatsConnectionCallbacks *callbacks) {
    ValidateNatsConnectionConfig(config);
    CheckNatsStatus(natsOptions_SetTimeout(options, 5000), "Failed to set NATS timeout");
    CheckNatsStatus(natsOptions_SetURL(options, config.url.c_str()), "Failed to set NATS URL");
    CheckNatsStatus(natsOptions_SetAllowReconnect(options, true), "Failed to enable NATS reconnect");
    CheckNatsStatus(natsOptions_SetMaxReconnect(options, 600), "Failed to set NATS reconnect limit");
    CheckNatsStatus(natsOptions_SetReconnectWait(options, 1000), "Failed to set NATS reconnect wait");

    if (callbacks != nullptr) {
        CheckNatsStatus(natsOptions_SetDisconnectedCB(options, callbacks->disconnected, callbacks->closure),
                       "Failed to set NATS disconnected callback");
        CheckNatsStatus(natsOptions_SetReconnectedCB(options, callbacks->reconnected, callbacks->closure),
                       "Failed to set NATS reconnected callback");
        CheckNatsStatus(natsOptions_SetClosedCB(options, callbacks->closed, callbacks->closure),
                       "Failed to set NATS closed callback");
    }

    if (!config.credentials_file.empty()) {
        CheckNatsStatus(natsOptions_SetUserCredentialsFromFiles(options, config.credentials_file.c_str(), nullptr),
                        "Failed to load NATS credentials file");
    }

    bool use_tls = !config.tls_ca_file.empty() || !config.tls_cert_file.empty() ||
                   !config.tls_server_name.empty() || config.tls_skip_verify;
    if (!use_tls) {
        return;
    }
    CheckNatsStatus(natsOptions_SetSecure(options, true), "Failed to enable NATS TLS");
    if (!config.tls_ca_file.empty()) {
        CheckNatsStatus(natsOptions_LoadCATrustedCertificates(options, config.tls_ca_file.c_str()),
                        "Failed to load NATS TLS CA file");
    }
    if (!config.tls_cert_file.empty()) {
        CheckNatsStatus(natsOptions_LoadCertificatesChain(options, config.tls_cert_file.c_str(),
                                                          config.tls_key_file.c_str()),
                        "Failed to load NATS TLS client certificate");
    }
    if (!config.tls_server_name.empty()) {
        CheckNatsStatus(natsOptions_SetExpectedHostname(options, config.tls_server_name.c_str()),
                        "Failed to set NATS TLS server name");
    }
    if (config.tls_skip_verify) {
        CheckNatsStatus(natsOptions_SkipServerVerification(options, true),
                        "Failed to disable NATS TLS server verification");
    }
}

void ConnectNats(const NatsConnectionConfig &config, natsConnection **connection, idx_t max_attempts,
                 const NatsConnectionCallbacks *callbacks) {
    natsStatus last_status = NATS_OK;
    for (idx_t attempt = 0; attempt < max_attempts; attempt++) {
        natsOptions *options = nullptr;
        CheckNatsStatus(natsOptions_Create(&options), "Failed to create NATS options");
        try {
            ConfigureNatsOptions(options, config, callbacks);
            last_status = natsConnection_Connect(connection, options);
            natsOptions_Destroy(options);
        } catch (...) {
            natsOptions_Destroy(options);
            throw;
        }
        if (last_status == NATS_OK) {
            return;
        }
        if (attempt + 1 < max_attempts) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
        }
    }
    throw std::runtime_error(string("Failed to connect to NATS: ") + natsStatus_GetText(last_status));
}

void ConnectJetStream(const NatsConnectionConfig &config, natsConnection **connection, jsCtx **jetstream,
                      idx_t max_attempts, const NatsConnectionCallbacks *callbacks) {
    ConnectNats(config, connection, max_attempts, callbacks);
    auto status = natsConnection_JetStream(jetstream, *connection, nullptr);
    if (status != NATS_OK) {
        natsConnection_Destroy(*connection);
        *connection = nullptr;
        throw std::runtime_error(string("Failed to create JetStream context: ") + natsStatus_GetText(status));
    }
}

} // namespace duckdb
