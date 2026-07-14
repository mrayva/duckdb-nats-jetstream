PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=nats_js
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# DuckDB v1.5.x's all-target release build links plan_serializer when BUILD_SHELL
# is enabled. The extension release artifact does not need that tool.
BUILD_SHELL ?= 0
override EXT_FLAGS += -DBUILD_SHELL=$(BUILD_SHELL)

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Custom targets for NATS JetStream testing
.PHONY: start stop setup-streams generate-data

start:
	@echo "Starting NATS JetStream..."
	docker-compose up -d
	@echo "Waiting for NATS to be healthy..."
	@sleep 5
	@docker-compose ps

stop:
	@echo "Stopping NATS JetStream..."
	docker-compose down

setup-streams: start
	@echo "Creating JetStream streams..."
	./scripts/setup-streams.sh

generate-data: setup-streams
	@echo "Generating synthetic telemetry data..."
	./scripts/generate-telemetry.py
