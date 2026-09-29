#!/usr/bin/env python3

from __future__ import annotations

import argparse
import os
import subprocess
import tempfile
from pathlib import Path

from google.protobuf import descriptor_pb2

from duckdb_session import DuckDBBridge, find_duckdb_lib


def run_protoc(proto_dir: Path, output: Path, *inputs: str, include_imports: bool = True) -> None:
    command = ["protoc", f"-I{proto_dir}"]
    if include_imports:
        command.append("--include_imports")
    command.extend([f"--descriptor_set_out={output}", *inputs])
    subprocess.run(command, check=True, cwd=proto_dir)


def expect_error(bridge: DuckDBBridge, sql: str, expected: str) -> None:
    try:
        bridge.execute(sql)
    except Exception as exc:
        if expected not in str(exc):
            raise AssertionError(f"Expected error containing {expected!r}, got: {exc}") from exc
        return
    raise AssertionError(f"Expected query to fail containing {expected!r}: {sql}")


def main() -> int:
    parser = argparse.ArgumentParser(description="NATS-independent protobuf descriptor-set tests")
    parser.add_argument("--duckdb-bin", default="")
    parser.add_argument("--extension", required=True)
    parser.add_argument("--duckdb-lib", default="")
    args = parser.parse_args()

    if args.duckdb_lib:
        duckdb_lib = args.duckdb_lib
    elif args.duckdb_bin:
        duckdb_lib = find_duckdb_lib(args.duckdb_bin)
    else:
        parser.error("provide either --duckdb-lib or --duckdb-bin")
    with tempfile.TemporaryDirectory(prefix="nats-protoset-tests-") as tmp:
        root = Path(tmp)
        proto_dir = root / "proto"
        proto_dir.mkdir()
        (proto_dir / "dep.proto").write_text(
            'syntax = "proto3"; package descriptor_test; message Child { string label = 1; }\n', encoding="utf-8"
        )
        (proto_dir / "root.proto").write_text(
            'syntax = "proto3"; package descriptor_test; import "dep.proto"; '
            'message Envelope { Child child = 1; string previous = 2; }\n', encoding="utf-8"
        )
        (proto_dir / "timestamp.proto").write_text(
            'syntax = "proto3"; package descriptor_test; import "google/protobuf/timestamp.proto"; '
            'message Timed { google.protobuf.Timestamp created = 1; }\n', encoding="utf-8"
        )

        ordered = root / "ordered.protoset"
        run_protoc(proto_dir, ordered, "root.proto", "dep.proto")
        file_set = descriptor_pb2.FileDescriptorSet()
        file_set.ParseFromString(ordered.read_bytes())
        files = list(file_set.file)
        file_set.ClearField("file")
        file_set.file.extend(reversed(files))
        ordered.write_bytes(file_set.SerializeToString())

        timestamp = root / "timestamp.protoset"
        run_protoc(proto_dir, timestamp, "timestamp.proto")
        missing_dependency = root / "missing-dependency.protoset"
        run_protoc(proto_dir, missing_dependency, "root.proto", include_imports=False)
        malformed = root / "malformed.protoset"
        malformed.write_bytes(b"not a protobuf descriptor set")
        empty = root / "empty.protoset"
        empty.write_bytes(b"")

        bridge = DuckDBBridge(duckdb_lib, ":memory:")
        try:
            bridge.execute(f"LOAD '{Path(args.extension).resolve()}';")
            scan = lambda path, message="descriptor_test.Envelope", fields="['child.label']": (
                "DESCRIBE SELECT * FROM nats_scan('unused', "
                f"proto_descriptor_set := '{path}', proto_message := '{message}', proto_extract := {fields})"
            )

            # Repeated binding exercises the cache-hit path; the reversed set proves
            # dependency construction does not rely on FileDescriptorSet order.
            bridge.execute(scan(ordered))
            bridge.execute(scan(ordered))
            bridge.execute(scan(timestamp, "descriptor_test.Timed", "['created.seconds']"))

            expect_error(bridge, scan(malformed), "Failed to parse protobuf descriptor set")
            expect_error(bridge, scan(empty), "Message type 'descriptor_test.Envelope' not found")
            expect_error(bridge, scan(missing_dependency), "unresolved or cyclic file dependencies")
            expect_error(bridge, scan(ordered, "descriptor_test.Missing"), "not found in descriptor set")

            # Replace a cached schema at the same path and force a newer mtime.
            revised_proto = proto_dir / "revised.proto"
            revised_proto.write_text(
                'syntax = "proto3"; package descriptor_test; message Envelope { int64 current = 1; }\n',
                encoding="utf-8",
            )
            revised = root / "revised.protoset"
            run_protoc(proto_dir, revised, "revised.proto")
            replacement_path = root / "replace.protoset"
            replacement_path.write_bytes(ordered.read_bytes())
            bridge.execute(scan(replacement_path))
            cached_stat = replacement_path.stat()
            replacement_path.write_bytes(b"malformed after initial bind")
            os.utime(replacement_path, ns=(cached_stat.st_atime_ns, cached_stat.st_mtime_ns))
            bridge.execute(scan(replacement_path))
            replacement_path.write_bytes(revised.read_bytes())
            old_time = replacement_path.stat().st_mtime
            os.utime(replacement_path, (old_time + 5, old_time + 5))
            bridge.execute(scan(replacement_path, fields="['current']"))
            expect_error(bridge, scan(replacement_path, fields="['child.label']"), "Field 'child'")

            # API-level validation is bind-time and therefore needs no running NATS server.
            expect_error(
                bridge,
                "SELECT * FROM nats_scan('unused', proto_extract := ['x'], proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_scan('unused', proto_extract := ['x'], proto_file := 'x.proto', "
                "proto_descriptor_set := 'x.protoset', proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_scan('unused', proto_extract := ['x'], proto_descriptor_set := 'x.protoset')",
                "proto_message parameter is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_ingest(job_name := 'test', stream_name := 's', target_table := 't', "
                "durable_name := 'd', proto_extract := ['x'], proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_ingest(job_name := 'test', stream_name := 's', target_table := 't', "
                "durable_name := 'd', proto_extract := ['x'], proto_file := 'x.proto', "
                "proto_descriptor_set := 'x.protoset', proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_ingest(job_name := 'test', stream_name := 's', target_table := 't', "
                "durable_name := 'd', proto_extract := ['x'], proto_descriptor_set := 'x.protoset')",
                "proto_message parameter is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_subscribe(job_name := 'test', target_table := 't', subject := 's', "
                "proto_extract := ['x'], proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_subscribe(job_name := 'test', target_table := 't', subject := 's', "
                "proto_extract := ['x'], proto_file := 'x.proto', proto_descriptor_set := 'x.protoset', "
                "proto_message := 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "SELECT * FROM nats_start_subscribe(job_name := 'test', target_table := 't', subject := 's', "
                "proto_extract := ['x'], proto_descriptor_set := 'x.protoset')",
                "proto_message parameter is required",
            )
            expect_error(
                bridge,
                "COPY (SELECT 's' AS subject, 'v' AS value) TO 'unused' (FORMAT nats_js, payload_format 'protobuf', "
                "url 'nats://127.0.0.1:4222', proto_message 'descriptor_test.Envelope')",
                "exactly one of proto_file or proto_descriptor_set",
            )
            expect_error(
                bridge,
                "COPY (SELECT 's' AS subject, 'v' AS value) TO 'unused' (FORMAT nats_js, payload_format 'protobuf', "
                "url 'nats://127.0.0.1:4222', proto_file 'root.proto', proto_descriptor_set 'root.protoset', "
                "proto_message 'descriptor_test.Envelope')",
                "exactly one of proto_file or proto_descriptor_set",
            )
            expect_error(
                bridge,
                "COPY (SELECT 's' AS subject, 'v' AS value) TO 'unused' (FORMAT nats_js, payload_format 'protobuf', "
                "url 'nats://127.0.0.1:4222', proto_descriptor_set 'root.protoset')",
                "proto_message",
            )
            bridge.execute("CREATE TABLE descriptor_validation(payload BLOB, subject VARCHAR)")
            expect_error(
                bridge,
                "COPY descriptor_validation FROM 'unused' (FORMAT nats_js, proto_extract ['x'], proto_message 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "COPY descriptor_validation FROM 'unused' (FORMAT nats_js, proto_extract ['x'], "
                "proto_file 'root.proto', proto_descriptor_set 'root.protoset', proto_message 'X')",
                "Exactly one of proto_file or proto_descriptor_set is required",
            )
            expect_error(
                bridge,
                "COPY descriptor_validation FROM 'unused' (FORMAT nats_js, proto_extract ['x'], "
                "proto_descriptor_set 'root.protoset')",
                "proto_message parameter is required",
            )
        finally:
            bridge.close()

    print("protobuf descriptor-set tests passed")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
