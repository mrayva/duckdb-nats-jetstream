#!/usr/bin/env python3

from __future__ import annotations

import argparse
import ctypes
import os
import sys
import time
from dataclasses import dataclass
from typing import Iterable, List


class DuckDBResult(ctypes.Structure):
    _fields_ = [
        ("deprecated_column_count", ctypes.c_uint64),
        ("deprecated_row_count", ctypes.c_uint64),
        ("deprecated_rows_changed", ctypes.c_uint64),
        ("deprecated_columns", ctypes.c_void_p),
        ("deprecated_error_message", ctypes.c_char_p),
        ("internal_data", ctypes.c_void_p),
    ]


class DuckDBSessionError(RuntimeError):
    pass


@dataclass
class PlanAction:
    kind: str
    arg: str = ""
    timeout: float = 30.0


def parse_plan(stream) -> List[PlanAction]:
    actions: List[PlanAction] = []
    collecting_sql = False
    sql_lines: List[str] = []

    for raw_line in stream:
        line = raw_line.rstrip("\n")
        stripped = line.strip()

        if collecting_sql:
            if stripped == "END":
                actions.append(PlanAction("send", "\n".join(sql_lines)))
                sql_lines = []
                collecting_sql = False
            else:
                sql_lines.append(line)
            continue

        if not stripped or stripped.startswith("#"):
            continue

        if stripped == "SEND":
            collecting_sql = True
            sql_lines = []
            continue

        if stripped.startswith("EXPECT "):
            parts = stripped.split(None, 2)
            if len(parts) < 2:
                raise ValueError(f"Invalid EXPECT line: {line}")
            timeout = float(parts[2]) if len(parts) > 2 else 30.0
            actions.append(PlanAction("expect", parts[1], timeout))
            continue

        if stripped == "QUIT":
            actions.append(PlanAction("quit"))
            continue

        if stripped.startswith("SLEEP "):
            parts = stripped.split(None, 1)
            actions.append(PlanAction("sleep", parts[1], float(parts[1])))
            continue

        raise ValueError(f"Unknown plan directive: {line}")

    if collecting_sql:
        raise ValueError("Unterminated SEND block in plan")

    return actions


def split_sql_statements(sql: str) -> List[str]:
    statements: List[str] = []
    current: List[str] = []
    in_single_quote = False
    in_double_quote = False
    i = 0

    while i < len(sql):
        ch = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if ch == "'" and not in_double_quote:
            if in_single_quote and nxt == "'":
                current.append(ch)
                current.append(nxt)
                i += 2
                continue
            in_single_quote = not in_single_quote
            current.append(ch)
            i += 1
            continue

        if ch == '"' and not in_single_quote:
            in_double_quote = not in_double_quote
            current.append(ch)
            i += 1
            continue

        if ch == ";" and not in_single_quote and not in_double_quote:
            statement = "".join(current).strip()
            if statement:
                statements.append(statement)
            current = []
            i += 1
            continue

        current.append(ch)
        i += 1

    tail = "".join(current).strip()
    if tail:
        statements.append(tail)
    return statements


def format_value(lib: ctypes.CDLL, result: DuckDBResult, col: int, row: int) -> str:
    lib.duckdb_value_varchar.restype = ctypes.c_void_p
    lib.duckdb_value_varchar.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64, ctypes.c_uint64]
    ptr = lib.duckdb_value_varchar(ctypes.byref(result), col, row)
    if not ptr:
        return "NULL"
    try:
        return ctypes.string_at(ptr).decode("utf-8", errors="replace")
    finally:
        lib.duckdb_free(ctypes.c_void_p(ptr))


class DuckDBBridge:
    def __init__(self, duckdb_lib_path: str, db_file: str):
        self.lib = ctypes.CDLL(duckdb_lib_path)

        self.lib.duckdb_create_config.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_create_config.restype = ctypes.c_int
        self.lib.duckdb_set_config.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.c_char_p]
        self.lib.duckdb_set_config.restype = ctypes.c_int
        self.lib.duckdb_destroy_config.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_destroy_config.restype = None
        self.lib.duckdb_open.argtypes = [ctypes.c_char_p, ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_open.restype = ctypes.c_int
        self.lib.duckdb_open_ext.argtypes = [
            ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.c_void_p,
            ctypes.POINTER(ctypes.c_char_p),
        ]
        self.lib.duckdb_open_ext.restype = ctypes.c_int
        self.lib.duckdb_close.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_close.restype = None
        self.lib.duckdb_connect.argtypes = [ctypes.c_void_p, ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_connect.restype = ctypes.c_int
        self.lib.duckdb_disconnect.argtypes = [ctypes.POINTER(ctypes.c_void_p)]
        self.lib.duckdb_disconnect.restype = None
        self.lib.duckdb_query.argtypes = [ctypes.c_void_p, ctypes.c_char_p, ctypes.POINTER(DuckDBResult)]
        self.lib.duckdb_query.restype = ctypes.c_int
        self.lib.duckdb_destroy_result.argtypes = [ctypes.POINTER(DuckDBResult)]
        self.lib.duckdb_destroy_result.restype = None
        self.lib.duckdb_result_error.argtypes = [ctypes.POINTER(DuckDBResult)]
        self.lib.duckdb_result_error.restype = ctypes.c_char_p
        self.lib.duckdb_row_count.argtypes = [ctypes.POINTER(DuckDBResult)]
        self.lib.duckdb_row_count.restype = ctypes.c_uint64
        self.lib.duckdb_column_count.argtypes = [ctypes.POINTER(DuckDBResult)]
        self.lib.duckdb_column_count.restype = ctypes.c_uint64
        self.lib.duckdb_column_name.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64]
        self.lib.duckdb_column_name.restype = ctypes.c_char_p
        self.lib.duckdb_value_varchar.argtypes = [ctypes.POINTER(DuckDBResult), ctypes.c_uint64, ctypes.c_uint64]
        self.lib.duckdb_value_varchar.restype = ctypes.c_void_p
        self.lib.duckdb_free.argtypes = [ctypes.c_void_p]
        self.lib.duckdb_free.restype = None

        self.database = ctypes.c_void_p()
        self.connection = ctypes.c_void_p()
        self.transcript = ""

        config = ctypes.c_void_p()
        state = self.lib.duckdb_create_config(ctypes.byref(config))
        if state != 0:
            raise DuckDBSessionError("duckdb_create_config failed")
        try:
            state = self.lib.duckdb_set_config(config, b"allow_unsigned_extensions", b"true")
            if state != 0:
                raise DuckDBSessionError("duckdb_set_config(allow_unsigned_extensions) failed")

            error_message = ctypes.c_char_p()
            state = self.lib.duckdb_open_ext(
                db_file.encode("utf-8"),
                ctypes.byref(self.database),
                config,
                ctypes.byref(error_message),
            )
            if state != 0:
                detail = error_message.value.decode("utf-8", errors="replace") if error_message.value else ""
                raise DuckDBSessionError(f"duckdb_open_ext failed for {db_file}: {detail}")
        finally:
            self.lib.duckdb_destroy_config(ctypes.byref(config))

        state = 0
        if not self.database.value:
            state = 1
        if state != 0:
            raise DuckDBSessionError(f"duckdb_open failed for {db_file}")
        state = self.lib.duckdb_connect(self.database, ctypes.byref(self.connection))
        if state != 0:
            self.lib.duckdb_close(ctypes.byref(self.database))
            raise DuckDBSessionError(f"duckdb_connect failed for {db_file}")

    def close(self) -> None:
        if self.connection.value:
            self.lib.duckdb_disconnect(ctypes.byref(self.connection))
            self.connection = ctypes.c_void_p()
        if self.database.value:
            self.lib.duckdb_close(ctypes.byref(self.database))
            self.database = ctypes.c_void_p()

    def execute(self, sql: str) -> None:
        statements = split_sql_statements(sql)
        if not statements:
            return
        for statement in statements:
            result = DuckDBResult()
            state = self.lib.duckdb_query(self.connection, statement.encode("utf-8"), ctypes.byref(result))
            try:
                if state != 0:
                    err = self.lib.duckdb_result_error(ctypes.byref(result))
                    err_msg = err.decode("utf-8", errors="replace") if err else "duckdb_query failed"
                    raise DuckDBSessionError(err_msg)

                rows = self.lib.duckdb_row_count(ctypes.byref(result))
                cols = self.lib.duckdb_column_count(ctypes.byref(result))
                if rows and cols:
                    headers = [
                        self.lib.duckdb_column_name(ctypes.byref(result), col).decode("utf-8", errors="replace")
                        for col in range(cols)
                    ]
                    rendered = []
                    rendered.append(" | ".join(headers))
                    for row in range(rows):
                        values = [format_value(self.lib, result, col, row) for col in range(cols)]
                        rendered.append(" | ".join(values))
                    rendered_text = "\n".join(rendered) + "\n"
                    sys.stdout.write(rendered_text)
                    sys.stdout.flush()
                    self.transcript += rendered_text
            finally:
                self.lib.duckdb_destroy_result(ctypes.byref(result))

    def expect(self, pattern: str, timeout: float) -> None:
        deadline = time.monotonic() + timeout
        while pattern not in self.transcript:
            if time.monotonic() >= deadline:
                raise DuckDBSessionError(
                    f"Timed out waiting for pattern: {pattern}\n--- Transcript tail ---\n{self.transcript[-4000:]}"
                )
            time.sleep(0.05)


def find_duckdb_lib(duckdb_bin: str) -> str:
    candidates = []
    env_lib = os.environ.get("DUCKDB_LIB")
    if env_lib:
        candidates.append(env_lib)

    bin_dir = os.path.dirname(os.path.abspath(duckdb_bin))
    candidates.extend(
        [
            os.path.join(bin_dir, "src", "libduckdb.so"),
            os.path.join(bin_dir, "libduckdb.so"),
            os.path.join(os.path.dirname(bin_dir), "src", "libduckdb.so"),
            os.path.join(os.path.dirname(bin_dir), "libduckdb.so"),
        ]
    )

    for candidate in candidates:
        if candidate and os.path.exists(candidate):
            return candidate
    raise DuckDBSessionError(
        "Unable to locate libduckdb.so. Set DUCKDB_LIB or point DUCKDB_BIN at a DuckDB build directory."
    )


def main() -> int:
    parser = argparse.ArgumentParser(description="Drive DuckDB through the C API")
    parser.add_argument("--duckdb-bin", required=True)
    parser.add_argument("--db-file", required=True)
    args = parser.parse_args()

    duckdb_lib = find_duckdb_lib(args.duckdb_bin)
    bridge = DuckDBBridge(duckdb_lib, args.db_file)

    try:
        actions = parse_plan(sys.stdin)
        for action in actions:
            if action.kind == "send":
                bridge.execute(action.arg)
            elif action.kind == "expect":
                bridge.expect(action.arg, action.timeout)
            elif action.kind == "sleep":
                time.sleep(action.timeout)
            elif action.kind == "quit":
                break
            else:
                raise DuckDBSessionError(f"Unknown action kind: {action.kind}")
        return 0
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        return 1
    finally:
        bridge.close()


if __name__ == "__main__":
    raise SystemExit(main())
