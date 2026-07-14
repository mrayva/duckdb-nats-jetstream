#!/usr/bin/env bash

duckdb_host_lib_is_clean() {
  local lib_path="$1"

  if [ ! -f "$lib_path" ]; then
    return 1
  fi

  if ! command -v nm >/dev/null 2>&1; then
    return 0
  fi

  if nm -D "$lib_path" 2>/dev/null | grep -Eq 'nats_js_duckdb_cpp_(init|version)$'; then
    return 1
  fi

  return 0
}

duckdb_host_lib_resolve() {
  local preferred="${DUCKDB_HOST_LIB:-}"
  local tip_root="${TIP_ROOT:-/tmp/duckdb-tip-clean}"
  local duckdb_bin="${DUCKDB_BIN:-}"
  local candidate
  local bin_dir
  local parent_dir

  if [ -n "$preferred" ]; then
    if duckdb_host_lib_is_clean "$preferred"; then
      printf '%s\n' "$preferred"
      return 0
    fi
    printf '%s\n' "DuckDB host library is contaminated: $preferred" >&2
    return 1
  fi

  if [ -n "$duckdb_bin" ] && [ -x "$duckdb_bin" ]; then
    bin_dir="$(cd "$(dirname "$duckdb_bin")" && pwd)"
    parent_dir="$(dirname "$bin_dir")"
    for candidate in \
      "$bin_dir/src/libduckdb.so" \
      "$bin_dir/libduckdb.so" \
      "$parent_dir/src/libduckdb.so" \
      "$parent_dir/libduckdb.so"; do
      if duckdb_host_lib_is_clean "$candidate"; then
        printf '%s\n' "$candidate"
        return 0
      fi
    done
  fi

  for candidate in \
    "$tip_root/build/release/src/libduckdb.so" \
    "$tip_root/build/release/libduckdb.so"; do
    if duckdb_host_lib_is_clean "$candidate"; then
      printf '%s\n' "$candidate"
      return 0
    fi
  done

  printf '%s\n' "Unable to locate a clean libduckdb.so. Set DUCKDB_HOST_LIB to a DuckDB host library built without nats_js." >&2
  return 1
}
