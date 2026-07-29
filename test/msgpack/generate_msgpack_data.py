#!/usr/bin/env python3
"""Write a small deterministic MessagePack fixture for the NATS integration tests."""

import argparse

import struct


def pack_string(value):
    encoded = value.encode("utf-8")
    return bytes([0xA0 | len(encoded)]) + encoded


def pack_map(values):
    result = bytes([0x80 | len(values)])
    for key, value in values:
        result += pack_string(key) + value
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("output")
    args = parser.parse_args()

    payload = pack_map([
        ("device_id", pack_string("msgpack-device-1")),
        ("metrics", pack_map([
            ("kw", bytes([0xCB]) + struct.pack(">d", 42.5)),
            ("voltage", bytes([0xCB]) + struct.pack(">d", 480.25)),
        ])),
        ("online", bytes([0xC3])),
        ("reading_count", bytes([0x07])),
    ])
    with open(args.output, "wb") as output:
        output.write(payload)


if __name__ == "__main__":
    main()
