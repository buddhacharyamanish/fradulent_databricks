#!/usr/bin/env python3
"""
========================================================================================
Production Diagnostic Utility: Kafka Network Reachability & Offset Inspector
========================================================================================
This utility inspects Kafka brokers and topics without requiring PySpark, JVM, or
third-party Kafka libraries (uses raw Python socket + struct protocol frames).

Intended Usage in Production:
  - Run from internal cloud environments, bastion hosts, CI/CD runners, or developer
    workstations with network routing to the Kafka cluster (e.g. via AWS DirectConnect,
    VPC Peering, Azure ExpressRoute, or corporate VPN).
  - Dynamically discovers configuration from:
      1. CLI Arguments (`--bootstrap-servers`, `--topic`).
      2. Environment Variables (`KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC`).
      3. Pipeline Metadata File (`config/pipeline_metadata.json`).

Capabilities:
  1. Verifies TCP network reachability and connection latency to Kafka brokers.
  2. Issues Kafka MetadataRequest (API Key 3) to verify topic existence, partition
     count, and leader broker IDs.
  3. Issues Kafka OffsetRequests (API Key 2) to calculate earliest offset, high-watermark
     latest offset, and total unconsumed message lag.
========================================================================================
"""

import sys
import os
import socket
import struct
import json
import argparse
from typing import Tuple, Optional


def load_default_config() -> Tuple[str, int, str]:
    """
    Attempts to discover default Kafka parameters from environment variables
    or pipeline_metadata.json.

    Returns:
        Tuple[str, int, str]: (broker_host, broker_port, topic_name)
    """
    host = os.environ.get("KAFKA_HOST")
    port = int(os.environ.get("KAFKA_PORT", 9092))
    topic = os.environ.get("KAFKA_TOPIC")

    # If full bootstrap servers string is set in environment (e.g. host:port)
    servers_env = os.environ.get("KAFKA_BOOTSTRAP_SERVERS")
    if servers_env:
        first = servers_env.split(",")[0].strip()
        parts = first.split(":")
        host = parts[0]
        port = int(parts[1]) if len(parts) > 1 else 9092

    # Attempt to read config/pipeline_metadata.json if parameters not fully resolved
    if not host or not topic:
        script_dir = os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd()
        config_path = os.path.abspath(os.path.join(script_dir, "..", "config", "pipeline_metadata.json"))
        if os.path.exists(config_path):
            try:
                with open(config_path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    source = data.get("source", {})
                    bootstrap = source.get("bootstrap_servers", "")
                    if bootstrap and not host:
                        first = bootstrap.split(",")[0].strip()
                        parts = first.split(":")
                        host = parts[0]
                        port = int(parts[1]) if len(parts) > 1 else 9092
                    if not topic:
                        topic = source.get("topic", "")
            except Exception:
                pass

    return (host or "127.0.0.1", port or 9092, topic or "frauddb.frauddb.activity_log")


def check_kafka(broker_host: str, broker_port: int, topic_name: str, timeout_sec: int = 5) -> bool:
    """
    Probes Kafka broker reachability, issues metadata queries, and inspects topic offsets.

    Args:
        broker_host (str): Kafka broker IP or hostname.
        broker_port (int): Kafka broker listener port.
        topic_name (str): Upstream topic name.
        timeout_sec (int): Network socket connection timeout in seconds.

    Returns:
        bool: True if broker and topic are reachable and responsive; False otherwise.
    """
    print("=" * 80)
    print("KAFKA NETWORK & PROTOCOL DIAGNOSTIC CHECK")
    print("=" * 80)
    print(f"Target Broker : {broker_host}:{broker_port}")
    print(f"Target Topic  : {topic_name}")
    print(f"Socket Timeout: {timeout_sec}s")
    print("-" * 80)

    # Step 1: TCP Network Socket Reachability Check
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout_sec)

    try:
        sock.connect((broker_host, broker_port))
        print(f"[PASS] Network Reachability: TCP connection to {broker_host}:{broker_port} established.")
    except ConnectionRefusedError:
        print(f"[FAIL] Network Reachability: Connection REFUSED on {broker_host}:{broker_port}.")
        print("       The port is not listening. The Kafka broker service is stopped or crashed.")
        return False
    except socket.timeout:
        print(f"[FAIL] Network Reachability: Connection TIMED OUT to {broker_host}:{broker_port}.")
        print("       Check security groups, VPC routing, cloud firewall rules, or VPN tunnels.")
        return False
    except Exception as e:
        print(f"[FAIL] Network Reachability: {type(e).__name__} - {e}")
        return False

    client_id = b"prod_diag_cli"
    topic_bytes = topic_name.encode()

    try:
        # Step 2: Query Topic Metadata (Kafka API Key 3, Version 0)
        # Wire format: [size][api_key=3][api_ver=0][corr_id=1][client_id][topic_count=1][topic_name]
        payload = struct.pack(">hhih", 3, 0, 1, len(client_id)) + client_id
        payload += struct.pack(">ih", 1, len(topic_bytes)) + topic_bytes
        msg = struct.pack(">i", len(payload)) + payload
        sock.sendall(msg)

        size_raw = sock.recv(4)
        if len(size_raw) < 4:
            print("[FAIL] Protocol Negotiation: Did not receive response length from broker.")
            sock.close()
            return False

        size = struct.unpack(">i", size_raw)[0]
        resp = sock.recv(size)
        sock.close()

        # Parse broker count
        corr_id, brokers_count = struct.unpack(">ii", resp[0:8])
        offset = 8

        for _ in range(brokers_count):
            node_id, host_len = struct.unpack(">ih", resp[offset:offset + 6])
            offset += 6 + host_len + 4  # node_id + host_len + host + port

        # Parse topic metadata
        topics_count = struct.unpack(">i", resp[offset:offset + 4])[0]
        offset += 4

        for _ in range(topics_count):
            err_code, t_len = struct.unpack(">hh", resp[offset:offset + 4])
            offset += 4
            t_name = resp[offset:offset + t_len].decode("utf-8", errors="replace")
            offset += t_len
            partitions_count = struct.unpack(">i", resp[offset:offset + 4])[0]
            offset += 4

            if err_code != 0:
                print(f"[WARN] Topic '{t_name}' returned error code {err_code} (Topic may not exist yet).")
                return False

            print(f"[PASS] Topic Existence    : '{t_name}' discovered with {partitions_count} partition(s).")

            # Collect partition IDs and leaders
            partitions = []
            for _ in range(partitions_count):
                p_err, p_id, leader, replicas_cnt = struct.unpack(">hhii", resp[offset:offset + 12])
                offset += 12
                offset += replicas_cnt * 4  # skip replica IDs
                isr_cnt = struct.unpack(">i", resp[offset:offset + 4])[0]
                offset += 4 + isr_cnt * 4   # skip ISR IDs
                partitions.append((p_id, leader))

            # Step 3: Query Offsets per Partition (Kafka API Key 2, Version 0)
            print("-" * 80)
            print("PARTITION OFFSET & LAG ASSESSMENT:")
            for p_id, leader in partitions:
                # Latest high-watermark offset (time = -1)
                s_off = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s_off.settimeout(timeout_sec)
                s_off.connect((broker_host, broker_port))

                off_req = struct.pack(">hhih", 2, 0, 2, len(client_id)) + client_id
                off_req += struct.pack(">i", -1)
                off_req += struct.pack(">ih", 1, len(topic_bytes)) + topic_bytes
                off_req += struct.pack(">iiqi", 1, p_id, -1, 1)  # -1 = latest
                s_off.sendall(struct.pack(">i", len(off_req)) + off_req)

                sz = struct.unpack(">i", s_off.recv(4))[0]
                resp_off = s_off.recv(sz)
                latest_off = struct.unpack(">q", resp_off[-8:])[0] if sz >= 8 else 0
                s_off.close()

                # Earliest offset (time = -2)
                s_off2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s_off2.settimeout(timeout_sec)
                s_off2.connect((broker_host, broker_port))

                off_req2 = struct.pack(">hhih", 2, 0, 3, len(client_id)) + client_id
                off_req2 += struct.pack(">i", -1)
                off_req2 += struct.pack(">ih", 1, len(topic_bytes)) + topic_bytes
                off_req2 += struct.pack(">iiqi", 1, p_id, -2, 1)  # -2 = earliest
                s_off2.sendall(struct.pack(">i", len(off_req2)) + off_req2)

                sz2 = struct.unpack(">i", s_off2.recv(4))[0]
                resp_off2 = s_off2.recv(sz2)
                earliest_off = struct.unpack(">q", resp_off2[-8:])[0] if sz2 >= 8 else 0
                s_off2.close()

                available = latest_off - earliest_off
                print(f"  Partition {p_id} (Leader Broker {leader}):")
                print(f"    Earliest Offset : {earliest_off}")
                print(f"    Latest Offset   : {latest_off}")
                print(f"    Pending Messages: {available}")

        print("=" * 80)
        return True

    except Exception as e:
        print(f"[FAIL] Kafka protocol communication error: {e}")
        return False


def main():
    default_host, default_port, default_topic = load_default_config()

    parser = argparse.ArgumentParser(
        description="Production Kafka Reachability & Offset Diagnostic Utility"
    )
    parser.add_argument(
        "--bootstrap-servers", "-b",
        type=str,
        default=f"{default_host}:{default_port}",
        help=f"Kafka bootstrap broker host:port (default: {default_host}:{default_port})"
    )
    parser.add_argument(
        "--topic", "-t",
        type=str,
        default=default_topic,
        help=f"Kafka topic name (default: {default_topic})"
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=5,
        help="Socket timeout in seconds (default: 5)"
    )

    args = parser.parse_args()

    # Parse host and port
    broker_entry = args.bootstrap_servers.split(",")[0].strip()
    parts = broker_entry.split(":")
    host = parts[0]
    port = int(parts[1]) if len(parts) > 1 else 9092

    success = check_kafka(
        broker_host=host,
        broker_port=port,
        topic_name=args.topic,
        timeout_sec=args.timeout
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
