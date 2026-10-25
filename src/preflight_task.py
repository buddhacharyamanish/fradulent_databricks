#!/usr/bin/env python3
"""
========================================================================================
Lightweight Pre-Flight Readiness Task Entrypoint
========================================================================================
This module serves as the dedicated entrypoint for lightweight, serverless pre-flight
health checks in Databricks Workflows.

Operational Guarantees:
  1. Zero Heavy Compute: Runs on Databricks Serverless (or minimal Python compute) with
     no VM startup lag and 0 Azure regional vCPU quota consumption.
  2. Fail-Fast Network Probe: Tests raw TCP socket connectivity to the upstream Kafka
     broker before heavy streaming compute clusters are ever allocated.
  3. Upstream Gatekeeper: If Kafka or upstream services are stopped or unreachable,
     halts with an explicit error code (1) and human-readable diagnostic box, preventing
     downstream Spark clusters from spinning up.
========================================================================================
"""

import sys
import os
import argparse

if "__file__" in globals() and __file__:
    _current_dir = os.path.dirname(os.path.abspath(__file__))
elif len(sys.argv) > 0 and sys.argv[0]:
    _current_dir = os.path.dirname(os.path.abspath(sys.argv[0]))
else:
    _current_dir = os.path.abspath(os.getcwd())

_project_root = os.path.abspath(os.path.join(_current_dir, ".."))

for _p in [_current_dir, _project_root]:
    if os.path.exists(_p) and _p not in sys.path:
        sys.path.insert(0, _p)


try:
    from cdc.config import PipelineConfig, load_config
    from cdc.preflight import PreflightChecker, PreflightException
except ImportError:
    from src.cdc.config import PipelineConfig, load_config
    from src.cdc.preflight import PreflightChecker, PreflightException



def parse_args():
    """
    Parses command-line arguments passed by Databricks Workflow tasks.
    """
    parser = argparse.ArgumentParser(
        description="Lightweight Pre-Flight Health Checker for Kafka CDC Pipeline"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Path to pipeline_metadata.json"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Target Unity Catalog name"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="Target schema name"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Target Delta table name"
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default=None,
        help="Kafka bootstrap broker list (e.g. 34.228.32.86:9092)"
    )
    parser.add_argument(
        "--topic",
        type=str,
        default=None,
        help="Kafka topic name"
    )
    return parser.parse_known_args()[0]


def main():
    """
    Executes lightweight pre-flight health checks.
    """
    args = parse_args()

    # Step 1: Load pipeline metadata configuration
    config = load_config(args.config)

    # Step 2: Apply CLI argument overrides from Databricks bundle variables
    if args.catalog:
        config.target.catalog = args.catalog
    if args.schema:
        config.target.schema = args.schema
    if args.table:
        config.target.table = args.table
    if args.bootstrap_servers:
        config.source.bootstrap_servers = args.bootstrap_servers
    if args.topic:
        config.source.topic = args.topic

    # Step 3: Execute early network and invariant checks without Spark
    print("=" * 88)
    print("INITIALIZING LIGHTWEIGHT SERVERLESS PRE-FLIGHT TASK")
    print("=" * 88)
    print(f"Target Broker : {config.source.bootstrap_servers}")
    print(f"Target Topic  : {config.source.topic}")
    print(f"Target Table  : {config.full_table_name}")
    print("=" * 88 + "\n")

    checker = PreflightChecker(spark=None, config=config)

    try:
        checker.run_early_connectivity_checks()
        print("\n" + "=" * 88)
        print("PRE-FLIGHT READINESS VERIFICATION: ALL CHECKS PASSED")
        print("=" * 88)
        print("Upstream Kafka broker is ONLINE and fully reachable.")
        print("Granting authorization to start downstream streaming compute cluster.")
        print("=" * 88 + "\n")
    except PreflightException as pe:
        print("\n" + "=" * 88)
        print("PRE-FLIGHT READINESS VERIFICATION: CHECKS FAILED")
        print("=" * 88)
        print(f"Details: {pe}")
        print("Downstream streaming cluster will NOT be provisioned.")
        print("=" * 88 + "\n")
        raise RuntimeError(f"Pre-flight health check failed: {pe}")


if __name__ == "__main__":
    main()
