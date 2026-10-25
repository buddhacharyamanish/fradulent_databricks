"""
========================================================================================
CDC Pipeline Application Entry Point (Databricks Asset Bundle Task)
========================================================================================
This module serves as the primary executable entry point for the Databricks Asset Bundle
(DAB) job. It performs the following roles:
  1. Dynamically patches `sys.path` to ensure custom modules under `src/cdc` can be
     imported inside the Databricks cluster runtime environment.
  2. Parses command-line arguments passed from `databricks.yml` task parameters to enable
     runtime configuration overrides.
  3. Initializes or connects to the active PySpark `SparkSession`.
  4. Loads baseline pipeline metadata from `config/pipeline_metadata.json`.
  5. Instantiates and executes the `CDCPipeline` orchestrator to run the 3-step
     metadata-driven Kafka-to-Databricks Bronze ingestion.
========================================================================================
"""

import os
import sys
import argparse
from typing import Dict, Any


def build_trigger(mode: str) -> Dict[str, Any]:
    """
    Parses a trigger mode string into a PySpark writeStream trigger dictionary.

    Args:
        mode (str): Trigger mode specification. Supported formats:
            - 'availableNow': Micro-batch trigger that processes all pending data
              and terminates (ideal for scheduled batch runs).
            - 'processingTime': Triggers continuously with default interval (10 seconds).
            - '10 seconds' or 'processingTime=30 seconds': Continuous micro-batches with
              custom specified intervals.

    Returns:
        Dict[str, Any]: Trigger options passed to `writeStream.trigger(**options)`.

    Raises:
        ValueError: If an unrecognized trigger mode string is provided.
    """
    m = mode.strip()
    if m.lower() == "availablenow":
        return {"availableNow": True}
    if m.lower().startswith("processingtime"):
        interval = m.split("=", 1)[1].strip() if "=" in m else "10 seconds"
        return {"processingTime": interval}
    # Direct interval strings (e.g. "10 seconds", "1 minute")
    if any(m.lower().endswith(unit) for unit in ["seconds", "second", "minutes", "minute", "hours", "hour"]):
        return {"processingTime": m}
    raise ValueError(f"Unknown trigger mode: {mode}")


# --------------------------------------------------------------------------------------
# Dynamic sys.path Resolution
# --------------------------------------------------------------------------------------
# When a Databricks Job runs a `spark_python_task`, Databricks copies bundle files into
# workspace storage and executes the script. Under certain Databricks runtime conditions,
# `__file__` may not be populated (e.g. when executed via exec()).
# --------------------------------------------------------------------------------------
# Dynamic sys.path Resolution (Production-grade cross-environment discovery)
# --------------------------------------------------------------------------------------
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

# Graceful PySpark import for local syntax validation and remote cluster execution
try:
    from pyspark.sql import SparkSession
except ImportError:
    SparkSession = object

try:
    from cdc.config import load_config
    from cdc.pipeline import CDCPipeline
    from cdc.preflight import PreflightChecker, PreflightException
except ImportError:
    from src.cdc.config import load_config
    from src.cdc.pipeline import CDCPipeline
    from src.cdc.preflight import PreflightChecker, PreflightException



def parse_args():
    """
    Parses command-line arguments passed into the job task.
    These arguments mirror the bundle variables declared in `databricks.yml`.
    Any CLI argument passed here takes precedence over values in `pipeline_metadata.json`.
    """
    parser = argparse.ArgumentParser(
        description="Metadata-Driven Kafka to Databricks Bronze Ingestion"
    )
    parser.add_argument(
        "--config",
        type=str,
        default=None,
        help="Optional path to pipeline_metadata.json"
    )
    parser.add_argument(
        "--catalog",
        type=str,
        default=None,
        help="Target Unity Catalog name (default: fradulenttransactionmonitoring)"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=None,
        help="Target schema name (default: bronze)"
    )
    parser.add_argument(
        "--table",
        type=str,
        default=None,
        help="Target Delta table name (default: activity_log_staging)"
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
        help="Kafka topic name (e.g. frauddb.frauddb.activity_log)"
    )
    parser.add_argument(
        "--trigger-mode",
        type=str,
        default=None,
        help="Stream trigger mode (e.g. availableNow, 10 seconds)"
    )
    # Parse known arguments to allow Databricks-injected internal flags without failure
    return parser.parse_known_args()[0]


def main():
    """
    Main execution workflow:
      1. Parses CLI flags.
      2. Initializes SparkSession.
      3. Loads metadata configuration and merges CLI overrides.
      4. Instantiates and runs the CDCPipeline orchestrator.
    """
    args = parse_args()

    # Step 1: Load base configuration from pipeline_metadata.json
    config = load_config(args.config)

    # Step 2: Apply CLI argument overrides (from databricks.yml bundle variables)
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
    if args.trigger_mode:
        config.target.trigger_mode = args.trigger_mode

    # Step 3: Run Early Network & Invariant Checks BEFORE initializing Spark
    # This prevents any cluster or JVM overhead if Kafka or the AWS host is stopped.
    preflight = PreflightChecker(spark=None, config=config)
    try:
        preflight.run_early_connectivity_checks()
    except PreflightException as pe:
        print("\n" + "=" * 88)
        print("PIPELINE STARTUP HALTED: EARLY PRE-FLIGHT READINESS CHECK FAILED")
        print("=" * 88)
        print(f"Details: {pe}")
        print("SparkSession was NOT initialized because upstream Kafka broker is offline.")
        print("=" * 88)
        sys.exit(1)

    # Step 4: ONLY IF network & configuration checks pass, initialize SparkSession
    spark = SparkSession.builder.appName("KafkaCDC_BronzeIngestion").getOrCreate()

    # Step 5: Execute the complete 3-step metadata-driven ingestion lifecycle
    try:
        pipeline = CDCPipeline(spark=spark, config=config)
        pipeline.run()
    except PreflightException as pe:
        print("\n" + "=" * 88)
        print("PIPELINE STARTUP HALTED: PRE-FLIGHT READINESS CHECK FAILED")
        print("=" * 88)
        print(f"Details: {pe}")
        print("Please resolve the network/service issue detailed in the diagnostic box above.")
        print("=" * 88)
        sys.exit(1)
    except Exception as e:
        print("\n" + "=" * 88)
        print("PIPELINE STREAMING RUNTIME ERROR")
        print("=" * 88)
        error_msg = str(e)
        if "TimeoutException" in error_msg and "describeTopics" in error_msg:
            print(f"Error: Kafka broker at '{config.source.bootstrap_servers}' timed out during topic discovery.")
            print("The Kafka broker was not reachable or stopped responding to describeTopics.")
        else:
            first_lines = "\n".join(error_msg.splitlines()[:6])
            print(f"Error Details:\n{first_lines}")
        print("=" * 88)
        sys.exit(1)


if __name__ == "__main__":
    main()
