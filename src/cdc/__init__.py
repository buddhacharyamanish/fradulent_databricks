"""
========================================================================================
CDC (Change Data Capture) Ingestion Package
========================================================================================
This package implements a modular, metadata-driven streaming ingestion framework
designed to ingest Kafka CDC events directly into Databricks Unity Catalog Delta tables.

Core Modules Exposed:
  - `PipelineConfig`, `load_config`:
      Configuration schema and JSON/environment loader.
  - `KafkaClient`:
      Network connectivity diagnostics, partition offset inspection, and readStream builder.
  - `SchemaManager`:
      Unity Catalog environment governance, table existence checks, dynamic table
      creation, and automatic `ALTER TABLE` schema evolution.
  - `StreamWriter`:
      Stream transformation, Debezium CDC envelope unwrapping, audit metadata injection,
      and Delta Lake writeStream execution with `mergeSchema`.
  - `CDCPipeline`:
      End-to-end pipeline orchestrator executing the 3-step lifecycle.
========================================================================================
"""

from .config import PipelineConfig, load_config
from .kafka_client import KafkaClient
from .schema_manager import SchemaManager
from .stream_writer import StreamWriter
from .pipeline import CDCPipeline
from .preflight import PreflightChecker, PreflightException

__all__ = [
    "PipelineConfig",
    "load_config",
    "KafkaClient",
    "SchemaManager",
    "StreamWriter",
    "CDCPipeline",
    "PreflightChecker",
    "PreflightException",
]
