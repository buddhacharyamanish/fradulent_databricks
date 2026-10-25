"""
========================================================================================
Pipeline Configuration & Metadata Governance Module
========================================================================================
This module defines the strongly-typed data structures and loader utilities that drive
the metadata-driven CDC ingestion pipeline.

Design Principles:
  - Configuration as Code: Ingestion behavior (sources, targets, schema evolution,
    checkpointing) is defined declaratively rather than hardcoded.
  - Multi-Environment Support: Values can be specified via JSON files, overridden by
    environment variables, or dynamically injected via CLI arguments in `databricks.yml`.
  - Type-Safety: Implemented using Python `@dataclass` for clear attribute typing,
    default values, and IDE autocompletion.
========================================================================================
"""

import json
import os
from dataclasses import dataclass, field
from typing import Dict, Any, Optional


@dataclass
class SourceConfig:
    """
    Configuration parameters for the Kafka streaming source.

    Attributes:
        type (str): Source type identifier (e.g. 'kafka').
        bootstrap_servers (str): Comma-separated list of Kafka broker host:port endpoints.
        topic (str): Upstream Kafka topic to subscribe to.
        starting_offsets (str): Where to begin consuming ('earliest' or 'latest').
        fail_on_data_loss (bool): Whether to crash stream if Kafka offsets have expired.
        max_offsets_per_trigger (Optional[int]): Rate-limiting option to cap micro-batch size.
    """
    type: str = "kafka"
    bootstrap_servers: str = "34.228.32.86:9092"
    topic: str = "frauddb.frauddb.activity_log"
    starting_offsets: str = "earliest"
    fail_on_data_loss: bool = False
    max_offsets_per_trigger: Optional[int] = 50000


@dataclass
class TargetConfig:
    """
    Configuration parameters for the Databricks Unity Catalog target.

    Attributes:
        catalog (str): Unity Catalog name (e.g. 'fradulenttransactionmonitoring').
        schema (str): Unity Catalog schema/database (e.g. 'bronze').
        table (str): Target Delta Lake table name (e.g. 'activity_log_staging').
        format (str): Destination storage format (always 'delta').
        checkpoint_volume (str): Unity Catalog Volume path for stream checkpoints.
        checkpoint_path (str): Full subdirectory path for this specific table's stream state.
        checkpoint_adls_fallback (Optional[str]): Direct ADLS Gen2 URI fallback if needed.
        trigger_mode (str): Micro-batch interval (e.g. 'availableNow', '10 seconds').
    """
    catalog: str = "fradulenttransactionmonitoring"
    schema: str = "bronze"
    table: str = "activity_log_staging"
    format: str = "delta"
    checkpoint_volume: str = "/Volumes/fradulenttransactionmonitoring/bronze/fraud_checkpoints"
    checkpoint_path: str = "/Volumes/fradulenttransactionmonitoring/bronze/fraud_checkpoints/activity_log_staging"
    checkpoint_adls_fallback: Optional[str] = None
    trigger_mode: str = "10 seconds"


@dataclass
class GovernanceConfig:
    """
    Metadata governance, schema evolution, and audit policies.

    Attributes:
        auto_create_catalog_schema (bool): Automatically run CREATE SCHEMA IF NOT EXISTS.
        auto_create_table (bool): Automatically create target Delta table if missing.
        auto_add_missing_columns (bool): Detect upstream DDL alterations and run ALTER TABLE.
        merge_schema (bool): Pass `.option('mergeSchema', 'true')` to writeStream.
        include_raw_payload (bool): Persist full unparsed JSON string for 100% data preservation.
        include_cdc_metadata (bool): Extract Debezium operational metadata (_cdc_op, _cdc_ts_ms).
        include_kafka_metadata (bool): Capture stream offset, partition, and timestamp.
    """
    auto_create_catalog_schema: bool = True
    auto_create_table: bool = True
    auto_add_missing_columns: bool = True
    merge_schema: bool = True
    include_raw_payload: bool = True
    include_cdc_metadata: bool = True
    include_kafka_metadata: bool = True


@dataclass
class PipelineConfig:
    """
    Root configuration object aggregating source, target, and governance settings.
    """
    pipeline_name: str = "kafka_cdc_bronze_ingestion"
    version: str = "1.0.0"
    source: SourceConfig = field(default_factory=SourceConfig)
    target: TargetConfig = field(default_factory=TargetConfig)
    metadata_governance: GovernanceConfig = field(default_factory=GovernanceConfig)

    @property
    def full_table_name(self) -> str:
        """Returns the fully qualified Unity Catalog table identifier: catalog.schema.table."""
        return f"{self.target.catalog}.{self.target.schema}.{self.target.table}"

    @property
    def full_schema_name(self) -> str:
        """Returns the fully qualified Unity Catalog schema identifier: catalog.schema."""
        return f"{self.target.catalog}.{self.target.schema}"

    @classmethod
    def from_json_file(cls, path: str) -> "PipelineConfig":
        """
        Loads configuration from a JSON file path.

        Args:
            path (str): File system path to pipeline_metadata.json.

        Returns:
            PipelineConfig: Instantiated configuration object.
        """
        if not os.path.exists(path):
            raise FileNotFoundError(f"Configuration file not found at: {path}")

        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)

        return cls.from_dict(data)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PipelineConfig":
        """
        Deserializes a raw Python dictionary into a structured PipelineConfig.

        Args:
            data (Dict[str, Any]): Parsed JSON dictionary.

        Returns:
            PipelineConfig: Strongly-typed configuration instance.
        """
        source_data = data.get("source", {})
        target_data = data.get("target", {})
        gov_data = data.get("metadata_governance", {})

        return cls(
            pipeline_name=data.get("pipeline_name", "kafka_cdc_bronze_ingestion"),
            version=data.get("version", "1.0.0"),
            source=SourceConfig(**source_data),
            target=TargetConfig(**target_data),
            metadata_governance=GovernanceConfig(**gov_data),
        )


def load_config(config_path: Optional[str] = None) -> PipelineConfig:
    """
    Discovers and loads the pipeline metadata configuration using a multi-tier search path.

    Search Priority:
      1. Explicitly provided `config_path` argument.
      2. Environment variable `PIPELINE_CONFIG_PATH`.
      3. Relative file path `config/pipeline_metadata.json` from the repository root.
      4. Current working directory `config/pipeline_metadata.json`.
      5. Built-in defaults if no configuration file is located.

    Args:
        config_path (Optional[str]): Explicit override file path.

    Returns:
        PipelineConfig: The resolved configuration.
    """
    candidates = []
    if config_path:
        candidates.append(config_path)

    env_path = os.getenv("PIPELINE_CONFIG_PATH")
    if env_path:
        candidates.append(env_path)

    # Resolve path relative to module location
    base_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    candidates.append(os.path.join(base_dir, "config", "pipeline_metadata.json"))
    candidates.append(os.path.join(os.getcwd(), "config", "pipeline_metadata.json"))

    for c in candidates:
        if os.path.exists(c):
            print(f"Loaded pipeline metadata config from: {c}")
            return PipelineConfig.from_json_file(c)

    print("No config file found in candidate paths; using default configuration.")
    return PipelineConfig()
