"""
========================================================================================
CDC Pipeline Orchestrator Module
========================================================================================
This module serves as the primary coordinator for the metadata-driven ingestion
lifecycle. It binds together the Kafka client, schema manager, and Delta stream writer
into a unified workflow:

Orchestrated Lifecycle:
  1. Low-level TCP connectivity probe to Kafka brokers.
  2. Step 1: Kafka Stream Inspection & Offset Diagnostics
     - Reads current offsets and message counts from Kafka.
     - Samples live messages and infers schema / payload data types.
  3. Unity Catalog Governance:
     - Confirms Catalog, Schema, and Volume objects exist.
  4. Step 2 & 3: Table Existence Check & Schema Synchronization:
     - If table DOES NOT exist: Dynamically creates the Delta table with inferred
       business columns, CDC lineage metadata, Kafka audit metadata, and raw payload.
     - If table ALREADY exists: Compares existing schema columns against incoming Kafka
       payload columns. If upstream columns were added (schema drift), executes
       `ALTER TABLE ... ADD COLUMN` statements before writing data.
  5. Streaming Ingestion:
     - Transforms raw Kafka stream into typed Bronze staging format.
     - Writes into Delta Lake table with checkpointing and `.option("mergeSchema", "true")`.
  6. Post-Ingestion Verification:
     - Queries target table to audit row counts and preview recently inserted records.
========================================================================================
"""

from typing import Optional

# Graceful PySpark import handling for local syntax checking vs cluster execution
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import col
except ImportError:
    SparkSession = object
    col = None

from .config import PipelineConfig, load_config
from .kafka_client import KafkaClient
from .schema_manager import SchemaManager
from .stream_writer import StreamWriter
from .preflight import PreflightChecker, PreflightException


class CDCPipeline:
    """
    Coordinates the 3-step metadata-driven lifecycle:
      Kafka Inspection -> Table Verification/Creation -> Delta Streaming Ingestion.
    """

    def __init__(self, spark: SparkSession, config: Optional[PipelineConfig] = None):
        """
        Initializes the CDCPipeline orchestrator.

        Args:
            spark (SparkSession): The active PySpark session.
            config (Optional[PipelineConfig]): Optional configuration instance. If omitted,
                                               configuration is loaded automatically.
        """
        self.spark = spark
        self.config = config or load_config()
        self.preflight = PreflightChecker(self.spark, self.config)
        self.kafka_client = KafkaClient(self.spark, self.config)
        self.schema_manager = SchemaManager(self.spark, self.config)
        self.stream_writer = StreamWriter(self.config)

    def run(self) -> None:
        """
        Executes the complete metadata-driven ingestion pipeline lifecycle.
        """
        # Phase 0: Preliminary Pre-Flight Environment & Connectivity Checks
        self.preflight.run_preflight_suite()

        print("Starting metadata-driven Kafka CDC Bronze pipeline")
        print(f"Pipeline: {self.config.pipeline_name} (v{self.config.version})")
        print(f"Source Topic: {self.config.source.topic} @ {self.config.source.bootstrap_servers}")
        print(f"Target Table: {self.config.full_table_name}")

        # Phase 1: Check Data in Kafka Stream (Step 1)
        total_records, _, sample_batch_df = self.kafka_client.inspect_kafka_stream()

        incoming_fields = self.schema_manager.extract_incoming_fields(sample_batch_df)
        print(f"Discovered {len(incoming_fields)} payload field(s) from Kafka stream.")

        # Phase 3: Ensure Unity Catalog & Schema Environment
        self.schema_manager.ensure_catalog_and_schema()

        # Phase 4: Check Databricks Target Table Existence (Step 2 & 3)
        full_table = self.config.full_table_name
        is_table_present = self.schema_manager.table_exists()
        print(f"STEP 2: CHECKING DATABRICKS TABLE: '{full_table}'")

        if not is_table_present:
            print(f"Table '{full_table}' is NOT PRESENT. Creating table before inserting data...")
            self.schema_manager.create_target_table(incoming_fields)
        else:
            print(f"Table '{full_table}' is ALREADY PRESENT. Checking existing columns...")
            added_columns = self.schema_manager.sync_table_columns(incoming_fields)
            if added_columns:
                print(f"Synchronized {len(added_columns)} new column(s) into table.")
            else:
                print("All columns verified. Ready for direct ingestion.")

        # Phase 5: Stream Data from Kafka into Databricks Bronze Delta Table (Step 3)
        print(f"STEP 3: STREAMING DATA FROM KAFKA TO DATABRICKS")
        raw_stream_df = self.kafka_client.create_read_stream()
        transformed_df = self.stream_writer.transform_stream(raw_stream_df, incoming_fields)

        # Launch streaming query
        query = self.stream_writer.write_to_delta(transformed_df)
        print("Awaiting stream execution...")
        query.awaitTermination()
        print("Streaming execution finished.")
