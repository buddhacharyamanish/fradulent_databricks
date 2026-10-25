"""
========================================================================================
Stream Writer & Delta Lake Transformation Module
========================================================================================
This module handles Step 3 of the metadata-driven ingestion architecture:
  1. Stream Transformation: Takes the raw Kafka streaming DataFrame and extracts
     typed business entities from Debezium CDC JSON envelopes (`payload.after` / `payload.before`).
  2. Zero-Loss Architecture: Retains the unparsed `raw_payload` (STRING) in the Delta table,
     guaranteeing that even if upstream schema alterations produce unmapped fields,
     no data or historical attribute is ever lost.
  3. CDC Operational Lineage: Extracts operational metadata (`_cdc_op`, `_cdc_ts_ms`,
     database, table, and binlog coordinates) to support downstream Silver layer merges.
  4. Kafka Audit Lineage: Captures topic, partition, offset, Kafka message timestamp,
     and Databricks cluster ingestion timestamp.
  5. Delta writeStream Execution: Configures checkpointing inside Unity Catalog Volumes,
     enables Delta Lake `.option("mergeSchema", "true")`, applies the configured trigger
     (e.g. continuous micro-batches every 10 seconds or availableNow), and writes to the
     target Delta table.
========================================================================================
"""

from typing import Dict, Any, Optional

# Graceful PySpark import handling for local syntax checking vs cluster execution
try:
    from pyspark.sql import DataFrame
    from pyspark.sql.functions import (
        col,
        current_timestamp,
        get_json_object,
        coalesce,
        to_timestamp
    )
    from pyspark.sql.streaming import StreamingQuery
except ImportError:
    DataFrame = object
    StreamingQuery = object
    col = None
    current_timestamp = None
    get_json_object = None
    coalesce = None
    to_timestamp = None

from .config import PipelineConfig


class StreamWriter:
    """
    Transforms Kafka stream events into a structured Delta Lake schema and executes
    the continuous writeStream operation with schema merging.
    """

    def __init__(self, config: PipelineConfig):
        """
        Initializes the StreamWriter with pipeline configuration settings.

        Args:
            config (PipelineConfig): The pipeline configuration instance.
        """
        self.config = config
        self.full_table_name = config.full_table_name

    def transform_stream(
        self, raw_stream_df: DataFrame, business_fields: Dict[str, str]
    ) -> DataFrame:
        """
        Transforms the raw Kafka stream into the structured Bronze Delta schema:
          1. Retains full `raw_payload` string for auditability and replayability.
          2. Coalesces Debezium `payload.after` (INSERT/UPDATE) and `payload.before` (DELETE).
          3. Dynamically extracts and casts each discovered business column.
          4. Extracts CDC envelope metadata for transaction replay.
          5. Captures Kafka partition, offset, and ingestion timestamps.

        Args:
            raw_stream_df (DataFrame): Raw streaming DataFrame from KafkaClient.
            business_fields (Dict[str, str]): Column names and Spark SQL types discovered
                                             during Step 1.

        Returns:
            DataFrame: Transformed streaming DataFrame ready for Delta write.
        """
        raw_val = col("value").cast("string")

        # Debezium CDC messages package data inside `payload.after` (inserts/updates)
        # or `payload.before` (deletes). Coalescing ensures row content is available for all ops.
        transformed = (
            raw_stream_df
            .withColumn("raw_payload", raw_val)
            .withColumn("_after_json", get_json_object(raw_val, "$.payload.after"))
            .withColumn("_before_json", get_json_object(raw_val, "$.payload.before"))
            .withColumn("_record_json", coalesce(col("_after_json"), col("_before_json"), raw_val))
        )

        select_exprs = []

        # -------------------------------------------------------------
        # 1. Project Discovered Business Columns Dynamically
        # -------------------------------------------------------------
        for col_name, spark_type in business_fields.items():
            json_extractor = get_json_object(col("_record_json"), f"$.{col_name}")
            if spark_type.upper() == "TIMESTAMP":
                select_exprs.append(to_timestamp(json_extractor).alias(col_name))
            else:
                select_exprs.append(json_extractor.cast(spark_type.lower()).alias(col_name))

        # -------------------------------------------------------------
        # 2. Raw Unparsed Payload (Zero Data Loss Guarantee)
        # -------------------------------------------------------------
        if self.config.metadata_governance.include_raw_payload:
            select_exprs.append(col("raw_payload"))

        # -------------------------------------------------------------
        # 3. Debezium CDC Operational Metadata
        # -------------------------------------------------------------
        if self.config.metadata_governance.include_cdc_metadata:
            select_exprs.extend([
                # Operation flag: 'c'=Create/Insert, 'u'=Update, 'd'=Delete, 'r'=Snapshot Read
                get_json_object(raw_val, "$.payload.op").alias("_cdc_op"),
                # Source event timestamp in milliseconds
                get_json_object(raw_val, "$.payload.ts_ms").cast("bigint").alias("_cdc_ts_ms"),
                get_json_object(raw_val, "$.payload.source.db").alias("_cdc_source_db"),
                get_json_object(raw_val, "$.payload.source.table").alias("_cdc_source_table"),
                get_json_object(raw_val, "$.payload.source.file").alias("_cdc_binlog_file"),
                get_json_object(raw_val, "$.payload.source.pos").cast("bigint").alias("_cdc_binlog_pos"),
            ])

        # -------------------------------------------------------------
        # 4. Kafka Stream Audit & Lineage Metadata
        # -------------------------------------------------------------
        if self.config.metadata_governance.include_kafka_metadata:
            select_exprs.extend([
                col("key").cast("string").alias("_kafka_key"),
                col("topic").alias("_kafka_topic"),
                col("partition").cast("int").alias("_kafka_partition"),
                col("offset").cast("bigint").alias("_kafka_offset"),
                col("timestamp").alias("_kafka_timestamp"),
                current_timestamp().alias("_ingested_at"),
            ])

        return transformed.select(*select_exprs)

    def write_to_delta(self, stream_df: DataFrame) -> StreamingQuery:
        """
        Executes writeStream to the target Delta Lake table with checkpointing
        and automatic schema merging enabled.

        Trigger Configuration:
          - 'availableNow': Processes all available data in micro-batch mode and stops.
          - 'once': Legacy one-batch execution.
          - '10 seconds' / processingTime: Continuous micro-batch execution every 10 seconds.

        Args:
            stream_df (DataFrame): Transformed streaming DataFrame.

        Returns:
            StreamingQuery: The active streaming query handle.
        """
        checkpoint_dir = self.config.target.checkpoint_path
        print(f"Starting streaming write into Delta table: '{self.full_table_name}'")
        print(f"  Checkpoint Location: {checkpoint_dir}")
        print(f"  Trigger Mode: {self.config.target.trigger_mode}")

        writer = (
            stream_df.writeStream
            .format(self.config.target.format)
            .outputMode("append")
            .option("checkpointLocation", checkpoint_dir)
        )

        # Delta Lake schema evolution: automatically allows new columns into the table
        if self.config.metadata_governance.merge_schema:
            writer = writer.option("mergeSchema", "true")

        # Configure micro-batch triggering strategy
        trigger_mode = self.config.target.trigger_mode.strip()
        if trigger_mode.lower() == "availablenow":
            writer = writer.trigger(availableNow=True)
        elif trigger_mode.lower() == "once":
            writer = writer.trigger(once=True)
        else:
            # Continuous processing time interval (e.g. '10 seconds', '30 seconds')
            interval = (
                "10 seconds"
                if trigger_mode.lower() == "processingtime"
                else trigger_mode
            )
            writer = writer.trigger(processingTime=interval)

        query = writer.toTable(self.full_table_name)
        return query
