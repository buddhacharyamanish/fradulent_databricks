"""
========================================================================================
Schema Manager & Table Governance Module
========================================================================================
This module implements Steps 2 and 3 of the metadata-driven ingestion architecture:
  1. Unity Catalog Governance: Ensures target Catalog, Schema, and Volume objects exist.
  2. Table Existence Verification: Checks whether the destination Delta table exists in
     Databricks Unity Catalog (`spark.catalog.tableExists`).
  3. Dynamic Schema Discovery: Parses Debezium CDC envelopes (`schema` block and
     `payload.after`/`payload.before`) from live Kafka messages to determine business
     attributes and infer their Spark SQL data types.
  4. Auto-Table Provisioning (When Table is NOT Present):
     Constructs and executes a `CREATE TABLE ... USING DELTA` DDL statement containing:
       - Discovered business entity fields (e.g. `id`, `event_id`, `event_type`, etc.)
       - `raw_payload` (STRING) to guarantee 100% data preservation under schema drift
       - CDC operational metadata (`_cdc_op`, `_cdc_ts_ms`, binlog file and position)
       - Kafka audit metadata (`_kafka_offset`, `_kafka_partition`, `_ingested_at`)
       - Delta auto-compaction and optimized writes table properties.
  5. Schema Drift & Alteration Sync (When Table IS Already Present):
     Inspects existing table columns against incoming Kafka fields. If new columns were
     added upstream (e.g. `ALTER TABLE activity_log ADD COLUMN ...` in MySQL), it
     automatically generates and executes `ALTER TABLE ... ADD COLUMN` DDL statements
     before inserting data.
========================================================================================
"""

import json
from typing import Dict, Any, List, Tuple, Optional

# Graceful PySpark import handling for local syntax checking vs cluster execution
try:
    from pyspark.sql import SparkSession, DataFrame
except ImportError:
    SparkSession = object
    DataFrame = object

from .config import PipelineConfig


class SchemaManager:
    """
    Manages Unity Catalog object lifecycle, schema comparison, dynamic table creation,
    and automatic ALTER TABLE schema evolution for Delta Lake.
    """

    def __init__(self, spark: SparkSession, config: PipelineConfig):
        """
        Initializes the SchemaManager.

        Args:
            spark (SparkSession): The active PySpark session.
            config (PipelineConfig): The pipeline configuration instance.
        """
        self.spark = spark
        self.config = config
        self.catalog = config.target.catalog
        self.schema = config.target.schema
        self.table = config.target.table
        self.full_table_name = config.full_table_name
        self.full_schema_name = config.full_schema_name

    def ensure_catalog_and_schema(self) -> None:
        """
        Ensures the target Unity Catalog, Schema, and Checkpoint Volume exist.

        Behavior:
          - Attempts to run `CREATE CATALOG IF NOT EXISTS`.
          - Ensures the target schema exists (`CREATE SCHEMA IF NOT EXISTS`).
          - Ensures the checkpoint volume exists if `/Volumes/` is used.
        """
        if not self.config.metadata_governance.auto_create_catalog_schema:
            return

        print(f"Checking Unity Catalog environment: '{self.full_schema_name}'...")
        try:
            self.spark.sql(f"CREATE CATALOG IF NOT EXISTS `{self.catalog}`")
            print(f"  Catalog '{self.catalog}' confirmed.")
        except Exception as e:
            # In some Azure Databricks workspaces without account-level default storage,
            # catalog creation may require MANAGED LOCATION or pre-creation by admin.
            print(f"  Catalog check note: {e}")

        try:
            self.spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{self.catalog}`.`{self.schema}`")
            print(f"  Schema '{self.full_schema_name}' confirmed.")
        except Exception as e:
            print(f"  Warning creating schema '{self.full_schema_name}': {e}")

        # Check if the checkpoint directory is configured inside a Unity Catalog Volume
        if self.config.target.checkpoint_volume and self.config.target.checkpoint_volume.startswith("/Volumes/"):
            try:
                volume_name = self.config.target.checkpoint_volume.split("/")[-1]
                self.spark.sql(
                    f"CREATE VOLUME IF NOT EXISTS `{self.catalog}`.`{self.schema}`.`{volume_name}`"
                )
                print(f"  Volume '{self.catalog}.{self.schema}.{volume_name}' confirmed.")
            except Exception as e:
                print(f"  Volume creation note: {e}")

    def table_exists(self) -> bool:
        """
        Checks whether the target Delta table exists in Databricks Unity Catalog.

        Returns:
            bool: True if table exists; False if table is yet to be created.
        """
        try:
            return self.spark.catalog.tableExists(self.full_table_name)
        except Exception:
            # Fallback check using spark.table resolution
            try:
                self.spark.table(self.full_table_name)
                return True
            except Exception:
                return False

    def get_existing_columns(self) -> Dict[str, str]:
        """
        Inspects the current schema of the target Delta table in Unity Catalog.

        Returns:
            Dict[str, str]: Mapping of lowercase column names to their Spark SQL data type strings.
        """
        try:
            table_schema = self.spark.table(self.full_table_name).schema
            cols = {field.name.lower(): field.dataType.simpleString() for field in table_schema.fields}
            return cols
        except Exception as e:
            print(f"Warning: Could not read existing table schema: {e}")
            return {}

    @staticmethod
    def infer_spark_type(val: Any) -> str:
        """
        Infers the appropriate Spark SQL data type from a Python runtime value.

        Mapping Rules:
          - int     -> BIGINT   (prevents integer overflow for database primary keys/timestamps)
          - float   -> DOUBLE   (standard IEEE 754 floating-point)
          - bool    -> BOOLEAN  (true/false flags)
          - dict    -> STRING   (serialized nested JSON)
          - list    -> STRING   (serialized JSON array)
          - None/str-> STRING   (safe textual representation)

        Args:
            val (Any): Python object from parsed payload.

        Returns:
            str: Spark SQL data type string (e.g. 'BIGINT', 'DOUBLE', 'BOOLEAN', 'STRING').
        """
        if val is None:
            return "STRING"
        if isinstance(val, bool):
            return "BOOLEAN"
        if isinstance(val, int):
            return "BIGINT"
        if isinstance(val, float):
            return "DOUBLE"
        if isinstance(val, (dict, list)):
            return "STRING"
        return "STRING"

    def extract_incoming_fields(self, sample_df: Optional[DataFrame]) -> Dict[str, str]:
        """
        Extracts incoming column names and their Spark SQL types by inspecting sample Kafka data.

        Inspection Strategy:
          1. Schema Block Inspection: If the Kafka message is generated by Debezium with an
             embedded JSON Schema (`schema.fields`), extracts precise column types
             (int32, int64, boolean, string, etc.).
          2. Payload Dictionary Inspection: If no schema block is present, unwraps
             `payload.after` (INSERT/UPDATE) or `payload.before` (DELETE) and infers
             types from Python values using `infer_spark_type()`.

        Args:
            sample_df (Optional[DataFrame]): Bounded batch sample DataFrame from KafkaClient.

        Returns:
            Dict[str, str]: Discovered field names mapped to their inferred Spark SQL types.
        """
        if sample_df is None:
            return {}

        fields_dict: Dict[str, str] = {}
        try:
            rows = sample_df.selectExpr("CAST(value AS STRING) AS val_str").limit(10).collect()
            for r in rows:
                raw_str = r["val_str"]
                if not raw_str:
                    continue
                try:
                    parsed = json.loads(raw_str)
                except Exception:
                    continue

                # Strategy 1: Check for explicit Debezium schema descriptor
                schema_block = parsed.get("schema")
                if isinstance(schema_block, dict) and "fields" in schema_block:
                    for f in schema_block["fields"]:
                        if f.get("field") == "after" and "fields" in f:
                            for sub_f in f["fields"]:
                                f_name = sub_f.get("field")
                                f_type = sub_f.get("type", "string")
                                type_map = {
                                    "int8": "SMALLINT",
                                    "int16": "SMALLINT",
                                    "int32": "INT",
                                    "int64": "BIGINT",
                                    "float": "FLOAT",
                                    "double": "DOUBLE",
                                    "boolean": "BOOLEAN",
                                    "bytes": "BINARY",
                                    "string": "STRING",
                                }
                                if f_name and f_name not in fields_dict:
                                    fields_dict[f_name] = type_map.get(f_type.lower(), "STRING")

                # Strategy 2: Extract record data from Debezium payload envelope
                record_data = {}
                if "payload" in parsed and isinstance(parsed["payload"], dict):
                    payload = parsed["payload"]
                    record_data = payload.get("after") or payload.get("before") or {}
                elif isinstance(parsed, dict):
                    record_data = parsed

                # Fallback value-based type inference
                if isinstance(record_data, dict):
                    for k, v in record_data.items():
                        if k not in fields_dict:
                            fields_dict[k] = self.infer_spark_type(v)

            return fields_dict
        except Exception as e:
            print(f"Warning: Error extracting incoming fields from Kafka sample: {e}")
            return {}

    def create_target_table(self, incoming_fields: Dict[str, str]) -> None:
        """
        Creates the target Delta Lake table in Unity Catalog when it is not yet present.

        Column Architecture:
          1. Discovered Business Columns: Derived from the Kafka stream sample.
          2. Raw Payload Column (`raw_payload STRING`): Stores unparsed JSON for zero data loss.
          3. CDC Metadata Columns: `_cdc_op`, `_cdc_ts_ms`, binlog file, and position.
          4. Kafka Audit Metadata Columns: `_kafka_offset`, `_kafka_partition`, `_kafka_timestamp`, `_ingested_at`.

        Table Optimizations:
          - `delta.autoOptimize.optimizeWrite = true`: Combines small files during write.
          - `delta.autoOptimize.autoCompact = true`: Background compactor for optimal query speed.

        Args:
            incoming_fields (Dict[str, str]): Discovered column names and types.
        """
        print(f"Creating Delta table '{self.full_table_name}'...")
        col_defs = []

        # 1. Discovered business columns
        if incoming_fields:
            for col_name, spark_type in incoming_fields.items():
                col_defs.append(f"`{col_name}` {spark_type}")
        else:
            # Fallback starter column if Kafka topic was empty upon initial run
            col_defs.append("`id` BIGINT")

        # 2. Raw payload column (guarantees 100% data preservation)
        if self.config.metadata_governance.include_raw_payload:
            col_defs.append("`raw_payload` STRING")

        # 3. CDC metadata columns
        if self.config.metadata_governance.include_cdc_metadata:
            col_defs.append("`_cdc_op` STRING")
            col_defs.append("`_cdc_ts_ms` BIGINT")
            col_defs.append("`_cdc_source_db` STRING")
            col_defs.append("`_cdc_source_table` STRING")
            col_defs.append("`_cdc_binlog_file` STRING")
            col_defs.append("`_cdc_binlog_pos` BIGINT")

        # 4. Kafka stream metadata columns
        if self.config.metadata_governance.include_kafka_metadata:
            col_defs.append("`_kafka_key` STRING")
            col_defs.append("`_kafka_topic` STRING")
            col_defs.append("`_kafka_partition` INT")
            col_defs.append("`_kafka_offset` BIGINT")
            col_defs.append("`_kafka_timestamp` TIMESTAMP")
            col_defs.append("`_ingested_at` TIMESTAMP")

        ddl = f"""
        CREATE TABLE IF NOT EXISTS `{self.catalog}`.`{self.schema}`.`{self.table}` (
            {', '.join(col_defs)}
        )
        USING DELTA
        TBLPROPERTIES (
            'delta.autoOptimize.optimizeWrite' = 'true',
            'delta.autoOptimize.autoCompact' = 'true'
        )
        """
        self.spark.sql(ddl)
        print(f"Successfully created target Delta table: '{self.full_table_name}'")

    def sync_table_columns(self, incoming_fields: Dict[str, str]) -> List[str]:
        """
        Compares existing table columns with incoming Kafka payload columns.
        If columns were added upstream in MySQL, executes ALTER TABLE DDL to add them
        to the Databricks table prior to inserting data.

        Args:
            incoming_fields (Dict[str, str]): Incoming fields from Kafka stream.

        Returns:
            List[str]: List of column names added via ALTER TABLE.
        """
        existing_cols = self.get_existing_columns()
        print(f"Existing columns in '{self.full_table_name}': {len(existing_cols)} column(s)")

        # Determine which incoming fields are missing in the target table
        missing_columns: Dict[str, str] = {}
        for col_name, spark_type in incoming_fields.items():
            if col_name.lower() not in existing_cols:
                missing_columns[col_name] = spark_type

        # If all columns exist, schema is in sync
        if not missing_columns:
            print(f"All {len(incoming_fields)} incoming column(s) are present in '{self.full_table_name}'. Schema is in sync.")
            return []

        # Alter the table to add missing columns
        print(f"Upstream schema changes detected. Adding {len(missing_columns)} missing column(s) to Databricks table:")
        added = []
        for col_name, spark_type in missing_columns.items():
            alter_sql = f"ALTER TABLE `{self.catalog}`.`{self.schema}`.`{self.table}` ADD COLUMN `{col_name}` {spark_type}"
            print(f"  Executing: {alter_sql}")
            self.spark.sql(alter_sql)
            added.append(col_name)

        print(f"Successfully altered table '{self.full_table_name}'. Added columns: {added}")
        return added
