# Architectural Specification: Metadata-Driven Kafka CDC Pipeline

## 1. Executive Summary

This project implements an enterprise-grade, metadata-driven Change Data Capture (CDC) streaming pipeline using **Databricks Asset Bundles (DAB)**. It streams real-time database change events from an upstream **AWS MySQL** database (`frauddb.activity_log`) via **Debezium and Apache Kafka** directly into an **Azure Databricks Unity Catalog Delta Lake Bronze table** (`fradulenttransactionmonitoring.bronze.activity_log_staging`).

The ingestion pipeline operates on three fundamental principles:
1. **Metadata-Driven Operation**: Ingestion behavior, topics, schemas, table properties, and checkpoint locations are governed by configuration files (`config/pipeline_metadata.json`) and CLI parameter overrides rather than hardcoded logic.
2. **Autonomous Schema Discovery and Adaptation**:
   - The pipeline inspects the Kafka stream to discover payload schema fields dynamically.
   - If the destination Delta table does not exist in Databricks, the pipeline automatically generates the appropriate `CREATE TABLE` DDL and creates it.
   - If the table already exists, the pipeline compares existing columns against incoming fields and executes `ALTER TABLE` DDL to add any newly detected upstream columns prior to data insertion.
3. **Auditability and Zero Data Loss**: In addition to extracting strongly-typed business attributes, every record preserves the complete, unaltered Debezium JSON payload in a `raw_payload` column, along with Kafka partition/offset coordinates and CDC operation metadata.

---

## 2. Text-Based Architecture Diagrams

### Diagram 1: End-to-End Enterprise Data Flow

```text
+---------------------------------------------------------------------------------------+
|                                UPSTREAM SOURCE TIER                                  |
|                                                                                       |
|   +-----------------------+              +----------------------------------------+   |
|   |   AWS EC2 / RDS       |              |        Debezium MySQL Connector        |   |
|   |      (MySQL 8)        |   binlog     |  - Reads row-level binary logs         |   |
|   |  frauddb.activity_log | -----------> |  - Serializes changes into JSON events  |   |
|   +-----------------------+              |  - Emits: op (c,u,d,r), before, after  |   |
|                                          +----------------------------------------+   |
+------------------------------------------------------│--------------------------------+
                                                       ▼
+---------------------------------------------------------------------------------------+
|                                TRANSPORT / BUFFER TIER                               |
|                                                                                       |
|   +-------------------------------------------------------------------------------+   |
|   |                           Apache Kafka Cluster                                |   |
|   |                       Broker: 34.228.32.86:9092                               |   |
|   |                                                                               |   |
|   |   Topic: frauddb.frauddb.activity_log                                         |   |
|   |   Partition 0: [Offset 0] -> [Offset 1] -> ... -> [Offset 217] (High-Watermark)|   |
|   +-------------------------------------------------------------------------------+   |
+------------------------------------------------------│--------------------------------+
                                                       ▼
+---------------------------------------------------------------------------------------+
|                               DATABRICKS PROCESSING TIER                              |
|                         (Databricks Asset Bundle Job: mysql-cdc-stream)               |
|                                                                                       |
|   Cluster: Standard_D2ds_v6 (SingleNode, 8 GB Memory, 2 vCPUs)                        |
|   Runtime: DBR 15.4 LTS (Apache Spark 3.5.0, Scala 2.12)                              |
|                                                                                       |
|   +-------------------------------------------------------------------------------+   |
|   | 1. KafkaClient: Low-level socket verify + batch bounded offset discovery      |   |
|   +-------------------------------------------------------------------------------+   |
|                                          │                                            |
|                                          ▼                                            |
|   +-------------------------------------------------------------------------------+   |
|   | 2. SchemaManager: Catalog/Schema/Volume verification + table existence check  |   |
|   |    - If table missing -> CREATE TABLE DDL (29 columns + autoOptimize)         |   |
|   |    - If table present -> ALTER TABLE ADD COLUMN for upstream schema drift     |   |
|   +-------------------------------------------------------------------------------+   |
|                                          │                                            |
|                                          ▼                                            |
|   +-------------------------------------------------------------------------------+   |
|   | 3. StreamWriter: Structured Streaming readStream -> transform -> writeStream  |   |
|   |    - Trigger: 10 seconds continuous micro-batch                               |   |
|   |    - Checkpoint: /Volumes/fradulenttransactionmonitoring/bronze/...           |   |
|   +-------------------------------------------------------------------------------+   |
+------------------------------------------------------│--------------------------------+
                                                       ▼
+---------------------------------------------------------------------------------------+
|                              UNITY CATALOG STORAGE TIER                               |
|                            (Azure ADLS Gen2 Managed Storage)                          |
|                                                                                       |
|   Catalog : fradulenttransactionmonitoring                                            |
|   Schema  : bronze                                                                    |
|   Table   : activity_log_staging (Delta Lake Format)                                  |
|                                                                                       |
|   +-------------------+  +--------------------+  +--------------------------------+   |
|   | 16 Business Cols  |  | 1 Raw Payload Col  |  | 12 Stream & CDC Metadata Cols  |   |
|   | id, user_id,      |  | raw_payload (JSON) |  | _cdc_op, _cdc_ts_ms,           |   |
|   | event_type, ...   |  | (100% preservation)|  | _kafka_offset, _ingested_at... |   |
|   +-------------------+  +--------------------+  +--------------------------------+   |
+---------------------------------------------------------------------------------------+
```

---

### Diagram 2: Metadata-Driven Decision and Schema Lifecycle

```text
                      [ Pipeline Execution Begins ]
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │ Socket Reachability Check to Brokers │
                 │        Host: 34.228.32.86:9092       │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │  Step 1: Inspect Kafka Stream Batch  │
                 │   - Read earliest to latest offsets  │
                 │   - Discover per-partition counts   │
                 │   - Sample live payload JSON         │
                 │   - Infer Spark SQL column types     │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │ Ensure Unity Catalog Environment     │
                 │  - Catalog: fradulenttransaction...  │
                 │  - Schema:  bronze                   │
                 │  - Volume:  fraud_checkpoints        │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │ Step 2: Does Target Table Exist?     │
                 │  spark.catalog.tableExists(table)    │
                 └──────────────────┬───────────────────┘
                                    │
                  ┌─────────────────┴─────────────────┐
                  │                                   │
             [ Table False ]                     [ Table True ]
                  │                                   │
                  ▼                                   ▼
   ┌──────────────────────────────┐    ┌──────────────────────────────┐
   │ Step 3A: Create Table        │    │ Step 3B: Synchronize Columns │
   │ - 16 Discovered business cols│    │ - Get existing table columns │
   │ - raw_payload (STRING)       │    │ - Compare with Kafka payload │
   │ - CDC metadata columns       │    │ - If missing columns found:  │
   │ - Kafka stream audit columns │    │   ALTER TABLE ADD COLUMN     │
   │ - Delta autoOptimize options │    │ - If no missing columns:     │
   │ DDL: CREATE TABLE IF NOT...  │    │   Proceed with direct write  │
   └──────────────┬───────────────┘    └──────────────┬───────────────┘
                  │                                   │
                  └─────────────────┬─────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │ Step 3C: Continuous Stream Ingestion │
                 │ - PySpark Structured Streaming       │
                 │ - Extract CDC 'after' structure      │
                 │ - Append audit metadata              │
                 │ - Write to Delta with checkpoint     │
                 │ - Micro-batch trigger: 10 seconds    │
                 └──────────────────┬───────────────────┘
                                    │
                                    ▼
                 ┌──────────────────────────────────────┐
                 │          awaitTermination()          │
                 │ Continuous Ingestion Loop Running    │
                 └──────────────────────────────────────┘
```

---

### Diagram 3: Directory and File Dependency Map

```text
+---------------------------------------------------------------------------------------+
| databricks.yml (DAB Root Specification)                                               |
| - Defines bundle name, workspace paths, single-node cluster config, job task          |
+---------------------------------------------------------------------------------------+
                                           │ references
                                           ▼
+---------------------------------------------------------------------------------------+
| src/main.py (Application Driver Entry Point)                                          |
| - Resolves sys.path for Databricks cluster driver                                     |
| - Parses CLI arguments (--catalog, --schema, --table, --topic, etc.)                  |
| - Initializes SparkSession                                                            |
| - Instantiates CDCPipeline and triggers run()                                         |
+---------------------------------------------------------------------------------------+
           │                                 │                              │
           │ loads                           │ orchestrates                 │ uses
           ▼                                 ▼                              ▼
+---------------------+           +--------------------+         +--------------------+
| src/cdc/config.py   |           | src/cdc/pipeline.py|         | config/            |
| - PipelineConfig    |           | - Master workflow  |         | pipeline_          |
| - SourceConfig      |           |   orchestrator     |         | metadata.json      |
| - TargetConfig      |           | - Phases 1 to 6    |         | - Baseline config  |
| - GovernanceConfig  |           +--------------------+         +--------------------+
+---------------------+                      │
                                             │ delegates tasks to
                      ┌──────────────────────┼──────────────────────┐
                      ▼                      ▼                      ▼
           +---------------------++--------------------+ +--------------------+
           |src/cdc/             ||src/cdc/            | |src/cdc/            |
           |kafka_client.py      ||schema_manager.py   | |stream_writer.py    |
           |                     ||                    | |                    |
           |- Socket connectivity||- Catalog & volume  | |- PySpark stream    |
           |  diagnostics        ||  provisioning      | |  transformations   |
           |- Batch offset probe ||- Table existence   | |- CDC payload       |
           |- ReadStream builder ||  verification      | |  unpacking         |
           |                     ||- Type inference    | |- Delta writeStream |
           |                     ||- DDL table creation| |  execution         |
           |                     ||- Schema evolution  | |                    |
           +---------------------++--------------------+ +--------------------+
                      ▲                      ▲
                      │ diagnostic tool      │ unit tested by
           +---------------------++--------------------+
           |scripts/             ||tests/              |
           |check_kafka.py       ||test_schema_        |
           |validate_            ||manager.py          |
           |reconciliation.py    ||                    |
           +---------------------++--------------------+
```

---

## 3. Directory and File Architecture

The repository adheres to the standard Databricks Asset Bundle (DAB) structure:

```text
cdc-pipeline/
├── databricks.yml                   <- Master DAB declarative deployment configuration
├── README.md                        <- Project quickstart and operator guide
├── ARCHITECTURE.md                  <- Complete technical and architectural specification
├── INFRASTRUCTURE.md                <- Plain-language infrastructure & compute summary
├── config/
│   └── pipeline_metadata.json       <- Metadata configuration for topics, tables, and rules
├── src/
│   ├── main.py                      <- Executable driver script invoked by Databricks task
│   └── cdc/
│       ├── __init__.py              <- Python package initialization
│       ├── config.py                <- Strongly typed configuration models and loaders
│       ├── kafka_client.py          <- Kafka network checks, stream inspection, readStream
│       ├── schema_manager.py        <- UC verification, table creation, and column sync
│       ├── stream_writer.py         <- Stream transformation and Delta writeStream execution
│       └── pipeline.py              <- Master pipeline coordinator
├── notebooks/
│   ├── 01_bronze_to_silver.py       <- Databricks notebook: CDC deduplication, payload parsing, silver sink
│   └── 02_silver_to_gold.py         <- Databricks notebook: User risk profiles, hourly KPIs, device mart
├── scripts/
│   ├── check_kafka.py               <- Standalone raw-socket Kafka topic and offset prober
│   └── validate_reconciliation.py   <- End-to-end source vs destination reconciliation tool
└── tests/
    └── test_schema_manager.py       <- Offline unit tests for type inference & schema sync
```

---

### Detailed Responsibility of Each File

#### 1. `databricks.yml` (Bundle Deployment Specification)
- **Role**: Declarative configuration file for the Databricks CLI bundle engine.
- **Key Responsibilities**:
  - Defines the bundle name (`cdc-pipeline`) and development target environment.
  - Declares dynamic bundle variables (`catalog`, `schema`, `table`, `kafka_bootstrap_servers`, `kafka_topic`, `trigger_mode`).
  - Provisions compute using a cost-efficient SingleNode cluster:
    - Node Type: `Standard_D2ds_v6` (2 vCPUs, 8 GiB memory).
    - `num_workers: 0` (Spark runs in `local[*]` mode on the driver).
    - Runtime: `15.4.x-scala2.12` (Long Term Support).
  - Declares the Databricks Workflow Job (`mysql-cdc-stream`):
    - Configured with `continuous: { pause_status: "UNPAUSED" }` for continuous execution.
    - Executes `src/main.py` via `spark_python_task` with CLI parameter pass-through.

#### 2. `config/pipeline_metadata.json` (Metadata Governance Contract)
- **Role**: Centralized JSON specification governing pipeline operational boundaries.
- **Key Responsibilities**:
  - `source`: Specifies Kafka broker address (`34.228.32.86:9092`), topic name (`frauddb.frauddb.activity_log`), starting offsets (`earliest`), rate-limiting (`max_offsets_per_trigger: 10000`), and data loss strategy.
  - `target`: Defines Unity Catalog hierarchy (`fradulenttransactionmonitoring.bronze.activity_log_staging`), format (`delta`), and checkpoint volume path (`/Volumes/...`).
  - `metadata_governance`: Flags controlling autonomous behavior:
    - `auto_create_table: true` (creates Delta table if absent).
    - `auto_add_new_columns: true` (executes `ALTER TABLE` on schema drift).
    - `include_raw_payload: true` (retains full Debezium message).
    - `include_cdc_metadata: true` (extracts binlog file, position, operation).
    - `include_kafka_metadata: true` (records partition, offset, timestamp).

#### 3. `src/main.py` (Driver Entrypoint)
- **Role**: Main executable executed on the Databricks driver node.
- **Key Responsibilities**:
  - Resolves `sys.path` dynamically so custom modules under `src/cdc` can be imported across varied cluster runtime environments.
  - Parses command-line arguments passed from `databricks.yml` task definitions.
  - Merges CLI overrides with `config/pipeline_metadata.json`.
  - Acquires the active `SparkSession`.
  - Instantiates `CDCPipeline` and triggers execution.

#### 4. `src/cdc/config.py` (Configuration Abstraction)
- **Role**: Type-safe configuration domain model.
- **Key Responsibilities**:
  - Provides `@dataclass` definitions: `SourceConfig`, `TargetConfig`, `MetadataGovernanceConfig`, and `PipelineConfig`.
  - Exposes `load_config(path)` which tests multiple candidate locations (CLI flag, environment variable, bundle workspace path, local relative path) to load metadata reliably.

#### 5. `src/cdc/kafka_client.py` (Kafka Transport Specialist)
- **Role**: Encapsulates network verification, stream inspection, and stream creation.
- **Key Responsibilities**:
  - `verify_connectivity()`: Runs TCP socket checks against broker ports before Spark initializes, providing immediate failure diagnosis for firewall or security group misconfigurations.
  - `inspect_kafka_stream()`: Executes a bounded batch read (`spark.read.format("kafka")`) to discover record count, partition high-watermarks, and sample messages for downstream schema inference.
  - `create_read_stream()`: Configures the streaming consumer (`spark.readStream.format("kafka")`) with starting offsets, rate limits, and fail-on-data-loss policies.

#### 6. `src/cdc/schema_manager.py` (Unity Catalog and Table Lifecycle Engine)
- **Role**: Manages Unity Catalog environments, type inference, table creation, and schema evolution.
- **Key Responsibilities**:
  - `ensure_catalog_and_schema()`: Validates and creates the catalog, schema, and volume if missing.
  - `table_exists()`: Checks whether the target Delta table exists via `spark.catalog.tableExists()`.
  - `extract_incoming_fields()`: Parses sampled Debezium JSON payloads to discover business fields and map runtime types to Spark SQL types.
  - `create_target_table()`: Generates the 29-column Delta Lake DDL and creates the managed table.
  - `sync_table_columns()`: Compares existing columns against discovered incoming fields and issues `ALTER TABLE ... ADD COLUMN` DDL statements when upstream changes occur.

#### 7. `src/cdc/stream_writer.py` (Stream Processing and Delta Sink)
- **Role**: Handles stream transformation and persistent Delta writing.
- **Key Responsibilities**:
  - `transform_stream()`: Unpacks Debezium CDC envelopes:
    - Extracts `after` payload fields into dedicated, typed columns.
    - Preserves unparsed message string into `raw_payload`.
    - Extracts Debezium metadata (`op`, `ts_ms`, source metadata).
    - Enriches with Kafka stream coordinates (`_kafka_topic`, `_kafka_partition`, `_kafka_offset`, `_kafka_timestamp`, `_ingested_at`).
  - `write_to_delta()`: Launches `writeStream` to the Delta table with checkpointing and micro-batch triggers.

#### 8. `src/cdc/pipeline.py` (Master Workflow Coordinator)
- **Role**: Orchestrates the multi-step metadata-driven lifecycle cleanly.
- **Key Responsibilities**:
  - Connects `KafkaClient`, `SchemaManager`, and `StreamWriter`.
  - Executes Step 1 (inspect Kafka) -> Step 2 (check/create table or sync columns) -> Step 3 (start stream).

#### 9. `scripts/check_kafka.py` (Zero-Dependency Kafka Diagnostics)
- **Role**: Standalone local terminal utility using raw Python sockets and binary protocol packing.
- **Key Responsibilities**:
  - Queries Kafka Metadata API (Key 3) and Offset API (Key 2).
  - Reports earliest offset, latest offset, partition count, and backlog without requiring Spark or JVM.

#### 10. `scripts/validate_reconciliation.py` (Data Reconciliation Engine)
- **Role**: End-to-end data integrity validator comparing Kafka and Databricks.
- **Key Responsibilities**:
  - Reconciles Kafka high-watermark offset against Delta `MAX(_kafka_offset)`.
  - Verifies zero message loss (`COUNT(*)` equivalence).
  - Checks ID sequence continuity and flags gaps.
  - Validates zero-null primary key invariants.

#### 11. `tests/test_schema_manager.py` (Offline Test Suite)
- **Role**: Offline CI/CD unit testing without Databricks dependencies.
- **Key Responsibilities**:
  - Tests Python-to-Spark type inference.
  - Tests schema difference detection and `ALTER TABLE` generation using a mock Spark session.

---

## 4. Deep Technical Dive: How the Tables Are Created and Maintained

### Step 1: Upstream Event Discovery
When MySQL changes occur, Debezium publishes JSON messages to Kafka. Each record contains a payload envelope:
```json
{
  "before": null,
  "after": {
    "id": 218,
    "event_id": "evt_1792582324679_3468",
    "event_type": "login",
    "event_time": "2026-10-21 11:32:04",
    "user_id": 631,
    "account_id": 5871,
    "transaction_id": 596208,
    "session_id": "sess_8741",
    "device_id": "dev_392",
    "ip_address": "198.51.100.44",
    "geo_country": "US",
    "channel": "mobile",
    "source_system": "auth_service",
    "severity": "LOW",
    "payload": "{\"auth_method\": \"biometric\"}",
    "created_at": "2026-10-21 11:32:04"
  },
  "source": {
    "version": "2.4.0.Final",
    "connector": "mysql",
    "name": "frauddb",
    "ts_ms": 1792582324000,
    "db": "frauddb",
    "table": "activity_log",
    "file": "mysql-bin.000003",
    "pos": 15420
  },
  "op": "c",
  "ts_ms": 1792582324680
}
```

The pipeline reads sample records and passes them to `SchemaManager.extract_incoming_fields()`, which dynamically discovers all 16 business columns and their types:
- Integer fields -> `BIGINT`
- Decimal fields -> `DOUBLE`
- Timestamps and strings -> `STRING`
- Booleans -> `BOOLEAN`

---

### Step 2: Target Table Creation DDL
If the table `fradulenttransactionmonitoring.bronze.activity_log_staging` does not exist, `SchemaManager.create_target_table()` constructs and executes the following DDL:

```sql
CREATE TABLE IF NOT EXISTS `fradulenttransactionmonitoring`.`bronze`.`activity_log_staging` (
    -- 1. Discovered Business Attributes (from Debezium 'after' payload)
    `id` BIGINT,
    `event_id` STRING,
    `event_type` STRING,
    `event_time` STRING,
    `user_id` BIGINT,
    `account_id` BIGINT,
    `transaction_id` BIGINT,
    `session_id` STRING,
    `device_id` STRING,
    `ip_address` STRING,
    `geo_country` STRING,
    `channel` STRING,
    `source_system` STRING,
    `severity` STRING,
    `payload` STRING,
    `created_at` STRING,

    -- 2. Complete Unparsed Payload (Zero Data Loss Guarantee)
    `raw_payload` STRING,

    -- 3. Debezium CDC Metadata Attributes
    `_cdc_op` STRING,
    `_cdc_ts_ms` BIGINT,
    `_cdc_source_db` STRING,
    `_cdc_source_table` STRING,
    `_cdc_binlog_file` STRING,
    `_cdc_binlog_pos` BIGINT,

    -- 4. Kafka Stream Audit Coordinates
    `_kafka_key` STRING,
    `_kafka_topic` STRING,
    `_kafka_partition` INT,
    `_kafka_offset` BIGINT,
    `_kafka_timestamp` TIMESTAMP,
    `_ingested_at` TIMESTAMP
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true'
);
```

### Table Properties Explained:
- `USING DELTA`: Creates an ACID-compliant Delta Lake table on Azure Data Lake Storage (ADLS Gen2).
- `delta.autoOptimize.optimizeWrite = true`: Dynamically optimizes partition file sizes during writes, mitigating the "small files problem" common to streaming pipelines.
- `delta.autoOptimize.autoCompact = true`: Automatically compacts small files into larger 128 MB files in the background after micro-batches complete.

---

### Step 3: Schema Drift and Column Synchronization
If the table already exists, the pipeline executes `SchemaManager.sync_table_columns()`.

```text
Existing Columns in Delta Table: 29 columns
Incoming Fields in Kafka Stream : 30 fields (e.g., 'risk_score' added upstream in MySQL)
Difference Detected             : ['risk_score'] missing in Databricks

Action Executed:
ALTER TABLE `fradulenttransactionmonitoring`.`bronze`.`activity_log_staging`
ADD COLUMN `risk_score` DOUBLE;
```

This guarantees that upstream `ALTER TABLE` modifications in MySQL never break the Databricks streaming sink.

---

### Step 4: Stream Ingestion and Checkpointing
The streaming writer executes:
```python
writer = (
    transformed_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "/Volumes/fradulenttransactionmonitoring/bronze/fraud_checkpoints/activity_log_staging")
    .option("mergeSchema", "true")
    .trigger(processingTime="10 seconds")
)
query = writer.toTable("fradulenttransactionmonitoring.bronze.activity_log_staging")
```

- **Output Mode**: `append` preserves complete historical event log.
- **Checkpoint Location**: Stored in a Unity Catalog Volume (`/Volumes/...`). This persists streaming state (offsets processed) across cluster restarts, guaranteeing exactly-once semantics.
- **Merge Schema**: Provides a secondary safety net allowing Delta Lake to evolve schemas automatically if unexpected columns appear in streaming micro-batches.
- **Trigger**: Runs micro-batches every 10 seconds to balance ingestion latency against compute cost.

---

## 5. Summary of Verification and Diagnostic Commands

| Task | Command | Description |
| :--- | :--- | :--- |
| **Validate DAB Bundle** | `databricks bundle validate` | Validates YAML syntax and resource definitions |
| **Deploy Bundle to Azure** | `databricks bundle deploy` | Deploys code and schedules jobs to Databricks workspace |
| **Run Pipeline Job** | `databricks bundle run mysql-cdc-stream` | Triggers immediate execution of the streaming job |
| **Inspect Kafka Broker** | `python3 scripts/check_kafka.py` | Queries broker partition offsets and unconsumed counts |
| **Validate Reconciliation** | `python3 scripts/validate_reconciliation.py` | Validates offset continuity and data consistency |
| **Run Unit Tests** | `PYTHONPATH=. python3 tests/test_schema_manager.py` | Validates type inference and schema sync logic |
