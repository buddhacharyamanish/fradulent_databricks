# Databricks notebook source
# import
import json
from functools import reduce
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, TimestampType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

col = F.col

# Catalog and Schema
CATALOG = "fradulenttransactionmonitoring"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.activity_log_staging"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.activity_log_parsed"

# Checkpoint path
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/fraud_checkpoints/silver_activity_log"

# Primary key used for CDC deduplication and merge
PRIMARY_KEYS = ["id"]

# Columns that I want to exclude
EXCLUDE_COLUMNS = ["raw_payload"]

# Nested JSON string 
MAX_JSON_STRING_UNWRAP_DEPTH = 3

# Enable autoMerge mode
try:
    spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
except Exception:
    pass

print(f"Source Bronze Table : {BRONZE_TABLE}")
print(f"Target Silver Table : {SILVER_TABLE}")
print(f"Streaming Checkpoint: {CHECKPOINT_PATH}")
print(f"Primary Key(s)      : {PRIMARY_KEYS}")
print("Step 1 configuration completed successfully.")

# 2nd part Testing

bronze_df = spark.table(BRONZE_TABLE)
total_bronze_records = bronze_df.count()

print(f"Total Bronze records: {total_bronze_records}")
print(f"Total columns in Bronze: {len(bronze_df.columns)}")

def detect_json_columns(df, sample_size=50, exclude_cols=EXCLUDE_COLUMNS):
    """
    DataFrame to find which column contain valid JSON object strings.
    """
    string_cols = [
        f.name for f in df.schema.fields 
        if f.dataType.simpleString() == "string" and f.name not in exclude_cols
    ]
    if not string_cols:
        return []
    
    sample_rows = df.select(*[F.col(f"`{c}`") for c in string_cols]).limit(sample_size).collect()
    json_cols = []
    
    for col_name in string_cols:
        values = [r[col_name] for r in sample_rows if r[col_name] is not None and str(r[col_name]).strip() != ""]
        is_json = False
        for v in values[:5]:
            v_str = str(v).strip()
            if (v_str.startswith("{") and v_str.endswith("}")) or (v_str.startswith("[") and v_str.endswith("]")):
                try:
                    json.loads(v_str)
                    is_json = True
                    break
                except Exception:
                    pass
        if is_json:
            json_cols.append(col_name)
            
    return json_cols

# Run automated JSON column discovery
detected_json_columns = detect_json_columns(bronze_df)
print(f"\nDiscovered JSON Columns ({len(detected_json_columns)}): {detected_json_columns}")

# Discovered non-JSON base columns to retain
base_columns_to_retain = [
    c for c in bronze_df.columns 
    if c not in detected_json_columns and c not in EXCLUDE_COLUMNS
]
print(f"Discovered Non-JSON Base Columns ({len(base_columns_to_retain)}): {base_columns_to_retain}")

# Preview sample rows showing detected JSON columns alongside identifiers
display(bronze_df.select(*[F.col(f"`{c}`") for c in detected_json_columns + ["id", "event_id", "event_type"]]).limit(5))

# 3rd Part

def infer_json_schema_ddl(df, json_col, sample_size=1000):
    """
    Dynamically infers the unified DDL schema for a JSON string column.
    Captures both flat fields and deeply nested struct hierarchies.
    """
    sample_df = (
        df.filter(F.col(f"`{json_col}`").isNotNull() & (F.trim(F.col(f"`{json_col}`")) != ""))
        .select(F.col(f"`{json_col}`").alias("_json_str"))
        .limit(sample_size)
    )
    
    if sample_df.isEmpty():
        raise ValueError(f"No non-null JSON strings found in column '{json_col}'.")
    
    try:
        # Native Spark 3.4+ / Databricks 13+ / Serverless method
        inferred_ddl = sample_df.agg(F.expr("schema_of_json_agg(_json_str)")).first()[0]
    except Exception as err:
        print(f"schema_of_json_agg note for '{json_col}': {err}; using schema_of_json fallback...")
        first_json = sample_df.first()["_json_str"]
        inferred_ddl = sample_df.select(F.schema_of_json(F.lit(first_json))).first()[0]
        
    return inferred_ddl

# Infer schema for all detected JSON columns
json_schemas = {}
for j_col in detected_json_columns:
    ddl = infer_json_schema_ddl(bronze_df, j_col)
    json_schemas[j_col] = ddl
    print(f"\nInferred DDL for '{j_col}':")
    print(f"  {ddl}")

# 4th Part

def extract_hierarchical_json_leaves(main_col, struct_type):
    """
    !!!!!! THis is the tricky Part !!!!!!
    Extracts leaf expressions from a StructType with hierarchical container naming.
    
    Rule:
      - If a field is an inner JSON container (e.g. column2, column3 inside payload),
        the parsed columns use the container name as the prefix:
        column2_sub_a, column3_sub_x
      - If a field is a top-level primitive (e.g. column1),
        it uses main_col_column1 (e.g. payload_column1).
      - If this is a Debezium envelope (schema + payload),
        it unwraps payload directly (e.g. _kafka_key_id).
    """
    field_names = [f.name for f in struct_type.fields]
    
    # Special handling: Debezium / Kafka Connect envelope
    if "schema" in field_names and "payload" in field_names:
        payload_field = next(f for f in struct_type.fields if f.name == "payload")
        if isinstance(payload_field.dataType, StructType):
            return _extract_sub_struct(main_col, payload_field.dataType, parent_path="`payload`")
            
    # Standard struct: iterate over all fields
    results = []
    for f in struct_type.fields:
        if isinstance(f.dataType, StructType):
            # Inner container (e.g. column2, column3): use the inner container name as prefix!
            sub_results = _extract_sub_struct(f.name, f.dataType, parent_path=f"`{f.name}`")
            results.extend(sub_results)
        else:
            # Primitive field directly on main_col (e.g. column1 inside payload)
            col_name = f"{main_col}_{f.name}"
            results.append((f"`{f.name}`", col_name))
            
    return results

def _extract_sub_struct(container_prefix, struct_type, parent_path=""):
    """
    Recursively extracts leaf fields using the container name as prefix.
    """
    results = []
    for f in struct_type.fields:
        curr_path = f"{parent_path}.`{f.name}`" if parent_path else f"`{f.name}`"
        if isinstance(f.dataType, StructType):
            results.extend(_extract_sub_struct(f"{container_prefix}_{f.name}", f.dataType, curr_path))
        else:
            col_name = f"{container_prefix}_{f.name}"
            results.append((curr_path, col_name))
    return results

# Build pre-computed leaf expressions for each JSON column
json_leaf_expressions = {}
for j_col, ddl in json_schemas.items():
    sample_parsed = bronze_df.select(F.from_json(F.col(f"`{j_col}`"), ddl).alias("_parsed")).limit(1)
    parsed_type = sample_parsed.schema["_parsed"].dataType
    if isinstance(parsed_type, StructType):
        leaves = extract_hierarchical_json_leaves(j_col, parsed_type)
        json_leaf_expressions[j_col] = leaves
        print(f"\nHierarchical Flattening Plan for '{j_col}' ({len(leaves)} leaf columns):")
        for path, col_name in leaves:
            print(f"  - {col_name:<35} <- path {path}")

# 5Th part

def transform_to_silver(df, json_ddl_map, leaf_expressions_map, pk_cols=PRIMARY_KEYS, exclude_cols=EXCLUDE_COLUMNS):
    """
    Pure DataFrame transformation pipeline:
    Parses all JSON columns, flattens leaves with container naming,
    standardizes timestamps, and deduplicates to the latest entity state.
    """
    current_df = df
    new_cols_to_select = []
    
    # 1. Unpack all JSON columns using pre-computed leaf expressions
    for json_col, ddl in json_ddl_map.items():
        if json_col not in current_df.columns:
            continue
            
        temp_col = f"_{json_col}_parsed_temp"
        current_df = current_df.withColumn(temp_col, F.from_json(F.col(f"`{json_col}`"), ddl))
        
        for path, col_name in leaf_expressions_map.get(json_col, []):
            new_cols_to_select.append(
                F.col(f"`{temp_col}`.{path}").alias(col_name)
            )
            
    # 2. Retain all non-JSON base columns (raw JSON strings are excluded!)
    all_json_cols = set(json_ddl_map.keys())
    retained_base_cols = [
        F.col(f"`{c}`") for c in df.columns
        if c not in all_json_cols and c not in exclude_cols
    ]
    
    # 3. Create flattened DataFrame
    transformed_df = current_df.select(*retained_base_cols, *new_cols_to_select)
    
    # 4. Standardize timestamps
    for date_col in ["event_time", "created_at"]:
        if date_col in transformed_df.columns:
            transformed_df = transformed_df.withColumn(date_col, F.to_timestamp(F.col(f"`{date_col}`")))
            
    transformed_df = transformed_df.withColumn("_silver_processed_at", F.current_timestamp())
    
    # 5. Deduplicate to get the latest record per primary key
    window_spec = Window.partitionBy(*[f"`{k}`" for k in pk_cols]).orderBy(
        F.col("_cdc_ts_ms").desc(),
        F.col("_kafka_offset").desc()
    )
    
    latest_df = (
        transformed_df
        .withColumn("_rn", F.row_number().over(window_spec))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    
    # 6. Filter out records deleted in MySQL
    live_df = latest_df.filter(F.col("_cdc_op") != "d")
    
    return live_df

# Apply transformation to the Bronze data
silver_ready_df = transform_to_silver(bronze_df, json_schemas, json_leaf_expressions)

print(f"Bronze raw count    : {total_bronze_records}")
print(f"Silver active count : {silver_ready_df.count()}")
print(f"Silver column count : {len(silver_ready_df.columns)}")

display(silver_ready_df.limit(5))

# 6Th Part

def format_table_identifier(table_name: str) -> str:
    """Properly backtick-quotes a 1, 2, or 3-level table identifier."""
    parts = [p.strip("`") for p in table_name.split(".")]
    return ".".join(f"`{p}`" for p in parts)

def check_table_exists(spark_session, target_table: str) -> bool:
    """Serverless-safe check for table existence."""
    try:
        return spark_session.catalog.tableExists(target_table)
    except Exception:
        try:
            spark_session.table(target_table)
            return True
        except Exception:
            return False

def sync_silver_table_schema(spark_session, target_table, catalog_name, schema_name, source_df):
    """
    Dynamically manages the target Silver table lifecycle:
    - Ensures the schema exists in Unity Catalog.
    - If the table does not exist: creates the Delta table from source_df schema.
    - If the table exists: checks for new columns and executes ALTER TABLE ADD COLUMN.
    """
    # 1. Ensure Silver schema exists in Unity Catalog
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")
    
    # 2. Case A: Table does not exist -> Create table
    if not check_table_exists(spark_session, target_table):
        print(f"Target table '{target_table}' does not exist. Creating table dynamically...")
        source_df.limit(0).write.format("delta").saveAsTable(target_table)
        print(f"Successfully created '{target_table}' with {len(source_df.columns)} columns.")
        return []
    
    # 3. Case B: Table already exists -> Reconcile schema drift
    existing_cols = {
        field.name.lower(): field.dataType.simpleString()
        for field in spark_session.table(target_table).schema.fields
    }
    incoming_fields = {
        field.name: field.dataType.simpleString()
        for field in source_df.schema.fields
    }
    
    missing_cols = {}
    for col_name, data_type in incoming_fields.items():
        if col_name.lower() not in existing_cols:
            missing_cols[col_name] = data_type
            
    if not missing_cols:
        print(f"Target table '{target_table}' is already in sync ({len(existing_cols)} columns).")
        return []
        
    print(f"Detected {len(missing_cols)} new column(s). Altering table '{target_table}':")
    added_columns = []
    quoted_table = format_table_identifier(target_table)
    for col_name, data_type in missing_cols.items():
        alter_stmt = f"ALTER TABLE {quoted_table} ADD COLUMN `{col_name}` {data_type}"
        print(f"  Executing: {alter_stmt}")
        spark_session.sql(alter_stmt)
        added_columns.append(col_name)
        
    print(f"Successfully altered table '{target_table}'. Added columns: {added_columns}")
    return added_columns

    # Run dynamic provisioning / alteration check
    added_cols = sync_silver_table_schema(spark, SILVER_TABLE, CATALOG, SILVER_SCHEMA, silver_ready_df)

# 7Th Part

(
    silver_ready_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .option("mergeSchema", "true")
    .saveAsTable(SILVER_TABLE)
)

print(f"Successfully populated Silver table: {SILVER_TABLE}")

# Verify the created Silver table
silver_table_df = spark.table(SILVER_TABLE)
print(f"Silver table row count    : {silver_table_df.count()}")
print(f"Silver table column count : {len(silver_table_df.columns)}")

# Preview populated Silver records
display(silver_table_df.orderBy(col("id").desc()).limit(10))

# 8Th Part

def write_silver_micro_batch(micro_batch_df, batch_id):
    """
    Executes dynamic parsing, schema synchronization, and CDC merge for each micro-batch.
    """
    if micro_batch_df.isEmpty():
        return
    
    # 1. Dynamically parse and flatten JSON payload for this micro-batch
    batch_flattened = parse_and_flatten_payload(micro_batch_df, JSON_COLUMN, payload_ddl)
    
    # 2. Clean and deduplicate within the micro-batch
    batch_prepared = clean_and_deduplicate_cdc(batch_flattened)
    
    # 3. Synchronize schema (creates table or runs ALTER TABLE ADD COLUMN if new columns appear)
    sync_silver_table_schema(spark, SILVER_TABLE, CATALOG, SILVER_SCHEMA, batch_prepared)
    
    # 4. Perform Delta Lake MERGE
    target_table = DeltaTable.forName(spark, SILVER_TABLE)
    
    # Match condition on primary keys
    join_condition = " AND ".join([f"target.`{k}` = source.`{k}`" for k in PRIMARY_KEYS])
    
    # Ensure older events do not overwrite newer data
    newer_condition = "source._cdc_ts_ms >= target._cdc_ts_ms"
    
    (
        target_table.alias("target")
        .merge(batch_prepared.alias("source"), join_condition)
        .whenMatchedDelete(condition=f"source._cdc_op = 'd' AND {newer_condition}")
        .whenMatchedUpdateAll(condition=f"source._cdc_op != 'd' AND {newer_condition}")
        .whenNotMatchedInsertAll(condition="source._cdc_op != 'd'")
        .execute()
    )

print("write_silver_micro_batch function registered successfully.")

# 9Th Part

print("Starting incremental Silver catch-up stream...")

stream_query = (
    spark.readStream
    .option("skipChangeCommits", "true")
    .table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(write_silver_micro_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_dynamic_catchup_stream")
    .start()
)

stream_query.awaitTermination()
print("Incremental catch-up stream finished successfully.")
print(f"Current Silver row count: {spark.table(SILVER_TABLE).count()}")

# 10Th Part

silver_final_df = spark.table(SILVER_TABLE)

# Check 1: Primary key duplicate check
duplicates = (
    silver_final_df.groupBy(*[f"`{k}`" for k in PRIMARY_KEYS])
    .count()
    .filter(col("count") > 1)
)
dup_count = duplicates.count()
print(f"Check 1 - Duplicate Primary Keys in Silver: {dup_count} (Expected: 0)")
assert dup_count == 0, f"Integrity Failure: Found {dup_count} duplicate primary keys in Silver table!"

# Check 2: Inspect known test record (id = 1)
print("\nCheck 2 - Inspecting test record (id = 1) showing extracted payload fields:")
display(silver_final_df.filter(col("id") == 1))

# Check 3: Schema and column summary
print("\nCheck 3 - Final Silver Table Column Inventory:")
for field in silver_final_df.schema.fields:
    print(f"  {field.name:<30} : {field.dataType.simpleString()}")

print("\nPipeline Summary:")
print(f"  Total events in Bronze : {spark.table(BRONZE_TABLE).count()}")
print(f"  Active rows in Silver  : {silver_final_df.count()}")
print("Silver table is fully dynamic, validated, and ready for Gold analytics.")

# 11Th Part

def dbg(batch_df, batch_id):
    print("batch", batch_id, "rows", batch_df.count())

qt = (spark.readStream.option("skipChangeCommits", "true").table(BRONZE_TABLE)
      .writeStream.foreachBatch(dbg)
      .option("checkpointLocation", CHECKPOINT_PATH + "_dbgtest")
      .trigger(availableNow=True).start())
qt.awaitTermination()