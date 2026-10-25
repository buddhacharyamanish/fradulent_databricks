# Databricks notebook source
# Bronze To Silver Transformation and Job

# 1st part 

import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql.window import Window
from delta.tables import DeltaTable

col = F.col

CATALOG = "fradulenttransactionmonitoring"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.activity_log_staging"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.activity_log_parsed"

# Checkpoint
CHECKPOINT_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/fraud_checkpoints/silver_activity_log"

PRIMARY_KEYS = ["id"]
EXCLUDE_COLUMNS = ["raw_payload"]

print("Bronze:", BRONZE_TABLE, "| Silver:", SILVER_TABLE)

# 1st Part
# Detect JSON Data

def detect_json_columns(df, sample_size=50, exclude_cols=EXCLUDE_COLUMNS):
    """String columns whose sample values are valid JSON."""
    string_cols = [f.name for f in df.schema.fields
                   if f.dataType.simpleString() == "string" and f.name not in exclude_cols]
    if not string_cols:
        return []
    sample_rows = df.select(*[F.col(f"`{c}`") for c in string_cols]).limit(sample_size).collect()
    json_cols = []
    for col_name in string_cols:
        values = [r[col_name] for r in sample_rows if r[col_name] is not None and str(r[col_name]).strip() != ""]
        for v in values[:5]:
            v_str = str(v).strip()
            if (v_str.startswith("{") and v_str.endswith("}")) or (v_str.startswith("[") and v_str.endswith("]")):
                try:
                    json.loads(v_str)
                    json_cols.append(col_name)
                    break
                except Exception:
                    pass
    return json_cols


def infer_json_schema_ddl(df, json_col, sample_size=1000):
    sample_df = (df.filter(F.col(f"`{json_col}`").isNotNull() & (F.trim(F.col(f"`{json_col}`")) != ""))
                   .select(F.col(f"`{json_col}`").alias("_json_str")).limit(sample_size))
    if sample_df.isEmpty():
        raise ValueError(f"No non-null JSON strings found in column '{json_col}'.")
    try:
        return sample_df.agg(F.expr("schema_of_json_agg(_json_str)")).first()[0]
    except Exception:
        first_json = sample_df.first()["_json_str"]
        return sample_df.select(F.schema_of_json(F.lit(first_json))).first()[0]


def _extract_sub_struct(container_prefix, struct_type, parent_path=""):
    results = []
    for f in struct_type.fields:
        curr_path = f"{parent_path}.`{f.name}`" if parent_path else f"`{f.name}`"
        if isinstance(f.dataType, StructType):
            results.extend(_extract_sub_struct(f"{container_prefix}_{f.name}", f.dataType, curr_path))
        else:
            results.append((curr_path, f"{container_prefix}_{f.name}"))
    return results


def extract_hierarchical_json_leaves(main_col, struct_type):
    field_names = [f.name for f in struct_type.fields]
    if "schema" in field_names and "payload" in field_names:          # Debezium / Kafka Connect envelope
        payload_field = next(f for f in struct_type.fields if f.name == "payload")
        if isinstance(payload_field.dataType, StructType):
            return _extract_sub_struct(main_col, payload_field.dataType, parent_path="`payload`")
    results = []
    for f in struct_type.fields:
        if isinstance(f.dataType, StructType):
            results.extend(_extract_sub_struct(f.name, f.dataType, parent_path=f"`{f.name}`"))
        else:
            results.append((f"`{f.name}`", f"{main_col}_{f.name}"))
    return results


def build_json_plan(df):
    """Detect JSON columns and build the flattening plan from THIS DataFrame (so new fields are picked up)."""
    ddl_map, leaf_map = {}, {}
    for c in detect_json_columns(df):
        try:
            ddl = infer_json_schema_ddl(df, c)
        except ValueError:                      # column is all null in this batch
            continue
        parsed_type = df.select(F.from_json(F.col(f"`{c}`"), ddl).alias("_p")).schema["_p"].dataType
        if isinstance(parsed_type, StructType):  # arrays stay as plain text columns
            ddl_map[c] = ddl
            leaf_map[c] = extract_hierarchical_json_leaves(c, parsed_type)
    return ddl_map, leaf_map

# 2nd part
# Streamline json


def transform_to_silver(df, json_ddl_map, leaf_expressions_map, pk_cols=PRIMARY_KEYS,
                        exclude_cols=EXCLUDE_COLUMNS, drop_deletes=True):
    current_df = df
    new_cols_to_select = []
    for json_col, ddl in json_ddl_map.items():
        if json_col not in current_df.columns:
            continue
        temp_col = f"_{json_col}_parsed_temp"
        current_df = current_df.withColumn(temp_col, F.from_json(F.col(f"`{json_col}`"), ddl))
        for path, col_name in leaf_expressions_map.get(json_col, []):
            new_cols_to_select.append(F.col(f"`{temp_col}`.{path}").alias(col_name))

    all_json_cols = set(json_ddl_map.keys())
    retained_base_cols = [F.col(f"`{c}`") for c in df.columns
                          if c not in all_json_cols and c not in exclude_cols]

    transformed_df = current_df.select(*retained_base_cols, *new_cols_to_select)

    for date_col in ["event_time", "created_at"]:
        if date_col in transformed_df.columns:
            transformed_df = transformed_df.withColumn(date_col, F.to_timestamp(F.col(f"`{date_col}`")))
    transformed_df = transformed_df.withColumn("_silver_processed_at", F.current_timestamp())

    window_spec = Window.partitionBy(*[f"`{k}`" for k in pk_cols]).orderBy(
        F.col("_cdc_ts_ms").desc(), F.col("_kafka_offset").desc())
    latest_df = (transformed_df.withColumn("_rn", F.row_number().over(window_spec))
                 .filter(F.col("_rn") == 1).drop("_rn"))

    # Stream keeps delete events so the MERGE can remove the row from silver
    return latest_df.filter(F.col("_cdc_op") != "d") if drop_deletes else latest_df

# 3rd Part
# Create table or Alter table

def format_table_identifier(table_name: str) -> str:
    return ".".join(f"`{p.strip('`')}`" for p in table_name.split("."))


def check_table_exists(spark_session, target_table: str) -> bool:
    try:
        return spark_session.catalog.tableExists(target_table)
    except Exception:
        try:
            spark_session.table(target_table)
            return True
        except Exception:
            return False


def sync_silver_table_schema(spark_session, target_table, catalog_name, schema_name, source_df):
    spark_session.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog_name}`.`{schema_name}`")

    if not check_table_exists(spark_session, target_table):
        source_df.limit(0).write.format("delta").saveAsTable(target_table)
        print(f"Created {target_table} with {len(source_df.columns)} columns")
        return []

    existing = {f.name.lower() for f in spark_session.table(target_table).schema.fields}
    missing = {f.name: f.dataType.simpleString() for f in source_df.schema.fields
               if f.name.lower() not in existing}
    quoted = format_table_identifier(target_table)
    for name, dtype in missing.items():
        spark_session.sql(f"ALTER TABLE {quoted} ADD COLUMN `{name}` {dtype}")
    if missing:
        print(f"Added columns to {target_table}: {list(missing)}")
    return list(missing)


def align_to_target(df, target_table, session):
    """Match the target's columns and types; null-fill columns this batch does not have."""
    cols = []
    for f in session.table(target_table).schema.fields:
        if f.name in df.columns:
            cols.append(F.col(f"`{f.name}`").cast(f.dataType).alias(f.name))
        else:
            cols.append(F.lit(None).cast(f.dataType).alias(f.name))
    return df.select(*cols)

# 4th Part
# Micro-batch writer (CDC merge)

def write_silver_micro_batch(micro_batch_df, batch_id):
    if micro_batch_df.isEmpty():
        return
    session = micro_batch_df.sparkSession          # batch's own session (required on serverless)

    ddl_map, leaf_map = build_json_plan(micro_batch_df)
    batch_prepared = transform_to_silver(micro_batch_df, ddl_map, leaf_map, drop_deletes=False)

    names = [c.lower() for c in batch_prepared.columns]
    dups = sorted({n for n in names if names.count(n) > 1})
    if dups:
        raise ValueError(f"Duplicate column names after flattening: {dups}")

    sync_silver_table_schema(session, SILVER_TABLE, CATALOG, SILVER_SCHEMA, batch_prepared)
    source = align_to_target(batch_prepared, SILVER_TABLE, session)

    target = DeltaTable.forName(session, SILVER_TABLE)
    join_condition = " AND ".join(f"target.`{k}` = source.`{k}`" for k in PRIMARY_KEYS)
    newer = "source._cdc_ts_ms >= target._cdc_ts_ms"
    (target.alias("target")
        .merge(source.alias("source"), join_condition)
        .whenMatchedDelete(condition=f"source._cdc_op = 'd' AND {newer}")
        .whenMatchedUpdateAll(condition=f"source._cdc_op != 'd' AND {newer}")
        .whenNotMatchedInsertAll(condition="source._cdc_op != 'd'")
        .execute())

# 5th part
# Run: process everything new since the last run, then stop

stream_query = (
    spark.readStream
    .option("skipChangeCommits", "true")
    .table(BRONZE_TABLE)
    .writeStream
    .foreachBatch(write_silver_micro_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(availableNow=True)
    .queryName("silver_activity_log_stream")
    .start()
)
stream_query.awaitTermination()      # raises if a batch fails, so the Job run is marked failed

print("Rows read from bronze this run:", sum((p.get("numInputRows") or 0) for p in stream_query.recentProgress))

# 6th part
# Final check (fails the Job if silver has duplicate keys)

silver_final_df = spark.table(SILVER_TABLE)
dup_count = (silver_final_df.groupBy(*[f"`{k}`" for k in PRIMARY_KEYS]).count()
             .filter(col("count") > 1).count())
print(f"Duplicate primary keys in silver: {dup_count} (expected 0)")
assert dup_count == 0, f"Integrity failure: {dup_count} duplicate primary keys in {SILVER_TABLE}"
print("Silver rows:", silver_final_df.count(), "| columns:", len(silver_final_df.columns))