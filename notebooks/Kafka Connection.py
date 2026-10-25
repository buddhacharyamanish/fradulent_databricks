# Databricks notebook source
import socket

host = "34.228.32.86"
port = 9092
kafka_broker = f"{host}:{port}"
kafka_topic = "frauddb.frauddb.activity_log"

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.settimeout(10)

try:
    sock.connect((host, port))
    print(f"SUCCESS: Databricks can reach {host}:{port}")
except Exception as e:
    print(f"FAILED: Cannot reach {host}:{port}")
    print(type(e).__name__, str(e))
finally:
    sock.close()

# COMMAND ----------

pipeline_config = [
    {
        "topic": "frauddb.frauddb.activity_log",
        "target": "main.bronze.activity_log",   # catalog.schema.table
        "pk": ["id"],
        "checkpoint": "/Volumes/main/bronze/checkpoints/activity_log",
    },
]
KAFKA_BROKER = "34.228.32.86:9092"

# COMMAND ----------

df = (spark.read.format("kafka")
      .option("kafka.bootstrap.servers", KAFKA_BROKER)
      .option("subscribe", pipeline_config[0]["topic"])
      .option("startingOffsets", "earliest")
      .option("endingOffsets", "latest")
      .load())
display(df.selectExpr("CAST(value AS STRING)").limit(5))

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

META_COLS = {"_op", "_cdc_ts_ms", "_kafka_offset"}

def make_batch_processor(cfg):
    target, pk = cfg["target"], cfg["pk"]

    def process_batch(batch_df, batch_id):
        if batch_df.isEmpty():
            return

        # 1. Unwrap the Debezium envelope
        env = batch_df.select(
            F.col("value").cast("string").alias("json"),
            F.col("offset").alias("_kafka_offset"),
        ).select(
            F.get_json_object("json", "$.payload.op").alias("_op"),
            F.get_json_object("json", "$.payload.ts_ms").cast("long").alias("_cdc_ts_ms"),
            "_kafka_offset",
            F.when(F.get_json_object("json", "$.payload.op") == "d",
                   F.get_json_object("json", "$.payload.before"))
             .otherwise(F.get_json_object("json", "$.payload.after")).alias("row_json"),
        ).where("row_json IS NOT NULL")   # drops tombstones

        # 2. Infer schema from this batch's actual data
        ddl = env.agg(F.expr("schema_of_json_agg(row_json)")).first()[0]
        rows = (env.withColumn("r", F.from_json("row_json", ddl))
                   .select("_op", "_cdc_ts_ms", "_kafka_offset", "r.*"))

        # 3. Latest event per PK within the batch
        w = Window.partitionBy(*pk).orderBy(F.col("_cdc_ts_ms").desc(), F.col("_kafka_offset").desc())
        latest = rows.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")

        # 4. Create the table if missing
        if not spark.catalog.tableExists(target):
            latest.limit(0).write.format("delta").saveAsTable(target)

        # 5. Schema evolution: add new columns (log type changes, don't auto-alter)
        tgt_fields = {f.name: f.dataType for f in spark.table(target).schema.fields}
        for f in latest.schema.fields:
            if f.name not in tgt_fields:
                spark.sql(f"ALTER TABLE {target} ADD COLUMNS (`{f.name}` {f.dataType.simpleString()})")
            elif tgt_fields[f.name] != f.dataType:
                print(f"WARN type change on {target}.{f.name}: {tgt_fields[f.name]} -> {f.dataType}")

        # 6. Upsert / delete
        data_cols = [c for c in latest.columns if c not in META_COLS]
        cond = " AND ".join(f"t.`{k}` = s.`{k}`" for k in pk)
        set_all = {f"`{c}`": f"s.`{c}`" for c in latest.columns if c != "_op"}

        (DeltaTable.forName(spark, target).alias("t")
            .merge(latest.alias("s"), cond)
            .whenMatchedDelete(condition="s._op = 'd' AND s._cdc_ts_ms >= t._cdc_ts_ms")
            .whenMatchedUpdate(condition="s._op != 'd' AND s._cdc_ts_ms >= t._cdc_ts_ms",
                               set={k: v for k, v in set_all.items()})
            .whenNotMatchedInsert(condition="s._op != 'd'",
                                  values={k: v for k, v in set_all.items()})
            .execute())
    return process_batch

queries = []
for cfg in pipeline_config:
    q = (spark.readStream.format("kafka")
         .option("kafka.bootstrap.servers", KAFKA_BROKER)
         .option("subscribe", cfg["topic"])
         .option("startingOffsets", "earliest")
         .option("failOnDataLoss", "false")
         .option("maxOffsetsPerTrigger", 100000)   # keeps each batch bounded
         .load()
         .writeStream
         .foreachBatch(make_batch_processor(cfg))
         .option("checkpointLocation", cfg["checkpoint"])
         .trigger(availableNow=True)
         .start())
    queries.append(q)

for q in queries:
    q.awaitTermination()

# COMMAND ----------

# 1. Read live stream from Kafka
kafka_streaming_raw_df = (
    spark.readStream.option("checkpointLocation", "/mnt/<your_checkpoint_path>")
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")  # or "latest" for real-time new arrivals
        .option("failOnDataLoss", "false")
        .load()
)


# 2. Cast binary key & value to readable strings
kafka_streaming_preview = kafka_streaming_raw_df.selectExpr(
    "CAST(key AS STRING) AS kafka_key",
    "CAST(value AS STRING) AS kafka_value_json",
    "topic AS kafka_topic",
    "partition AS kafka_partition",
    "offset AS kafka_offset",
    "timestamp AS kafka_timestamp"
)

# 3. Display live stream interactively in Databricks UI
# NOTE: Click 'Cancel' or stop the cell when you are done inspecting the stream
# display(kafka_streaming_preview)

# COMMAND ----------

from pyspark.sql.functions import col

# Read a batch snapshot from Kafka (bounded by earliest & latest offsets)
kafka_sample_df = (
    spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_broker)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
)

record_count = kafka_sample_df.count()
print(f"Total records currently in Kafka topic '{kafka_topic}': {record_count}")

# Display raw payload sample
display(
    kafka_sample_df
        .select(
            col("topic").alias("kafka_topic"),
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
            col("value").cast("string").alias("raw_payload_json")
        )
        .orderBy(col("offset").desc())
        .limit(5)
)