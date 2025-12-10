import os, re, json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf, lit, explode, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, FloatType, ArrayType

# ====== Config ======
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("ABSA_TOPIC", "absa-reviews")

PG_URL = os.getenv("PG_URL", "jdbc:postgresql://postgres:5432/absa_db")
PG_USER = os.getenv("PG_USER", "airflow")
PG_PWD  = os.getenv("PG_PWD",  "airflow")
CHECKPOINT = os.getenv("CHECKPOINT_DIR", "/tmp/checkpoints/absa_consumer")

MODEL_ID = os.getenv("MODEL_ID", "best_absa_hardshare.pt")

ASPECTS = ["Price","Shipping","Outlook","Quality","Size","Shop_Service","General","Others"]

# ====== Spark Init ======
spark = (
    SparkSession.builder
    .appName("ABSA_Streaming_Consumer")
    .config("spark.jars", "/opt/airflow/jars/postgresql-42.6.0.jar")
    .config("spark.driver.extraClassPath", "/opt/airflow/jars/postgresql-42.6.0.jar")
    .config("spark.executor.extraClassPath", "/opt/airflow/jars/postgresql-42.6.0.jar")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print("[Consumer] 🚀 SparkSession started.")
print(f"[Consumer] Kafka broker = {KAFKA_BROKER}, topic = {TOPIC}")
print(f"[Consumer] Postgres URL = {PG_URL}")

schema = StructType([StructField("review", StringType(), True)])

# ====== Heuristic ABSA ======
positive_kw = re.compile(r"(tốt|đẹp|ổn|oke|ok|tuyệt|hài lòng|nhanh|nhiệt tình|đúng mô tả)", re.I)
negative_kw = re.compile(r"(tệ|chậm|mùi|không|kém|sơ sài|thất vọng|không như mong)", re.I)

def infer_aspect_polarities(text: str):
    if text is None or not text.strip():
        return []
    pos = bool(positive_kw.search(text))
    neg = bool(negative_kw.search(text))
    if pos and not neg:
        sentiment = "positive"; conf = 0.80
    elif neg and not pos:
        sentiment = "negative"; conf = 0.80
    elif pos and neg:
        sentiment = "neutral"; conf = 0.55
    else:
        sentiment = "neutral"; conf = 0.50
    return [(a, sentiment, float(conf)) for a in ASPECTS]

udf_type = ArrayType(StructType([
    StructField("aspect", StringType(), False),
    StructField("sentiment", StringType(), False),
    StructField("confidence", FloatType(), False),
]))
infer_udf = udf(infer_aspect_polarities, udf_type)

# ====== Streaming pipeline ======
print("[Consumer] 🔄 Subscribing to Kafka topic ...")

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json")
       .select(from_json(col("json"), schema).alias("data"))
       .select(col("data.review").alias("review"))
)

scored = (
    parsed
    .withColumn("pred", infer_udf(col("review")))
    .withColumn("pred", explode(col("pred")))
    .select(
        col("review"),
        col("pred.aspect").alias("aspect"),
        col("pred.sentiment").alias("sentiment"),
        col("pred.confidence").alias("confidence")
    )
    .withColumn("model_id", lit(MODEL_ID))
    .withColumn("processed_at", current_timestamp())
)

# ====== Debug logging per batch ======
def foreach_batch_save(df, batch_id: int):
    count = df.count()
    print(f"\n[Consumer] 🧩 Batch {batch_id}: received {count} records")

    if count > 0:
        df.show(5, truncate=False)
        print("[Consumer] 💾 Writing batch to Postgres ...")

        (df.write
           .format("jdbc")
           .option("url", PG_URL)
           .option("dbtable", "absa_results")
           .option("user", PG_USER)
           .option("password", PG_PWD)
           .option("driver", "org.postgresql.Driver")
           .mode("append")
           .save())

        print(f"[Consumer] ✅ Batch {batch_id} saved successfully!\n")
    else:
        print(f"[Consumer] ⚠️ Batch {batch_id} empty, skipping write.\n")

# ====== Start Streaming ======
query = (
    scored.writeStream
    .outputMode("append")
    .foreachBatch(foreach_batch_save)
    .option("checkpointLocation", CHECKPOINT)
    .start()
)

print("[Consumer] 🧠 Waiting for streaming data ...")
query.awaitTermination()
