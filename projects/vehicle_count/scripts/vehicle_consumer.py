import base64, json, cv2, numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp, from_unixtime, udf
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from ultralytics import YOLO

spark = (
    SparkSession.builder
    .appName("VehicleStreamingConsumer")
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,"
            "org.postgresql:postgresql:42.7.1")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
print("[Consumer] 🚀 Spark Structured Streaming started...")

schema = StructType([
    StructField("camera_id", StringType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("video_name", StringType(), True),
    StructField("frame_id", IntegerType(), True),
    StructField("frame_data", StringType(), True)
])

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "vehicle-stream")
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_kafka
    .selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

yolo_model = YOLO("/opt/airflow/projects/vehicle_count/models/yolov8n.pt")

def detect_vehicles(frame_b64):
    try:
        img_data = base64.b64decode(frame_b64)
        nparr = np.frombuffer(img_data, np.uint8)
        frame = cv2.imdecode(nparr, cv2.IMREAD_COLOR)
        if frame is None:
            return (0, "")
        results = yolo_model(frame, verbose=False)
        vehicles = [yolo_model.names[int(b.cls)] for b in results[0].boxes if yolo_model.names[int(b.cls)] in ["car","bus","truck","motorbike"]]
        return (len(vehicles), ",".join(vehicles))
    except Exception as e:
        print(f"[UDF Error] {e}")
        return (0, "")

from pyspark.sql.types import StructType, StructField, IntegerType, StringType
detect_schema = StructType([
    StructField("count", IntegerType(), True),
    StructField("vehicle_types", StringType(), True)
])

detect_udf = udf(detect_vehicles, detect_schema)

df_detected = (
    df_parsed
    .withColumn("detection", detect_udf(col("frame_data")))
    .withColumn("count", col("detection.count"))
    .withColumn("vehicle_type", col("detection.vehicle_types"))
    .withColumn("frame_time", from_unixtime(col("timestamp")).cast("timestamp"))
    .withColumn("processed_at", current_timestamp())
    .select("camera_id", "video_name", "frame_id", "count", "vehicle_type", "frame_time", "processed_at")
)

postgres_url = "jdbc:postgresql://postgres:5432/vehicle_db"
postgres_props = {"user": "airflow", "password": "airflow", "driver": "org.postgresql.Driver"}

def write_to_postgres(batch_df, batch_id):
    cnt = batch_df.count()
    print(f"[Batch {batch_id}] Writing {cnt} records to PostgreSQL...")
    (
        batch_df.write
        .jdbc(url=postgres_url, table="vehicle_counts", mode="append", properties=postgres_props)
    )

query = (
    df_detected.writeStream
    .outputMode("update")
    .foreachBatch(write_to_postgres)
    .option("checkpointLocation", "/opt/airflow/checkpoints/vehicle_consumer_cp")
    .start()
)

print("[Consumer] 🧠 Waiting for streaming data...")
query.awaitTermination()
