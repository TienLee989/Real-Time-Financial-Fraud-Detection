#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import json
import ast
import operator
import logging
import time
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import joblib
import tensorflow as tf

from pyspark.sql import SparkSession, DataFrame
# Đảm bảo các hàm được import
from pyspark.sql.functions import col, from_json, current_timestamp 
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)

# ============================================================
# Logging
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [Consumer] %(message)s",
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIG
# ============================================================
KAFKA_BOOTSTRAP = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "kafka1:29092,kafka2:29093,kafka3:29094",
)
TOPIC = "financial_fraud"

MODEL_DIR = os.getenv("MODEL_DIR", "/opt/airflow/models")
MODEL_PATH = os.path.join(MODEL_DIR, "tf_fraud_model.keras")
SCALER_PATH = os.path.join(MODEL_DIR, "scaler.pkl")
RULE_PATH = "/opt/airflow/projects/financial_fraud/data/rules.json"

POSTGRES_URL = "jdbc:postgresql://postgres:5432/fraud_db"
POSTGRES_PROP = {
    "user": "airflow",
    "password": "airflow",
    "driver": "org.postgresql.Driver",
}

CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:9000/financial_fraud"
CLICKHOUSE_PROP = {
    "user": "admin",
    "password": "admin",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver",
}

# ============================================================
# Load Model, Scaler, Rules
# ============================================================
if not os.path.exists(MODEL_PATH):
    logger.error("❌ Model file not found at %s", MODEL_PATH)
    raise FileNotFoundError(MODEL_PATH)

if not os.path.exists(SCALER_PATH):
    logger.error("❌ Scaler file not found at %s", SCALER_PATH)
    raise FileNotFoundError(SCALER_PATH)

if not os.path.exists(RULE_PATH):
    logger.error("❌ Rules file not found at %s", RULE_PATH)
    raise FileNotFoundError(RULE_PATH)

logger.info("Loading TensorFlow model from %s", MODEL_PATH)
model = tf.keras.models.load_model(MODEL_PATH)

logger.info("Loading scaler from %s", SCALER_PATH)
scaler = joblib.load(SCALER_PATH)

logger.info("Loading rules from %s", RULE_PATH)
with open(RULE_PATH, encoding="utf-8") as f:
    RULES = json.load(f)
logger.info("Loaded %d rules.", len(RULES))

# ============================================================
# Features (31 feature đúng lúc train)
# ============================================================
TRAIN_FEATURE_COLS = [
    "income",
    "name_email_similarity",
    "prev_address_months_count",
    "current_address_months_count",
    "customer_age",
    "days_since_request",
    "intended_balcon_amount",
    "payment_type",
    "zip_count_4w",
    "velocity_6h",
    "velocity_24h",
    "velocity_4w",
    "bank_branch_count_8w",
    "date_of_birth_distinct_emails_4w",
    "employment_status",
    "credit_risk_score",
    "email_is_free",
    "housing_status",
    "phone_home_valid",
    "phone_mobile_valid",
    "bank_months_count",
    "has_other_cards",
    "proposed_credit_limit",
    "foreign_request",
    "source",
    "session_length_in_minutes",
    "device_os",
    "keep_alive_session",
    "device_distinct_emails_8w",
    "device_fraud_count",
    "month",
]

CATEGORICAL_COLS = [
    "employment_status",
    "payment_type",
    "housing_status",
    "source",
    "device_os",
    "month",
]

# ============================================================
# RULE ENGINE (Safe Eval)
# ============================================================
ALLOWED_OPS = {
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Eq: operator.eq,
}


def safe_eval(expr: str, value):
    """Evaluate condition like 'value > 10' một cách an toàn."""
    node = ast.parse(expr, mode="eval")

    def _eval(n):
        if isinstance(n, ast.Expression):
            return _eval(n.body)
        if isinstance(n, ast.Compare):
            left = _eval(n.left)
            for op, comp in zip(n.ops, n.comparators):
                if type(op) not in ALLOWED_OPS:
                    return False
                if not ALLOWED_OPS[type(op)](left, _eval(comp)):
                    return False
            return True
        if isinstance(n, ast.Name) and n.id == "value":
            return value
        if isinstance(n, ast.Constant):
            return n.value
        return False

    return bool(_eval(node))


def apply_rules(rec: dict):
    """Áp rules trên 1 record feature → list rule_hit."""
    hits = []
    for rid, rule in RULES.items():
        feature = rule["feature"]
        cond = rule["condition"]
        if feature in rec:
            try:
                if safe_eval(cond, rec[feature]):
                    hits.append(
                        {
                            "rule_id": rid,
                            "feature": feature,
                            "value": rec[feature],
                            "note": rule.get("note", ""),
                        }
                    )
            except Exception:
                # Nếu rule lỗi thì bỏ qua rule đó, tránh làm chết batch
                logger.exception(
                    "Error applying rule %s on feature %s", rid, feature
                )
    return hits


# ============================================================
# SPARK SESSION
# ============================================================
spark = (
    SparkSession.builder.appName("FinancialFraudConsumer")
    .config("spark.jars", "/opt/spark/jars/postgresql-42.6.0.jar")
    .config("spark.driver.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar")
    .config("spark.executor.extraClassPath", "/opt/spark/jars/postgresql-42.6.0.jar")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

logger.info("Spark session started.")

# ============================================================
# KAFKA → JSON → SPARK SCHEMA
# ============================================================
schema = StructType(
    [
        StructField("fraud_bool", IntegerType(), True),
        StructField("income", DoubleType(), True),
        StructField("name_email_similarity", DoubleType(), True),
        StructField("prev_address_months_count", DoubleType(), True),
        StructField("current_address_months_count", DoubleType(), True),
        StructField("customer_age", DoubleType(), True),
        StructField("days_since_request", DoubleType(), True),
        StructField("intended_balcon_amount", DoubleType(), True),
        StructField("payment_type", StringType(), True),
        StructField("zip_count_4w", DoubleType(), True),
        StructField("velocity_6h", DoubleType(), True),
        StructField("velocity_24h", DoubleType(), True),
        StructField("velocity_4w", DoubleType(), True),
        StructField("bank_branch_count_8w", DoubleType(), True),
        StructField("date_of_birth_distinct_emails_4w", DoubleType(), True),
        StructField("employment_status", StringType(), True),
        StructField("credit_risk_score", DoubleType(), True),
        StructField("email_is_free", IntegerType(), True),
        StructField("housing_status", StringType(), True),
        StructField("phone_home_valid", IntegerType(), True),
        StructField("phone_mobile_valid", IntegerType(), True),
        StructField("bank_months_count", DoubleType(), True),
        StructField("has_other_cards", IntegerType(), True),
        StructField("proposed_credit_limit", DoubleType(), True),
        StructField("foreign_request", IntegerType(), True),
        StructField("source", StringType(), True),
        StructField("session_length_in_minutes", DoubleType(), True),
        StructField("device_os", StringType(), True),
        StructField("keep_alive_session", IntegerType(), True),
        StructField("device_distinct_emails_8w", DoubleType(), True),
        StructField("device_fraud_count", DoubleType(), True),
        StructField("month", IntegerType(), True),
        # metadata
        StructField("event_time", DoubleType(), True),
        StructField("source_id", StringType(), True),
    ]
)

logger.info("Reading Kafka stream from %s, topic=%s", KAFKA_BOOTSTRAP, TOPIC)

df_kafka = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .option("failOnDataLoss", "false")
    .load()
)

df = (
    df_kafka.selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))
    .select("data.*")
    .withColumn("ingest_ts", current_timestamp())
)

# ============================================================
# PROCESS BATCH
# ============================================================
def process_batch(batch_df: DataFrame, batch_id: int):
    count = batch_df.count()
    if count == 0:
        logger.info("[Batch %d] Empty batch → skip.", batch_id)
        return

    logger.info("[Batch %d] Received %d record(s).", batch_id, count)

    pdf = batch_df.toPandas()
    if pdf.empty:
        logger.info("[Batch %d] Pandas DataFrame empty → skip.", batch_id)
        return

    records = pdf.to_dict(orient="records")

    # ---------------------- PREPROCESSING ----------------------
    # Bổ sung cột nếu thiếu
    for col_name in TRAIN_FEATURE_COLS:
        if col_name not in pdf.columns:
            pdf[col_name] = np.nan

    pdf_feat = pdf[TRAIN_FEATURE_COLS].copy()

    # Encode categorical (factorize theo batch – không cần encoder ngoài)
    for c in CATEGORICAL_COLS:
        pdf_feat[c] = pdf_feat[c].astype(str).fillna("missing")
        codes, _ = pd.factorize(pdf_feat[c])
        pdf_feat[c] = codes.astype(np.float32)

    # Numeric: to_numeric + fillna(mean hoặc 0)
    for c in TRAIN_FEATURE_COLS:
        if c not in CATEGORICAL_COLS:
            pdf_feat[c] = pd.to_numeric(pdf_feat[c], errors="coerce")
            mean_val = pdf_feat[c].mean()
            if np.isnan(mean_val):
                mean_val = 0.0
            pdf_feat[c] = pdf_feat[c].fillna(mean_val)

    X = pdf_feat.values.astype(np.float32)

    try:
        X_scaled = scaler.transform(X)
    except Exception as e:
        logger.exception(
            "[Batch %d] Scaler failed: %s | shape=%s", batch_id, e, X.shape
        )
        return

    scores = model.predict(X_scaled, verbose=0).flatten()
    preds = (scores > 0.5).astype(int)

    # ---------------------- WRITING TABLES ----------------------
    raw_records = []
    pred_records = []
    rules_records = []
    feature_records = []
    alert_records = []

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    now_str = now_utc.isoformat()

    for idx, (rec, score, pred) in enumerate(zip(records, scores, preds)):
        # event_time từ record (unix timestamp hoặc null)
        evt_ts = rec.get("event_time", None)
        evt_unix: float

        if isinstance(evt_ts, (datetime, pd.Timestamp)):
            if evt_ts.tzinfo is None:
                evt_ts = evt_ts.replace(tzinfo=timezone.utc)
            evt_unix = evt_ts.timestamp()
        else:
            try:
                evt_unix = float(evt_ts)
            except (TypeError, ValueError):
                evt_unix = time.time()

        # datetime UTC nhưng để naive (TIMESTAMP WITHOUT TIME ZONE)
        evt_dt = datetime.fromtimestamp(evt_unix, tz=timezone.utc).replace(tzinfo=None)

        src = rec.get("source_id", "SRC")
        txn_id = f"{int(evt_unix)}_{src}_{batch_id}_{idx}"
        rec["_transaction_id"] = txn_id

        # RAW
        raw_records.append(
            {
                "transaction_id": txn_id,
                "source_id": src,
                "event_time": evt_dt,
                "raw_payload": json.dumps(rec, ensure_ascii=False, default=str),
                "received_at": now_str,
            }
        )

        # RULE ENGINE
        rule_hits = apply_rules(rec)

        pred_records.append(
            {
                "transaction_id": txn_id,
                "prediction": int(pred),
                "fraud_score": float(score),
                "model_version": "v1",
                "violated_rules": json.dumps(
                    [h["rule_id"] for h in rule_hits],
                    ensure_ascii=False,
                    default=str,
                ),
                "violated_rule_count": len(rule_hits),
                "processed_at": now_str,
            }
        )

        for h in rule_hits:
            rules_records.append(
                {
                    "transaction_id": txn_id,
                    "rule_id": h["rule_id"],
                    "feature": h["feature"],
                    "value": float(h["value"]),
                    "note": h["note"],
                    "triggered_at": now_str,
                }
            )

        feature_records.append(
            {
                "transaction_id": txn_id,
                "features": json.dumps(rec, ensure_ascii=False, default=str),
                "created_at": now_str,
            }
        )

        if score > 0.8:
            alert_records.append(
                {
                    "transaction_id": txn_id,
                    "fraud_score": float(score),
                    "alert_level": "HIGH",
                    "reason": f"Fraud score {score:.3f}",
                    "created_at": now_str,
                }
            )

    # ---------------------- WRITE TO POSTGRES ----------------------
    try:
        # 1. fraud_raw
        if raw_records:
            df_raw = spark.createDataFrame(raw_records)
            
            # FIX LỖI CÚ PHÁP VÀ TIMESTAMP: Sử dụng CAST() chuẩn của Spark SQL
            df_raw_final = df_raw.selectExpr(
                "transaction_id",
                "source_id",
                "event_time", 
                # raw_payload là TEXT/VARCHAR (đã fix JSONB)
                "raw_payload", 
                # FIX TIMESTAMP: Ép kiểu String (received_at) sang kiểu TIMESTAMP
                "CAST(received_at AS TIMESTAMP) as received_at"
            )
            
            logger.info(
                "[Batch %d] Writing %d rows to fraud_raw (PostgreSQL)...",
                batch_id,
                len(raw_records),
            )
            
            df_raw_final.write.mode("append").jdbc(
                POSTGRES_URL,
                "fraud_raw",
                properties=POSTGRES_PROP,
            )

        # 2. fraud_predictions
        if pred_records:
            df_pred = spark.createDataFrame(pred_records)
            
            # FIX LỖI TIMESTAMP: SỬ DỤNG CAST() CHUẨN CỦA SPARK SQL
            df_pred_final = df_pred.selectExpr(
                "transaction_id",
                "prediction",
                "fraud_score",
                "model_version",
                "violated_rules",
                "violated_rule_count",
                "CAST(processed_at AS TIMESTAMP) as processed_at" # FIX TIMESTAMP
            )

            logger.info(
                "[Batch %d] Writing %d rows to fraud_predictions (PostgreSQL)...",
                batch_id,
                len(pred_records),
            )
            df_pred_final.write.mode("append").jdbc(
                POSTGRES_URL,
                "fraud_predictions",
                properties=POSTGRES_PROP,
            )

        # 3. fraud_rules_log
        if rules_records:
            df_rules = spark.createDataFrame(rules_records)
            
            # FIX LỖI TIMESTAMP: SỬ DỤNG CAST() CHUẨN CỦA SPARK SQL
            df_rules_final = df_rules.selectExpr(
                "transaction_id",
                "rule_id",
                "feature",
                "value",
                "note",
                "CAST(triggered_at AS TIMESTAMP) as triggered_at" # FIX TIMESTAMP
            )

            logger.info(
                "[Batch %d] Writing %d rows to fraud_rules_log (PostgreSQL)...",
                batch_id,
                len(rules_records),
            )
            df_rules_final.write.mode("append").jdbc(
                POSTGRES_URL,
                "fraud_rules_log",
                properties=POSTGRES_PROP,
            )

        # 4. fraud_feature_store
        if feature_records:
            df_feat = spark.createDataFrame(feature_records)
            
            # FIX LỖI TIMESTAMP: SỬ DỤNG CAST() CHUẨN CỦA SPARK SQL
            df_feat_final = df_feat.selectExpr(
                "transaction_id",
                "features", # features hiện là TEXT trong DB, không cần ép kiểu jsonb
                "CAST(created_at AS TIMESTAMP) as created_at" # FIX TIMESTAMP
            )
            
            logger.info(
                "[Batch %d] Writing %d rows to fraud_feature_store (PostgreSQL)...",
                batch_id,
                len(feature_records),
            )
            df_feat_final.write.mode("append").jdbc(
                POSTGRES_URL,
                "fraud_feature_store",
                properties=POSTGRES_PROP,
            )

        # 5. fraud_alerts
        if alert_records:
            df_alert = spark.createDataFrame(alert_records)
            
            # FIX LỖI TIMESTAMP: SỬ DỤNG CAST() CHUẨN CỦA SPARK SQL
            df_alert_final = df_alert.selectExpr(
                "transaction_id",
                "fraud_score",
                "alert_level",
                "reason",
                "CAST(created_at AS TIMESTAMP) as created_at" # FIX TIMESTAMP
            )
            
            logger.info(
                "[Batch %d] Writing %d rows to fraud_alerts (PostgreSQL)...",
                batch_id,
                len(alert_records),
            )
            df_alert_final.write.mode("append").jdbc(
                POSTGRES_URL,
                "fraud_alerts",
                properties=POSTGRES_PROP,
            )

    except Exception:
        logger.exception("[Batch %d] Error writing to PostgreSQL", batch_id)
        raise

    # ---------------------- WRITE TO CLICKHOUSE ----------------------
    try:
        if pred_records:
            df_pred_ch = spark.createDataFrame(pred_records)
            logger.info(
                "[Batch %d] Writing %d rows to fraud_predictions (ClickHouse)...",
                batch_id,
                len(pred_records),
            )
            df_pred_ch.write.mode("append").jdbc(
                CLICKHOUSE_URL,
                "fraud_predictions",
                properties=CLICKHOUSE_PROP,
            )
    except Exception:
        logger.exception("[Batch %d] Error writing to ClickHouse", batch_id)
        # Không raise để tránh kill stream chỉ vì OLAP lỗi

    logger.info("[Batch %d] ✅ Completed", batch_id)


# ============================================================
# START STREAM
# ============================================================
query = (
    df.writeStream.foreachBatch(process_batch)
    .outputMode("update")
    .option("checkpointLocation", "/opt/airflow/checkpoints/fraud_stream_cp")
    .start()
)

logger.info("Streaming started. Waiting for data...")
query.awaitTermination()