#!/bin/bash
# ==========================================
# Script: run_consumer.sh
# ==========================================

set -e

echo "[ABSA Consumer] starting Spark stream..."

spark-submit \
  --jars /opt/airflow/jars/postgresql-42.6.0.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master local[*] \
  --driver-memory 4g \
  /opt/airflow/projects/absa_streaming/scripts/absa_consumer.py

status=$?
if [ $status -eq 0 ]; then
  echo "[ABSA Consumer] ✅ Completed successfully."
else
  echo "[ABSA Consumer] ❌ Failed with exit code $status."
fi

exit $status
