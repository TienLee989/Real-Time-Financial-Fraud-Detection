#!/bin/bash
set -e

echo "[Consumer] 🚀 Starting Spark Financial Fraud Consumer..."

spark-submit \
  --jars /opt/spark/jars/postgresql-42.6.0.jar \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --master local[*] \
  --driver-memory 4g \
  /opt/airflow/projects/financial_fraud/scripts/financial_fraud_consumer.py

status=$?
if [ $status -eq 0 ]; then
  echo "[Consumer] ✅ Completed"
else
  echo "[Consumer] ❌ Failed (exit $status)"
fi

exit $status
