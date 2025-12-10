#!/usr/bin/env bash
set -euo pipefail
echo "[Producer] 💬 Starting ABSA Kafka Producer..."
python /opt/airflow/projects/absa_streaming/scripts/absa_producer.py
