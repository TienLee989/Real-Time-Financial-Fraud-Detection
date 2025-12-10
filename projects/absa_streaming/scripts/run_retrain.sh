#!/usr/bin/env bash
set -euo pipefail
echo "[Retrain] 🔁 Running ABSA retraining pipeline..."
python /opt/airflow/projects/absa_streaming/scripts/prepare_train_data.py
python /opt/airflow/projects/absa_streaming/scripts/retrain_model.py
python /opt/airflow/projects/absa_streaming/scripts/evaluate_model.py
python /opt/airflow/projects/absa_streaming/scripts/promote_model.py
