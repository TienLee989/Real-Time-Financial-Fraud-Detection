#!/bin/bash

set -e

echo "[Producer-CAM1] 🚗 Starting Vehicle Kafka Producer (Camera 1)..."

python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer.py CAM_1 /opt/airflow/projects/vehicle_count/data/cam1.mp4

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer-CAM1] ✅ Completed successfully."
else
  echo "[Producer-CAM1] ❌ Failed with exit code $status."
fi

exit $status
