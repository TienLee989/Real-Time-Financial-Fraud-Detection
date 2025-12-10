#!/bin/bash

set -e

echo "[Producer-CAM3] 🚗 Starting Vehicle Kafka Producer (Camera 3)..."

python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer.py CAM_1 /opt/airflow/projects/vehicle_count/data/cam3.mp4

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer-CAM3] ✅ Completed successfully."
else
  echo "[Producer-CAM3] ❌ Failed with exit code $status."
fi

exit $status
