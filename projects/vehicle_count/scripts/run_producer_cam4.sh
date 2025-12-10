#!/bin/bash

set -e

echo "[Producer-CAM4] 🏍️ Starting Vehicle Kafka Producer (Camera 4)..."

python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer.py CAM_4 /opt/airflow/projects/vehicle_count/data/cam4.mp4

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer-CAM4] ✅ Completed successfully."
else
  echo "[Producer-CAM4] ❌ Failed with exit code $status."
fi

exit $status
