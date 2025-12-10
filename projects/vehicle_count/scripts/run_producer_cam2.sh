#!/bin/bash

set -e
echo "[Producer-CAM2] 🏍️ Starting Vehicle Kafka Producer (Camera 2)..."

python /opt/airflow/projects/vehicle_count/scripts/vehicle_producer.py CAM_2 /opt/airflow/projects/vehicle_count/data/cam2.mp4

status=$?
if [ $status -eq 0 ]; then
  echo "[Producer-CAM2] ✅ Completed successfully."
else
  echo "[Producer-CAM2] ❌ Failed with exit code $status."
fi

exit $status
