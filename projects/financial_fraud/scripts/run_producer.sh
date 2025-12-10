#!/bin/bash
set -euo pipefail

SOURCE_ID=${1:-}
CSV_PATH=${2:-}

if [[ -z "$SOURCE_ID" || -z "$CSV_PATH" ]]; then
  echo "[Producer] ❌ Missing arguments."
  echo "Usage: run_producer.sh <SOURCE_ID> <CSV_PATH>"
  exit 1
fi

if [[ ! -f "$CSV_PATH" ]]; then
  echo "[Producer] ❌ CSV file not found: $CSV_PATH"
  exit 1
fi

echo "[Producer] ================================"
echo "[Producer] 🚀 Starting financial_fraud Kafka Producer"
echo "[Producer]   - SOURCE_ID : $SOURCE_ID"
echo "[Producer]   - CSV_PATH  : $CSV_PATH"
echo "[Producer]   - START_AT  : $(date -Iseconds)"
echo "[Producer] ================================"

python /opt/airflow/projects/financial_fraud/scripts/financial_fraud_producer.py \
  "$SOURCE_ID" "$CSV_PATH"

status=$?

if [[ $status -eq 0 ]]; then
  echo "[Producer] ✅ Completed successfully at $(date -Iseconds)"
else
  echo "[Producer] ❌ Failed with exit code $status at $(date -Iseconds)"
fi

exit $status
