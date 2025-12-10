#!/usr/bin/env bash
set -euo pipefail

# ============================================
# Config container & credentials
# ============================================
PG_CONTAINER="${PG_CONTAINER:-lee_airflow_postgres}"
PG_USER="${PG_USER:-airflow}"
FRAUD_DB="${FRAUD_DB:-fraud_db}"

CH_CONTAINER="${CH_CONTAINER:-lee_clickhouse}"
CH_USER="${CH_USER:-admin}"
CH_PASS="${CH_PASS:-admin}"
CH_DB="${CH_DB:-financial_fraud}"

SPARK_CHECKPOINT_DIR="/opt/airflow/checkpoints/fraud_stream_cp"

echo "============================================"
echo "🚨 STARTING CLEANUP AND REINITIALIZATION"
echo "============================================"

# ============================================
# 1. PostgreSQL Cleanup
# ============================================
echo "❌ [PostgreSQL] Dropping database '${FRAUD_DB}'..."

# Kiểm tra nếu DB tồn tại và xóa nó (sử dụng -tc để tránh log dài)
docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -tc "SELECT 1 FROM pg_database WHERE datname='${FRAUD_DB}'" | grep -q 1 
if [ $? -eq 0 ]; then
    # Để xóa DB đang được kết nối, cần đóng tất cả kết nối trước
    echo "   Closing active connections to ${FRAUD_DB}..."
    docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -d postgres <<'SQL'
    SELECT pg_terminate_backend(pg_stat_activity.pid)
    FROM pg_stat_activity
    WHERE pg_stat_activity.datname = '${FRAUD_DB}'
      AND pid <> pg_backend_pid();
SQL
    # Xóa DB
    docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -c "DROP DATABASE ${FRAUD_DB};"
    echo "✅ [PostgreSQL] Database '${FRAUD_DB}' dropped."
else
    echo "✅ [PostgreSQL] Database '${FRAUD_DB}' does not exist, skipping drop."
fi

# ============================================
# 2. ClickHouse Cleanup
# ============================================
echo "❌ [ClickHouse] Dropping database '${CH_DB}'..."

# Lệnh ClickHouse để xóa DB (IF EXISTS đảm bảo script không fail)
docker exec -i "${CH_CONTAINER}" clickhouse-client \
    -u "${CH_USER}" --password "${CH_PASS}" \
    --query "DROP DATABASE IF EXISTS ${CH_DB};"

echo "✅ [ClickHouse] Database '${CH_DB}' dropped."

# ============================================
# 3. Spark Checkpoint Cleanup (Rất quan trọng)
# ============================================
# Vì bạn chạy trong môi trường Airflow/Docker, thư mục checkpoint nằm trên volume
echo "❌ [Spark Streaming] Deleting checkpoint directory: ${SPARK_CHECKPOINT_DIR}..."

# Giả định thư mục /opt/airflow là thư mục gốc của project trong container worker/scheduler
# Bạn cần chạy lệnh này trên container Airflow Worker/Scheduler (nơi task chạy)
# Trong môi trường của bạn, có vẻ các container đều dùng chung volume /opt/airflow
# Ta sẽ sử dụng một container Airflow Worker/Webserver/Scheduler bất kỳ để xóa
AIRFLOW_CONTAINER="${AIRFLOW_CONTAINER:-lee_airflow_worker}" 

# Cần đảm bảo volume nơi checkpoint được lưu trữ được gắn đúng cách.
# Lệnh dưới đây sẽ xóa thư mục checkpoint bên trong container.
# Lưu ý: Nếu thư mục này được mount từ host, nó sẽ xóa cả trên host.
docker exec -i "${AIRFLOW_CONTAINER}" bash -c "rm -rf ${SPARK_CHECKPOINT_DIR}" || true

echo "✅ [Spark Streaming] Checkpoint directory removed."


echo "============================================"
echo "🎉 CLEANUP COMPLETE! Re-run the initialization script now."
echo "============================================"