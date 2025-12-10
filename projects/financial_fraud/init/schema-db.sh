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

echo "============================================"
echo "🚀 Initializing PostgreSQL (fraud_db) & ClickHouse (financial_fraud)"
echo "  - PostgreSQL container : ${PG_CONTAINER}"
echo "  - PostgreSQL user      : ${PG_USER}"
echo "  - Fraud DB (Postgres)  : ${FRAUD_DB}"
echo "  - ClickHouse container : ${CH_CONTAINER}"
echo "  - ClickHouse user      : ${CH_USER}"
echo "  - ClickHouse DB        : ${CH_DB}"
echo "============================================"

# ============================================
# 1. PostgreSQL: create fraud_db if not exists
# ============================================
echo "📦 [PostgreSQL] Checking/creating database '${FRAUD_DB}'..."

docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -tc "SELECT 1 FROM pg_database WHERE datname='${FRAUD_DB}'" | grep -q 1 \
  || docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -c "CREATE DATABASE ${FRAUD_DB};"

echo "✅ [PostgreSQL] Database '${FRAUD_DB}' ready."

# ============================================
# 2. PostgreSQL: create fraud tables
# ============================================
echo "🧱 [PostgreSQL] Creating fraud schema tables in ${FRAUD_DB}..."

docker exec -i "${PG_CONTAINER}" psql -U "${PG_USER}" -d "${FRAUD_DB}" <<'SQL'
-- ============================================
-- 1. Bảng giao dịch gốc (raw input)
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_raw (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100) UNIQUE,
    source_id VARCHAR(50),
    event_time TIMESTAMP,
    raw_payload TEXT,
    received_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- 2. Bảng kết quả inference của mô hình
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_predictions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    prediction INT,
    fraud_score FLOAT,
    model_version VARCHAR(50) DEFAULT 'v1',
    violated_rules TEXT,
    violated_rule_count INT DEFAULT 0,
    top_features TEXT,
    processed_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_fraud_predictions_txn
        FOREIGN KEY (transaction_id) REFERENCES fraud_raw(transaction_id)
        ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_pred_processed ON fraud_predictions (processed_at DESC);
CREATE INDEX IF NOT EXISTS idx_pred_score ON fraud_predictions (fraud_score DESC);

-- ============================================
-- 3. Feature Store
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_feature_store (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    features TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(transaction_id)
);

-- ============================================
-- 4. Bảng log rule engine
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_rules_log (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    rule_id VARCHAR(50),
    feature VARCHAR(50),
    value FLOAT,
    note TEXT,
    triggered_at TIMESTAMP DEFAULT NOW(),
    CONSTRAINT fk_rules_log_txn
        FOREIGN KEY (transaction_id) REFERENCES fraud_raw(transaction_id)
        ON DELETE CASCADE
);

-- ============================================
-- 5. Bảng Alert
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_alerts (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(100),
    fraud_score FLOAT,
    alert_level VARCHAR(10),
    reason TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_alert_created ON fraud_alerts(created_at DESC);

-- ============================================
-- 6. Fraud Summary Statistics
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_statistics (
    stat_date DATE PRIMARY KEY,
    total_transactions INT,
    fraud_transactions INT,
    avg_fraud_score FLOAT,
    max_fraud_score FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- 7. Device Fraud Activity
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_device_log (
    id SERIAL PRIMARY KEY,
    device_id VARCHAR(100),
    transaction_id VARCHAR(100),
    fraud_score FLOAT,
    event_type VARCHAR(30),
    created_at TIMESTAMP DEFAULT NOW()
);

-- ============================================
-- 8. User Fraud Profile
-- ============================================
CREATE TABLE IF NOT EXISTS fraud_user_profile (
    user_id VARCHAR(100) PRIMARY KEY,
    total_transactions INT DEFAULT 0,
    fraud_attempts INT DEFAULT 0,
    last_fraud_score FLOAT DEFAULT 0,
    updated_at TIMESTAMP DEFAULT NOW()
);
SQL

echo "✅ [PostgreSQL] Fraud schema tables created."

# ============================================
# 3. ClickHouse: create DB & tables (one-by-one)
# ============================================
echo "📦 [ClickHouse] Creating database & tables..."

# ---- Create database ----
docker exec -i "${CH_CONTAINER}" clickhouse-client \
  -u "${CH_USER}" --password "${CH_PASS}" \
  --query "CREATE DATABASE IF NOT EXISTS ${CH_DB};"

echo "✅ ClickHouse database created: ${CH_DB}"

# ---- Table: fraud_predictions ----
docker exec -i "${CH_CONTAINER}" clickhouse-client \
  -u "${CH_USER}" --password "${CH_PASS}" --query "
CREATE TABLE IF NOT EXISTS ${CH_DB}.fraud_predictions
(
    transaction_id String,
    prediction UInt8,
    fraud_score Float64,
    model_version String,
    violated_rules String,
    violated_rule_count UInt16,
    top_features String,
    source_id String,
    event_time DateTime64(3),
    processed_at DateTime,
    ingest_ts DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(processed_at)
ORDER BY (processed_at, transaction_id)
SETTINGS index_granularity = 8192;
"

echo "✅ fraud_predictions created"

# ---- Table: fraud_rules_log ----
docker exec -i "${CH_CONTAINER}" clickhouse-client \
  -u "${CH_USER}" --password "${CH_PASS}" --query "
CREATE TABLE IF NOT EXISTS ${CH_DB}.fraud_rules_log
(
    transaction_id String,
    rule_id String,
    feature String,
    value Float64,
    note String,
    triggered_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(triggered_at)
ORDER BY (triggered_at, transaction_id)
SETTINGS index_granularity = 8192;
"

echo "✅ fraud_rules_log created"

# ---- Table: fraud_alerts ----
docker exec -i "${CH_CONTAINER}" clickhouse-client \
  -u "${CH_USER}" --password "${CH_PASS}" --query "
CREATE TABLE IF NOT EXISTS ${CH_DB}.fraud_alerts
(
    transaction_id String,
    fraud_score Float64,
    alert_level String,
    reason String,
    created_at DateTime
)
ENGINE = MergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (created_at, fraud_score)
SETTINGS index_granularity = 8192;
"

echo "✅ [ClickHouse] Database & tables created."

echo "============================================"
echo "🎉 DONE: PostgreSQL (fraud_db) & ClickHouse (financial_fraud) are ready."
echo "============================================"
