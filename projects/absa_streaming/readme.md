Kafka Producer  →  Kafka Topic  →  Spark Structured Streaming Consumer
       │                                │
       ▼                                ▼
   raw reviews                   ABSA inference (load model_current)
                                        │
                                        ▼
                              PostgreSQL (absa_results)
                                        │
                         ┌──────────────┴───────────────┐
                         ▼                              ▼
                 Streamlit Dashboard             Airflow Retrain DAG
                                                      │
                                         train → evaluate → promote
                                                      │
                                                      ▼
                                       Update registry + model_current.pt

----------------------------------------------------------

projects/
└── absa_streaming/
    ├── data/
    │   ├── test_data.csv
    │   └── sql/
    │       └── schema.sql
    ├── models/
    │   ├── current/
    │   │   └── best_absa_hardshare.pt
    │   └── candidates/
    ├── scripts/
    │   ├── absa_producer.py
    │   ├── absa_consumer.py
    │   ├── prepare_train_data.py
    │   ├── retrain_model.py
    │   ├── evaluate_model.py
    │   ├── promote_model.py
    │   ├── run_producer.sh
    │   ├── run_consumer.sh
    │   └── run_retrain.sh
    └── streamlit/
        └── streamlit_app.py
dags/
├── absa_streaming_dag.py
└── absa_retrain_dag.py

----------------------------------------------------------
-- Bảng kết quả dự đoán online
CREATE TABLE IF NOT EXISTS absa_results (
  id            BIGSERIAL PRIMARY KEY,
  review        TEXT NOT NULL,
  sentiment     VARCHAR(20) NOT NULL,
  confidence    FLOAT,
  model_id      VARCHAR(64) NOT NULL,
  processed_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Đăng ký các model (đường dẫn, active,…)
CREATE TABLE IF NOT EXISTS absa_model_registry (
  model_id     VARCHAR(64) PRIMARY KEY,
  path         TEXT NOT NULL,
  created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  is_active    BOOLEAN DEFAULT FALSE
);

-- Lưu metric để so sánh trước khi promote
CREATE TABLE IF NOT EXISTS absa_model_metrics (
  model_id   VARCHAR(64) PRIMARY KEY,
  accuracy   FLOAT,
  f1_macro   FLOAT,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- View tiện kiểm tra model đang dùng
CREATE OR REPLACE VIEW absa_active_model AS
SELECT model_id, path, created_at
FROM absa_model_registry
WHERE is_active = TRUE
LIMIT 1;
