# 🛡️ **Financial Fraud Detection – Real-Time Streaming System**

Hệ thống phát hiện gian lận tài chính theo thời gian thực sử dụng:

* **Kafka** → ingestion
* **Spark Structured Streaming** → real-time processing
* **TensorFlow model** → fraud scoring
* **Rule Engine** → bổ sung cảnh báo theo luật
* **PostgreSQL** → OLTP storage
* **ClickHouse** → OLAP storage & analytics
* **Airflow** → orchestration
* **Producers (test + realtime)** → sinh dữ liệu

Hệ thống vận hành ổn định và đã qua chạy thử end-to-end.

---

## 📌 **1. Kiến trúc tổng thể**

```
Producer → Kafka → Spark Streaming → (Model + Rule Engine)
      ↘︎ PostgreSQL (OLTP)
      ↘︎ ClickHouse (OLAP)
```

### Thành phần:

* **Kafka Cluster**: nhận event giao dịch tài chính.
* **Spark Streaming Consumer**: đọc Kafka, parse JSON, chạy model, lưu kết quả.
* **TensorFlow Model**: model `.keras` được train trước với 31 feature.
* **Rule Engine**: nhận rules từ `rules.json`, evaluate an toàn bằng `ast`.
* **PostgreSQL**: ghi RAW, prediction, rules log, feature store, alert.
* **ClickHouse**: ghi prediction để phân tích nhanh.
* **Airflow DAG**: quản lý chạy consumer & realtime producer.
* **Producers**:

  * `financial_fraud_producer.py` → bắn dữ liệu theo lô
  * `financial_fraud_producer_realtime.py` → bắn 3s/lần
  * `financial_fraud_producer_test.py` → bắn 1 record test

---

## 📌 **2. Features sử dụng khi training (31 features)**

Model được train với danh sách đầy đủ:

```
income
name_email_similarity
prev_address_months_count
current_address_months_count
customer_age
days_since_request
intended_balcon_amount
payment_type
zip_count_4w
velocity_6h
velocity_24h
velocity_4w
bank_branch_count_8w
date_of_birth_distinct_emails_4w
employment_status
credit_risk_score
email_is_free
housing_status
phone_home_valid
phone_mobile_valid
bank_months_count
has_other_cards
proposed_credit_limit
foreign_request
source
session_length_in_minutes
device_os
keep_alive_session
device_distinct_emails_8w
device_fraud_count
month
```

Hệ thống tự động:

* Bổ sung cột còn thiếu
* Encode categorical bằng factorize
* Fill numeric bằng mean
  → đảm bảo **tương thích với scaler + model**.

---

## 📌 **3. Producer**

### **financial_fraud_producer_test.py**

* Gửi 1 record hard-coded để test pipeline.

### **financial_fraud_producer.py**

* Gửi batch dữ liệu (test/integration).

### **financial_fraud_producer_realtime.py**

* Gửi **1 record mỗi 3 giây**.

Tất cả Producer đều dùng:

```
bootstrap_servers: kafka1:29092, kafka2:29093, kafka3:29094
topic: financial_fraud
```

---

## 📌 **4. Consumer – Spark Structured Streaming**

File chính:
👉 **`financial_fraud_consumer.py`**

Pipeline xử lý gồm:

### 1️⃣ Parse Kafka JSON theo schema Spark

### 2️⃣ Chuyển thành Pandas → xử lý đặc trưng

### 3️⃣ Chuẩn hoá bằng scaler

### 4️⃣ Dự đoán bằng TensorFlow model

### 5️⃣ Chạy Rule Engine

### 6️⃣ Ghi dữ liệu vào PostgreSQL & ClickHouse

---

## 📌 **5. Lưu trữ dữ liệu**

### **PostgreSQL tables**

1. `fraud_raw` → lưu payload gốc
2. `fraud_predictions` → lưu kết quả model
3. `fraud_rules_log` → log rule được kích hoạt
4. `fraud_feature_store` → lưu feature cho ML Ops
5. `fraud_alerts` → cảnh báo có fraud_score cao

### **ClickHouse tables**

1. `fraud_predictions` → analytics tốc độ cao

Schema được cung cấp trong file `.sh` tạo database/tables.

---

## 📌 **6. Rule Engine**

File rules:
`data/rules.json`

Ví dụ:

```json
{
  "R1": { "feature": "velocity_24h", "condition": "value > 5", "note": "High 24h velocity" }
}
```

Hệ thống dùng:

* `ast.parse` + whitelist operators
* Không cho phép thực thi code nguy hiểm
* Mỗi rule chỉ chạy trên 1 feature → nhanh và an toàn

---

## 📌 **7. Airflow Integration**

DAG chính:

```
financial_fraud_streaming_dag
```

Bao gồm tasks:

| Task                 | Chức năng             |
| -------------------- | --------------------- |
| start_fraud_consumer | start Spark Streaming |
| realtime_producer    | sinh event 3s/lần     |
| test_producer        | bắn 1 record test     |
| ...                  |                       |

Consumer chạy liên tục → cấu hình:

```
restart_up_for_retry = True
```

---

## 📌 **8. Fix lỗi UnboundLocalError: rec**

Do code dùng `rec` trước khi gán.
Phiên bản final đã:

* Không dùng `rec` ngoài loop
* Không dùng biến chưa khởi tạo

---

## 📌 **9. Kiểm thử end-to-end**

Run:

```
airflow tasks test financial_fraud_streaming_dag start_fraud_consumer
```

Sau đó gửi test:

```
python financial_fraud_producer_test.py
```

Kết quả mong đợi:

* PostgreSQL xuất hiện bản ghi trong:

  * fraud_raw
  * fraud_predictions
* ClickHouse có prediction
* Spark log: `"Batch X Completed"`

---

## 📌 **10. Thư mục dự án**

```
financial_fraud/
│
├── scripts/
│   ├── financial_fraud_consumer.py
│   ├── financial_fraud_producer.py
│   ├── financial_fraud_producer_test.py
│   ├── financial_fraud_producer_realtime.py
│
├── models/
│   ├── tf_fraud_model.keras
│   ├── scaler.pkl
│
├── data/
│   ├── rules.json
│
├── sql/
│   ├── create_postgres_tables.sql
│   ├── create_clickhouse_tables.sql
│
├── airflow/
│   ├── dags/
│       ├── financial_fraud_streaming_dag.py
│
└── README.md
```


                 ┌──────────────────────────────┐
                 │          Producers           │
                 │  (Batch / Realtime / Test)   │
                 └──────────────┬───────────────┘
                                │ Kafka Events
                                ▼
                    ┌──────────────────────┐
                    │     Kafka Cluster    │◄─────► Prometheus Exporter
                    └───────────▲──────────┘
                                │
                       Real-time JSON
                                │
                                ▼
               ┌─────────────────────────────────┐
               │   Spark Structured Streaming     │◄───► Spark UI / Metrics / Logs
               │  (Model + Rule Engine + ETL)     │
               └──────┬───────────────┬──────────┘
                      │               │
                      ▼               ▼
          ┌─────────────────┐   ┌──────────────────────┐
          │   PostgreSQL    │   │     ClickHouse       │
          │ (OLTP Storage)  │   │    (OLAP Storage)    │◄───► CH System Tables Monitoring
          └─────────────────┘   └──────────────────────┘
                      ▲               ▲
                      │               │
                      └────────┬──────┘
                               ▼
                 ┌─────────────────────────────┐
                 │        Monitoring Stack      │
                 │ Prometheus • Grafana • Loki │
                 └─────────────────────────────┘
