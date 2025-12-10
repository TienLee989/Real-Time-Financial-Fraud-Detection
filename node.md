
docker exec -it airflow_postgres psql -U airflow -d airflow

CREATE DATABASE vehicle_db;
GRANT ALL PRIVILEGES ON DATABASE vehicle_db TO airflow;

CREATE TABLE IF NOT EXISTS vehicle_counts (
    id SERIAL PRIMARY KEY,
    camera_id VARCHAR(50) NOT NULL,              -- ID của camera (VD: CAM_1)
    video_name VARCHAR(255),                     -- Tên file video nguồn
    frame_id INT,                                -- Thứ tự frame trong video
    count INT DEFAULT 0,                         -- Số lượng phương tiện phát hiện
    vehicle_type TEXT,                           -- Danh sách loại phương tiện (VD: "car,bus,truck")
    frame_time TIMESTAMP,                        -- Thời điểm ghi hình frame
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- Thời điểm Spark xử lý frame
    CONSTRAINT chk_count_nonnegative CHECK (count >= 0)
);

CREATE INDEX idx_vehicle_counts_camera_time
ON vehicle_counts (camera_id, frame_time DESC);

CREATE INDEX idx_vehicle_counts_vehicle_type
ON vehicle_counts (vehicle_type);



SELECT camera_id, frame_time, count, vehicle_type
FROM vehicle_counts
ORDER BY processed_at DESC
LIMIT 10;

--
docker exec -it airflow_postgres psql -U airflow -d absa_db

CREATE DATABASE absa_db;
GRANT ALL PRIVILEGES ON DATABASE absa_db TO airflow;

CREATE TABLE absa_results (
    id SERIAL PRIMARY KEY,
    review TEXT,
    sentiment VARCHAR(20),
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
-----------------------------

docker exec -it airflow_postgres psql -U airflow -d airflow -f /opt/airflow/projects/vehicle-rt-counting/db/schema.sql

docker exec -it airflow_kafka kafka-topics --bootstrap-server kafka:9092 --list
docker exec -it airflow_kafka kafka-topics --create --topic vehicle-stream --bootstrap-server localhost:9092 --partitions 4 --replication-factor 1
docker exec -it airflow_kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic vehicle-stream --from-beginning



docker builder prune -af
docker rmi -f airflow-base:2.9.0-custom
docker build --no-cache -f base/Dockerfile -t airflow-base:2.9.0-custom .
