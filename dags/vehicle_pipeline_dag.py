# ===========================================
# DAG: Vehicle Streaming Pipeline (4 Cameras)
# ===========================================
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import os, subprocess

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="vehicle_pipeline_dag",
    default_args=default_args,
    description="Kafka–Spark–PostgreSQL–Streamlit vehicle counting pipeline (multi-camera demo)",
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=55),
    tags=["vehicle", "streaming", "kafka", "spark"],
) as dag:

    # === 1️⃣ 4 Producers song song (cam1–cam4) ===
    producers = []
    for cam_id in range(1, 5):
        task = BashOperator(
            task_id=f"producer_cam{cam_id}",
            bash_command=f'bash -c "timeout 45m /opt/airflow/projects/vehicle_count/scripts/run_producer_cam{cam_id}.sh"',
            execution_timeout=timedelta(minutes=50),
            trigger_rule="all_done",
        )
        producers.append(task)

    # === 2️⃣ Spark Streaming Consumer ===
    start_vehicle_consumer = BashOperator(
        task_id="start_vehicle_consumer",
        bash_command='bash -c "timeout 45m /opt/airflow/projects/vehicle_count/scripts/run_consumer.sh"',
        execution_timeout=timedelta(minutes=50),
        trigger_rule="all_done",
    )

    # === 3️⃣ Giám sát checkpoint ===
    def monitor_vehicle_job():
        path = "/opt/airflow/checkpoints/vehicle_pipeline_checkpoint"
        print("[Monitor] Checking vehicle streaming checkpoint...")
        if os.path.exists(path):
            size = subprocess.check_output(["du", "-sh", path]).decode().split()[0]
            print(f"[Monitor] ✅ Checkpoint exists ({size}) → job running normally.")
        else:
            print("[Monitor] ⚠️ No checkpoint found. Possibly failed or cleaned.")

    monitor_vehicle_stream = PythonOperator(
        task_id="monitor_vehicle_stream",
        python_callable=monitor_vehicle_job,
        trigger_rule="all_done",
    )

    # === 4️⃣ Cleanup checkpoint ===
    cleanup_vehicle_checkpoints = BashOperator(
        task_id="cleanup_vehicle_checkpoints",
        bash_command=(
            "echo '[Cleanup] 🧹 Removing old vehicle checkpoint...'; "
            "rm -rf /opt/airflow/checkpoints/vehicle_pipeline_checkpoint || true; "
            "echo '[Cleanup] ✅ Done.'"
        ),
        trigger_rule="all_done",
    )

    # === Task Dependency ===
    # Tuần tự hóa các producer và consumer
    producers >> start_vehicle_consumer
    start_vehicle_consumer >> monitor_vehicle_stream >> cleanup_vehicle_checkpoints
