from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "absa",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="absa_streaming_dag",
    default_args=default_args,
    schedule_interval=None,  # trigger khi muốn chạy
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=["absa", "streaming"],
) as dag:

    # ✅ Task 1: Chuẩn bị dữ liệu test
    prepare = BashOperator(
        task_id="prepare_test_data",
        bash_command="""/bin/bash -c 'mkdir -p /tmp/absa && cp /opt/airflow/projects/absa_streaming/data/test_data.csv /tmp/absa/test_data.csv'""",
        do_xcom_push=False,  # tắt push output (tránh template)
    )

    # ✅ Task 2: Kafka Producer
    producer = BashOperator(
        task_id="start_absa_producer",
        bash_command="""/bin/bash -c 'bash /opt/airflow/projects/absa_streaming/scripts/run_producer.sh'""",
        do_xcom_push=False,
    )

    # ✅ Task 3: Spark Consumer
    consumer = BashOperator(
        task_id="start_absa_consumer",
        bash_command="""/bin/bash -c 'bash /opt/airflow/projects/absa_streaming/scripts/run_consumer.sh'""",
        do_xcom_push=False,
    )

    # 🔗 Dependency
    prepare >> producer >> consumer
