"""
Financial Fraud Streaming DAG

Luồng:
1. Chuẩn bị file demo /tmp/financial_fraud/data1.csv
2. Gửi lên Kafka bằng run_producer.sh
3. Khởi động Spark consumer bằng run_consumer.sh
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "fraud_team",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="financial_fraud_streaming_dag",
    description="Kafka → Spark → TF Fraud Model → PostgreSQL + ClickHouse",
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=["fraud", "streaming", "kafka", "spark"],
) as dag:

    prepare_data = BashOperator(
        task_id="prepare_demo_data",
        bash_command="""
            mkdir -p /tmp/financial_fraud && \
            cp -f /opt/airflow/projects/financial_fraud/data/data1.csv \
                  /tmp/financial_fraud/data1.csv
        """,
        do_xcom_push=False,
    )

    start_producer = BashOperator(
        task_id="start_fraud_producer",
        bash_command="""
            bash /opt/airflow/projects/financial_fraud/scripts/run_producer.sh \
            "{{ dag_run.conf['source_id'] if dag_run.conf and 'source_id' in dag_run.conf else 'DATA_1' }}" \
            "{{ dag_run.conf['data_path'] if dag_run.conf and 'data_path' in dag_run.conf else '/tmp/financial_fraud/data1.csv' }}"
        """,
        do_xcom_push=False,
    )

    start_consumer = BashOperator(
        task_id="start_fraud_consumer",
        bash_command="""
            bash /opt/airflow/projects/financial_fraud/scripts/run_consumer.sh
        """,
        do_xcom_push=False,
    )

    prepare_data >> start_producer >> start_consumer
