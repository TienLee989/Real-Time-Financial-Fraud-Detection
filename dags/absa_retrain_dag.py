from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "absa",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="absa_retrain_dag",
    default_args=default_args,
    schedule_interval="0 3 * * *",  # hằng ngày 03:00
    start_date=datetime(2025, 11, 2),
    catchup=False,
    tags=["absa", "retrain"],
) as dag:

    prepare_train = BashOperator(
        task_id="prepare_train",
        bash_command="python /opt/airflow/projects/absa_streaming/scripts/prepare_train_data.py",
    )

    train = BashOperator(
        task_id="train_model",
        bash_command="python /opt/airflow/projects/absa_streaming/scripts/retrain_model.py",
    )

    evaluate = BashOperator(
        task_id="evaluate_model",
        bash_command="python /opt/airflow/projects/absa_streaming/scripts/evaluate_model.py",
    )

    promote = BashOperator(
        task_id="promote_model",
        bash_command="python /opt/airflow/projects/absa_streaming/scripts/promote_model.py",
    )

    prepare_train >> train >> evaluate >> promote
