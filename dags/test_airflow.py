from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_airflow_ok",
    start_date=datetime(2024, 1, 1),
    schedule=None,          # ✅ Airflow 2.9+
    catchup=False,
    tags=["test", "debug"],
) as dag:

    hello = BashOperator(
        task_id="say_hello",
        bash_command="echo 'Airflow DAG is working correctly'",
    )

    hello
