import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta

local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='dag_with_cron_expression_v04',
    default_args=default_args,
    start_date=pendulum.datetime(2025, 10, 20, tz=local_tz),
    schedule='5 4 * * Tue',    # 00:00 theo giờ VN
    catchup=True,
) as dag:
    task1 = BashOperator(
        task_id='task1',
        bash_command='echo "DAG chạy lúc nửa đêm theo giờ VN!"'
    )
