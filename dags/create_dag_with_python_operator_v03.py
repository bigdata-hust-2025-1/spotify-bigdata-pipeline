from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(name, age):
    print(f"Hello World! My name is {name},"
          f"I am {age} years old.")

def get_name():
    return 'Xuan Binh'
with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v03',
    description='Our first dag using python operator',
    start_date = datetime(2024, 10, 6),
    schedule = '@daily',
    catchup=False
) as dag:
    # task1 = PythonOperator(
    #     task_id ='greet',
    #     python_callable = greet,
    #     op_kwargs = {'name' : 'Tom', 'age': 20}
    # )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable = get_name,
    )

task2