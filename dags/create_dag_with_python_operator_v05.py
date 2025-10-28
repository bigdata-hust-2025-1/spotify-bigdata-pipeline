from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def greet(age, ti): #ti: task instance
    firstname = ti.xcom_pull(task_ids='get_name', key='firstname')
    lastname = ti.xcom_pull(task_ids='get_name',key='lastname')
    print(f"Hello World! My name is {firstname} { lastname},"
          f"I am {age} years old.")

def get_name(ti):
    ti.xcom_push(key='firstname', value='Xuan Binh')
    ti.xcom_push(key='lastname', value='Nguyen')

with DAG(
    default_args=default_args,
    dag_id='our_dag_with_python_operator_v05',
    description='Our first dag using python operator',
    start_date = datetime(2024, 10, 6),
    schedule = '@daily',
    catchup=False
) as dag:
    task1 = PythonOperator(
        task_id ='greet',
        python_callable = greet,
        op_kwargs = {'age': 20}
    )

    task2 = PythonOperator(
        task_id='get_name',
        python_callable = get_name,
    )

task2 >> task1