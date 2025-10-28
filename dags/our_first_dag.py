from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner':'coder2j',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id = 'our_first_dag_v5',
    default_args=default_args,
    description = 'This is our first dag that we write',
    start_date = datetime(2024, 7, 29, 2),
    schedule = '@daily',
    tags =["demo"]
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command ='echo hello world, this is the first task!'
    )

    task2 = BashOperator(
    task_id='second_task',
    bash_command ='echo hey, I am task 2 and will be running after task 1'
    )

    task3 = BashOperator(
    task_id='third_task',
    bash_command ='echo hey, I am task 3 and will be running after task 1'
    )


# set up task dependency m

# method1
# task1.set_downstream(task2)
# task1.set_downstream(task3)

#method2
# task1 >> task2
# task1 >> task3

#method 3
task1 >> [task2, task3]