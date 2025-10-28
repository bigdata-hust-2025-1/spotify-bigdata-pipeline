from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    'owner' : 'coder2j',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
        dag_id = 'dag_with_taskflow_api_v02',
        default_args = default_args,
        start_date = datetime(2025,9,5),
        schedule = '@daily'
)
def hello_world_etl():
    

    @task(multiple_outputs=True)
    def get_name():
        return {
            'firstname':'Binh',
            'lastname': 'Nguyen'
        }
    
    @task
    def get_age():
        return 20
    
    @task
    def greet(firstname,lastname, age):
        print(f"Hello World! My name is {firstname} { lastname}"
              f" and I am {age} years old.")
    
    name_dict = get_name()
    age = get_age()
    greet(firstname=name_dict['firstname'], lastname=name_dict['lastname'], age=age)

greet_dag = hello_world_etl()
