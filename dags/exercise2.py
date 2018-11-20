import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
import datetime

def print_execution_date(execution_date, **context):
    print(execution_date)

with DAG(
    dag_id="exercise2",
    default_args={
        "owner": "wverheul",
        "start_date": airflow.utils.dates.days_ago(3),
    },
) as dag:
    t1 = PythonOperator(task_id='print_execution_date', python_callable=print_execution_date, provide_context=True, dag=dag)
    t2 = BashOperator(task_id='wait_5', bash_command='sleep 5')
    t3 = BashOperator(task_id='wait_1', bash_command='sleep 1')
    t4 = BashOperator(task_id='wait_10', bash_command='sleep 10')
    t5 = DummyOperator(task_id='the_end')

t1 >> [t2,t3,t4] >> t5