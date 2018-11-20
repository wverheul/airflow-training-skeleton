import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.trigger_rule import TriggerRule
import datetime

args = {
    "owner":"wverheul",
    "start_date":airflow.utils.dates.days_ago(14)
}

weekday_mail = {
    0:"Ha",
    1:"Hi",
    2:"Ho",
    3:"Hu",
    4:"He",
    5:"Hy",
    6:"Hoe"
}

def print_weekday(execution_date, **context):
    print(int(execution_date.strftime("%u"))-1)

def _get_weekday(execution_date, **context):
    return int(execution_date.strftime("%u"))-1

with DAG(
    dag_id="exercise3",
    default_args=args
) as dag:
    print_weekday = PythonOperator(
        task_id="print_weekday",
        python_callable=print_weekday,
        provide_context=True,
        dag=dag)
    branching = BranchPythonOperator(
        task_id="branching",
        python_callable=_get_weekday,
        provide_context=True,
        dag=dag)
    emails = [DummyOperator(
        task_id=f"{i}",
        dag=dag)
        for i in weekday_mail
    ]
    final_task = DummyOperator(task_id="final_task", trigger_rule=TriggerRule.ONE_SUCCESS, dag=dag)

print_weekday >> branching >> emails >> final_task