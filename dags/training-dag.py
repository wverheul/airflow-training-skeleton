import airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id="airflow-training",
    default_args={
        "owner": "wverheul",
        "start_date": airflow.utils.dates.days_ago(3),
    },
) as dag:
    t1 = DummyOperator(task_id='t1', dag=dag)
    t2 = DummyOperator(task_id='t2', dag=dag)