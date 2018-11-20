import airflow
from airflow import DAG
from airflow.contrib.operators.postgres_to_gcs_operator import PostgresToGoogleCloudStorageOperator

args = {
    "owner":"wverheul",
    "start_date":airflow.utils.dates.days_ago(3)
}

with DAG(
    dag_id="exercise4",
    default_args=args,
    schedule_interval="0 0 * * *"
) as dag:
    psql_to_gcs = PostgresToGoogleCloudStorageOperator(
        task_id="psql_to_gcs",
        sql="SELECT * FROM land_registry_price_paid_uk WHERE transfer_date = '{{ ds }}",
        bucket="europe-west1-training-airfl-f9775318-bucket",
        filename="/wverheul/{}-psql-to-gcs.json".format(execution_date.strftime("%Y%m%d")),
        postgres_conn_id="psql_training",
        provide_context=True,
        dag=dag
    )

psql_to_gcs