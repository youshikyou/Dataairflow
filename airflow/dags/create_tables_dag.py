from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'email_on_retry': False,
}

with DAG(
    'create_tables',
    start_date=datetime(2018, 1, 1, 0, 0, 0, 0),
    default_args=default_args,
    schedule_interval="@once",
    max_active_runs=1
     ) as dag:
    
    PostgresOperator(
        task_id='Create_tables',
        dag=dag,
        postgres_conn_id="redshift",
        schedule_interval="@daily",
        sql='create_tables.sql'
    )