from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from helpers import SqlQueries



default_args = {
    'owner': 'xxxx',
    'depends_on_past': False,
    'email_on_retry': False,
}

dag = DAG('for_test',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2018, 11, 1, 0, 0, 0, 0),
          end_date=datetime(2018, 11, 5, 0, 0, 0, 0),
          schedule_interval="@hourly",
          catchup=True,
        )

start_operator = DummyOperator(task_id='Begin_execution',dag=dag)