from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_queries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, 
                 redshift_conn_id="", 
                 aws_credentials_id="", 
                 table="",
                 sql_query="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.sql_query=sql_query

    def execute(self, context):
        self.log.info('LoadFactOperator is being implemented')
        self.log.info(f"Insert into {self.table} ")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(f"INSERT INTO {self.table}{self.sql_query}")
