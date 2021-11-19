from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import sql_queries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self, redshift_conn_id="", aws_credentials_id="", 
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id

    def execute(self, context):
        self.log.info('LoadFactOperator is being implemented')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift.run(sql_queries.songplay_table_insert)
