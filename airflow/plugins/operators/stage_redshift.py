from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_field =("s3_key",)
    copy_sql ="""
        COPY {table}
        FROM '{s3_path}'
        ACCESS_EKY_ID '{aws_credentials_id}'
        SECRET_ACCESS_KEY '{aws_secret_key}'
        IGNOREHEADER {ignore_headers}
        DELIMITER '{delimiter}'        
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 delimiter=",",
                 ignore_headers=1,            
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials_id=aws_credentials_id
        self.table=table
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.delimiter=delimiter
        self.ignore_headers=ignore_headers

    def execute(self, context):
        self.log.info('StageToRedshiftOperator is being implemented')
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context) #dereferenced {{execution_date}} and {{ds}}
        s3_path = "{}/{}".format(self.s3_bucket, rendered_key)
        self.log.info(f"s3_path:{s3_path}")
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_bucket=s3_path,
            aws_credentials_id=credentials.access_key,
            aws_secret_key=credentials.secret_key,
            ignore_headers=self.ignore_headers,
            delimiter=self.delimiter,
        )





