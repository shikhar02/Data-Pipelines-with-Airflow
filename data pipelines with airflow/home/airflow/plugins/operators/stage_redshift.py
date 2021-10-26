from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",)
    
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        FORMAT json '{}'
        region 'us-west-2'
    """
# Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 # ,s3_bucket,s3_key, delimiter=",",ignore_headers=1,

    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 format_json="",
                 #delimiter=",",
                 #ignore_headers=1,
                 #files="json",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.format_json=format_json
       # self.delimiter=delimiter
        #self.ignore_headers=ignore_headers
        #self.files=files

    def execute(self, context):
        self.log.info('StageToRedshiftOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DELETE FROM {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.format_json
        )
            #self.files,
        redshift.run(formatted_sql)
        self.log.info('Staging Process Completed')

        
 





