from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
#from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    input_sql = """
    INSERT INTO {}
    {}
    """

    @apply_defaults
    def __init__(self,table="",redshift_conn_id="", sql_query="",delete_flag="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        #self.aws_credentials=aws_credentials
        self.sql_query=sql_query
        self.delete_flag=delete_flag

    def execute(self, context):
        self.log.info('LoadFactOperator not implemented yet')
        #aws_hook = AwsHook(self.aws_credentials)
        #credentials = aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
       # self.log.info('Loading data in songplays')
        
        if self.delete_flag==True:
            self.log.info(f'Delete-insert data in {self.table} table')
            redshift.run(f"DELETE FROM {self.table}; INSERT INTO {self.table} {self.sql_query}")
            
            self.log.info('Process Complete!')
        
        elif self.delete_flag==False:
            formatted_sql = LoadFactOperator.input_sql.format(
                self.table,
                self.sql_query)
        
            self.log.info(f'Loading data in {self.table} table')
            redshift.run(formatted_sql)
        
            self.log.info('Done')
        
        else:
            self.log.info('Please, specify delete flag parameter as True or False.')
        
      
            
