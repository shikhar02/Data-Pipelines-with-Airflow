from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from helpers import SqlQueries
class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9T'
   
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 table="",redshift_conn_id="",sql_query="",delete_flag="",*args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.sql_query=sql_query
        self.delete_flag=delete_flag
        
    def execute(self, context):
        self.log.info('LoadDimensionOperator not implemented yet')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.delete_flag==True:
            sql_statement = f'DELETE FROM {self.table}'
            redshift.run(sql_statement)
            sql_statement = f'INSERT INTO {self.table} {self.sql_query}'
            redshift.run(sql_statement)
            self.log.info('Process Completed!')
        
        elif self.delete_flag==False:
            self.log.info(f'Inserting data in {self.table} table')
            sql_statement=f'INSERT INTO {self.table}{self.sql_query}'
            redshift.run(sql_statement)
           
            
            self.log.info('Insertion Completed!')
        
        else:
            self.log.info('Please, specify delete_flag parameter as True or False.')
        
            
        
        
        
        
        
        
        
        
      
