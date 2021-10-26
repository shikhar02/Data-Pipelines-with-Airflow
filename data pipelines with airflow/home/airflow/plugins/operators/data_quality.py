from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,redshift_conn_id="",quality_check="",
                 # Define your operators params (with defaults) here
                 # Example:
                 # conn_id = your-connection-name
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.quality_check=quality_check
        
        # Map params here
        # Example:
        # self.conn_id = conn_id

    def execute(self, context):
        self.log.info('DataQualityOperator not implemented yet')
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
       # params = kwargs[self.param]['table']
        
        for check in self.quality_check:
            records=redshift.get_records(check['test_sql'])
            expected_result= check.get('expected_result')
          # self.log.info('Checking whether table {} contains records or not'.format(check['table']))
            if records[0][0]!=expected_result:
                self.log.info(f"Data quality check failed for {check['table']} table.")
                raise ValueError(f"{check['test_sql']}")
            
             
           # if len(records)<1 or len(records[0])<1:
                
                '''self.log.info('No records present in {}'.format(check['table']))
                raise ValueError('Table {} does not contain any records'.format(check['table']))
            
            #self.log.info('Check Completed. Table {} contains {} records'.format(check['table'],records[0][0]))
            
            null_record=redshift.get_records(
            f"SELECT SUM(CASE WHEN {table['primary_key']} IS NULL THEN 1 ELSE 0 END) AS total_null_records FROM {table['table']}"
            )
            self.log.info(f"Total Null Records:{null_record[0][0]}")
            
            if null_record[0][0] != table['expected_result']:
                self.log.info("Total {} null records are present in the {} table.".format(null_record,table['table']))
                raise ValueError("Data quality check failed.")
            
            self.log.info("No null records are present in the {} table.".format(table['table']))
                      '''
            
        