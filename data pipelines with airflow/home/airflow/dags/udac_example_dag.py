from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator, LoadDimensionOperator, DataQualityOperator, PostgresOperator)
                              
from helpers import SqlQueries
import datetime

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

"""default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12)
}
"""
default_args = {
    'owner': 'shikhar',
    'start_date': datetime.datetime(2019,1,12,0,0,0,0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'email_on_failure': False,
    'catchup': False
}
dag_name = 'udacity_dend_dag' 

      
dag = DAG(dag_name,
          default_args=default_args,
          description='Extract Load and Transform data from S3 to Redshift',
          end_date=datetime.datetime(2019,12,1,0,0,0,0),
          schedule_interval='@hourly',
          max_active_runs=1
         )

'''dag = DAG('udac_example_dag',
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime.datetime(2019,1,12,0,0,0,0),
          end_date=datetime.datetime(2019,1,13,0,0,0,0),
          schedule_interval="@hourly",
          max_active_runs=1
        )
'''
start_operator = DummyOperator(task_id='Begin_execution',dag=dag)



stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/2018/11/",
    format_json='s3://udacity-dend/log_json_path.json',
    provide_context=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data/A/Z/",
    format_json='auto',
    provide_context=True
)


load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table='songplays',
    redshift_conn_id='redshift',
   # aws_credentials="aws_credentials",
    sql_query=SqlQueries.songplay_table_insert,
    delete_flag=False
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    table='users',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.user_table_insert,
    delete_flag=False
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    table='songs',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.song_table_insert,
    delete_flag=False
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    table='artists',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.artist_table_insert,
    delete_flag=False
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    table='time',
    redshift_conn_id='redshift',
    sql_query=SqlQueries.time_table_insert,
    delete_flag=False
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    #dq_checks=['songplays','users','songs','artists','time'],
    quality_check=[{'table':'songs','test_sql': "SELECT COUNT(*) FROM songs WHERE songid IS NULL", 'expected_result': 0},
                   {'table':'users','test_sql': "SELECT COUNT(*) FROM users WHERE userid IS NULL", 'expected_result': 0},
                   {'table':'artists','test_sql': "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", 'expected_result': 0},
                   {'table':'songplays','test_sql': "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", 'expected_result': 0},
                   {'table':'time','test_sql': "SELECT COUNT(*) FROM time WHERE start_time IS NULL", 'expected_result': 0}
                  ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



start_operator>>stage_events_to_redshift>>load_songplays_table
start_operator>>stage_songs_to_redshift>>load_songplays_table
load_songplays_table>>[load_user_dimension_table,load_song_dimension_table]>>run_quality_checks
load_songplays_table>>[load_artist_dimension_table,load_time_dimension_table]>>run_quality_checks
run_quality_checks>>end_operator

'''load_songplays_table>>load_artist_dimension_table>>run_quality_checks
load_songplays_table>>load_time_dimension_table >>run_quality_checks

run_quality_checks>>end_operator'''
