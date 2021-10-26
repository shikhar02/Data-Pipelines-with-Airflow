from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import PostgresOperator
from helpers import SqlQueries
import datetime

dag_name2 = 'staging_tables_dag'

dag2 = DAG(dag_name2,
           description='Create staging events and staging songs tables in Redshift using Airflow',
           start_date=datetime.datetime(2019,1,12,0,0,0,0),
           schedule_interval='@once')

create_staging_events_table = PostgresOperator(
      task_id='create_events_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_events
    )

create_staging_songs_table = PostgresOperator(
      task_id='create_songs_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_songs
    )

create_dimension_songs_table = PostgresOperator(
      task_id='create_dim_songs_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_dimensions_songs_table
    )

create_dimension_artists_table = PostgresOperator(
      task_id='create_dim_artists_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_dimensions_artists_table
    )


create_dimension_users_table = PostgresOperator(
      task_id='create_dim_users_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_dimensions_users_table
    )

create_dimension_time_table = PostgresOperator(
      task_id='create_dim_time_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_dimensions_time_table
    )

create_facts_songplays_table = PostgresOperator(
      task_id='create_fact_songplays_table',
      dag=dag2,
      postgres_conn_id='redshift',
      sql=SqlQueries.create_facts_songplays_table
    )

create_staging_events_table>>create_facts_songplays_table
create_staging_songs_table>>create_facts_songplays_table

create_facts_songplays_table>>create_dimension_songs_table
create_facts_songplays_table>>create_dimension_artists_table
create_facts_songplays_table>>create_dimension_users_table
create_facts_songplays_table>>create_dimension_time_table
