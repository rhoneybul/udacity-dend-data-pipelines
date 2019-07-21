from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
try:
    from airflow.operators import (
        StageToRedshiftOperator,
        LoadFactOperator,
        LoadDimensionOperator,
        DataQualityOperator
    )
except Exception as e:
    print('could not import operators', e)
from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'rhoneybul',
    'start_date': datetime(2019,1,12),
    'depends_on_past': False, 
    'retries': 3,
    'retry_delta': timedelta(minutes=5),
    'catchup_by_default': False,
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG('sparkify_etl_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
        )

def begin_exection():
    print('Sparkify ETL pipeline is creating.')

start_operator = PythonOperator(task_id='Begin_execution',  
                               python_callable=begin_exection,
                               dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    s3_file_path="s3://udacity-dend/log_data/",
    target_table='staging_events',
    columns=[
        'artist', 
        'auth', 
        'firstname', 
        'gender',
        'iteminsession',
        'lastname',
        'length',
        'level',
        'location',
        'method',
        'page', 
        'registration',
        'sessionid',
        'song',
        'status',
        'ts', 
        'userAgent', 
        'userid'
    ],
    data_format='s3://udacity-dend/log_json_path.json',
    file_type="JSON",
    dag=dag
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    s3_file_path='s3://udacity-dend/song_data/',
    target_table='staging_songs',
    columns=[
        'num_songs',
        'artist_id',
        'artist_name',
        'artist_latitude', 
        'artist_longitude',
        'artist_location',
        'song_id',
        'title', 
        'duration', 
        'year'
    ],
    file_type='JSON',
    dag=dag
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    sql_statement=SqlQueries.songplay_table_insert,
    target_table='songplays',
    dag=dag   
)

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    sql_statement=SqlQueries.user_table_insert,
    target_table='users',
    dag=dag
)

load_songplays_table >> load_user_dimension_table

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    sql_statement=SqlQueries.song_table_insert,
    target_table='songs',
    dag=dag
)

load_songplays_table >> load_song_dimension_table

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    sql_statement=SqlQueries.artist_table_insert,
    target_table='artists',
    dag=dag
)

load_songplays_table >> load_artist_dimension_table

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    sql_statement=SqlQueries.time_table_insert,
    target_table='time',
    dag=dag
)

load_songplays_table >> load_time_dimension_table

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    tables=['songs', 'artists', 'users', 'time', 'songplays'],
    dag=dag
)

load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks

def stop_execution():
    print('Sparkify data pipeline has concluded.')

end_operator = PythonOperator(task_id='Stop_execution',
                              python_callable=stop_execution,
                              dag=dag)

run_quality_checks >> end_operator