import datetime
from datetime import timedelta
import pendulum
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from dags_help import sql_statements



default_args = {
    "owner": 'thinhpd10bucket',
    "depends_on_past": False,
    "start_date": datetime.now(),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
    "email_on_retry": False,
}

# set DAG to hourly according to project rubric
dag = DAG('final_project',
          default_args=default_args,
          schedule_interval='@hourly'
        )
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)


create_tables = PostgresOperator(
        task_id='create_tables',
        dag=dag,
        postgres_conn_id="redshift",
        sql='SqlQueriestable.sql',
)

stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_events",
        s3_path='s3://udacity-dend/log_data',
        region='us-west-2',
        json_option="s3://udacity-dend/log_json_path.json",
        provide_context=True,
    )

stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        dag=dag,
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        table="staging_songs",
        s3_bucket="udacity-dend",
        s3_key="song_data",
        s3_path='s3://udacity-dend/song_data',
        region='us-west-2',
        provide_context=True,
        json_option='auto'
    )

load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        dag=dag,
        redshift_conn_id="redshift",
        table='songplays',
        sql=sql_statements.songplay_table_insert,
        truncate=False
    )

load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table='users',
        sql=sql_statements.user_table_insert,
        truncate=False
    )

load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table='songs',
        sql=sql_statements.song_table_insert,
        truncate=False
    )

load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        dag=dag,
        redshift_conn_id="redshift",
        table='artists',
        sql=sql_statements.artist_table_insert,
        truncate=False
    )

load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        dag=dag,
        append=True,
        redshift_conn_id="redshift",
        table='times',
        sql=sql_statements.time_table_insert,
        truncate=False
    )

run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
        table = "songplays"
    )
end_operator = DummyOperator(task_id='End_execution',  dag=dag)

# set task dependencies according to required flow
start_operator >> [
    stage_events_to_redshift,
    stage_songs_to_redshift,
] >> load_songplays_table
load_songplays_table >> [
    load_user_dimension_table,
    load_song_dimension_table,
    load_artist_dimension_table,
    load_time_dimension_table,
] >> run_quality_checks >> end_operator