from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag, task
from airflow.operators.dummy_operator import DummyOperator
from final_project_operators.stage_redshift import StageToRedshiftOperator
from final_project_operators.load_fact import LoadFactOperator
from final_project_operators.load_dimension import LoadDimensionOperator
from final_project_operators.data_quality import DataQualityOperator
from udacity.common import final_project_sql_statements
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

default_args = {
    'owner': 'sparkify',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry':False
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='@hourly'
)

def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    @task()
    def crear_esquema():
        redshift_exec = PostgresHook(postgres_conn_id="redshift")
        redshift_exec.run(final_project_sql_statements.esquema)

    creacion_tablas = crear_esquema()

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift = 'redshift',
        aws_credentials = 'aws_credentials',
        table = 'staging_events',
        bucket_s3 = Variable.get('s3_bucket'),
        s3_key = 'log-data',
        s3_format="JSON 's3://jasedane/log_json_path.json'"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift="redshift",
        aws_credentials="aws_credentials",
        table="staging_songs",
        bucket_s3 = Variable.get('s3_bucket'),
        s3_key = "song-data/A/B/C",
        s3_format="JSON 'auto'"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift = 'redshift',
        table="songplays",
        sql=final_project_sql_statements.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift = 'redshift',
        table="users",
        truncate=True,
        sql=final_project_sql_statements.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift = 'redshift',
        table="songs",
        truncate=True,
        sql=final_project_sql_statements.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift = 'redshift',
        table="artists",
        truncate=True,
        sql=final_project_sql_statements.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift = 'redshift',
        table="time",
        truncate=True,
        sql=final_project_sql_statements.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Validaciones_Calidad',
        redshift="redshift",
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )

    end_operator = DummyOperator(task_id='Execution_End')

    start_operator >> creacion_tablas >> [stage_events_to_redshift, stage_songs_to_redshift]

    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table

    load_songplays_table >> [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]

    [
        load_song_dimension_table,
        load_user_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> end_operator



final_project_dag = final_project()
