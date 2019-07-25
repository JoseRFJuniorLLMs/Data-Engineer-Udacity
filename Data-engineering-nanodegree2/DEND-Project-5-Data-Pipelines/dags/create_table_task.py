from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import sql_statements

default_args = {
    'owner': 'fs',
    'start_date': datetime(2019, 6, 24),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'depends_on_past': False,
    'schedule_interval': '@hourly'
}

dag = DAG(
    'create_table_task',
    default_args=default_args,
    description='Drop and Create Tables That Will Be Loaded in RedShift'
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_table_artists = PostgresOperator(
    task_id='drop_artists',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_ARTISTS,
    dag=dag
)

create_table_artists = PostgresOperator(
    task_id='create_artists',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_ARTISTS, ## helper
    dag=dag
)

drop_table_songplays = PostgresOperator(
    task_id='drop_songplays',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGPLAYS,
    dag=dag
)

create_table_songplays = PostgresOperator(
    task_id='create_songplays',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGPLAYS,
    dag=dag
)

drop_table_songs = PostgresOperator(
    task_id='drop_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_SONGS,
    dag=dag
)

create_table_songs = PostgresOperator(
    task_id='create_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_SONGS,
    dag=dag
)

drop_table_staging_events = PostgresOperator(
    task_id='drop_staging_events',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGING_EVENTS,
    dag=dag
)

create_table_staging_events = PostgresOperator(
    task_id='create_staging_events',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_EVENTS,
    dag=dag
)

drop_table_staging_songs = PostgresOperator(
    task_id='drop_staging_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_STAGING_SONGS,
    dag=dag
)

create_table_staging_songs = PostgresOperator(
    task_id='create_staging_songs',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_STAGING_SONGS,
    dag=dag
)

drop_table_time = PostgresOperator(
    task_id='drop_time',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_TIME,
    dag=dag
)

create_table_time = PostgresOperator(
    task_id='create_time',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_TIME,
    dag=dag
)

drop_table_users = PostgresOperator(
    task_id='drop_users',
    postgres_conn_id="redshift",
    sql=sql_statements.DROP_TABLE_USERS,
    dag=dag
)

create_table_users = PostgresOperator(
    task_id='create_users',
    postgres_conn_id="redshift",
    sql=sql_statements.CREATE_TABLE_USERS,
    dag=dag
)

end_operator = DummyOperator(task_id='stop_execution',  dag=dag)

start_operator >> drop_table_artists
drop_table_artists >> create_table_artists
start_operator >> drop_table_songplays
drop_table_songplays >> create_table_songplays
start_operator >> drop_table_songs
drop_table_songs >> create_table_songs
start_operator >> drop_table_staging_events
drop_table_staging_events >> create_table_staging_events
start_operator >> drop_table_staging_songs
drop_table_staging_songs >> create_table_staging_songs
start_operator >> drop_table_time
drop_table_time >> create_table_time
start_operator >> drop_table_users
drop_table_users >> create_table_users
create_table_artists >> end_operator
create_table_songplays >> end_operator
create_table_songs >> end_operator
create_table_staging_events >> end_operator
create_table_staging_songs >> end_operator
create_table_time >> end_operator
create_table_users >> end_operator