from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
	'owner':'airflow',
	'depends_on_past':False,
	'start_date': datetime(2022, 2, 5),
	'retries':0
    }

dag_scripts = DAG(
    dag_id='DAG-2', 
    default_args=default_args, 
    catchup=False,  
    schedule_interval='@once'
    )

create_table = PostgresOperator(
    task_id = 'create_table',
    postgres_conn_id = 'postgres_db',
    sql = 'CREATE table stud01 (stud_id int, stud_name varchar(100))',
    dag = dag_scripts
    )

load_data = PostgresOperator(
    task_id = 'load_data',
    postgres_conn_id = 'postgres_db',
    sql = 'INSERT INTO stud01 (`stud_id`,`stud_name`) VALUES (1,"abc"),(2,"xyz")',
    dag = dag_scripts
    )
create_table >> load_data

if __name__ == '__main__':
    dag.cli()

