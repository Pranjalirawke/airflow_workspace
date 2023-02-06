from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
        'owner': 'dezyre',    
        #'start_date': airflow.utils.dates.days_ago(2),
        # 'end_date': datetime(),
        # 'depends_on_past': False,
        # 'email': ['test@example.com'],
        #'email_on_failure': False,
        #'email_on_retry': False,
        # If a task fails, retry it once after waiting
        # at least 5 minutes
        #'retries': 1,
        'retry_delay': timedelta(minutes=5),
        }
postgres_extract_data = DAG(
    dag_id='postgres_extract_data',
    default_args=default_args,
    # schedule_interval='0 0 * * *',
    schedule_interval='@once',
    start_date=days_ago(1),
    dagrun_timeout=timedelta(minutes=60),
    description='postgres_extract_data ',
)

extract_data_load_csv = BashOperator(
        task_id="extract_data_load_csv",
        bash_command="python3 /home/hduser/airflow/dags/postsql.py ",
        dag=postgres_extract_data
    )
	
task_1.set_upstream(task_2)

    if __name__ == "__main__":
        dag.cli()