from datetime import datetime
from airflow import DAG
import pendulum
from airflow.operators.bash import BashOperator
with DAG(
    dag_id='run_dbt',
    description='First DAG',
    schedule_interval= '@hourly',
    start_date=pendulum.datetime(2022, 10, 18,tz="UTC")
) as dag: 
    task = BashOperator(task_id = 'task_run',bash_command='/commands.sh',dag=dag)  
    task