import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from utils import data_extraction, data_load_to_db

with DAG(
    dag_id='main_dag',
    start_date=datetime.datetime(2025, 9, 16),
    schedule="@daily",
):
    start = EmptyOperator(task_id='Start_task')
    extract = PythonOperator(
        task_id='data_extraction',
        python_callable=data_extraction
    )
    load = PythonOperator(
        task_id='data_load',
        python_callable=data_load_to_db
    )
    dbt = BashOperator(task_id='dbt', bash_command='dbt run --profiles-dir /opt/airflow/.dbt --project-dir /opt/airflow/trades_pipeline')
    end = EmptyOperator(task_id='End_task')


    start >> extract >> load >> dbt >> end