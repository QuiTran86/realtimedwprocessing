from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.pinot_operator import PinotSubmitOperator

start_date = datetime(year=2024, month=10, day=12)
default_args = {
    'owner': 'quitran',
    'depends_on_past': False,
    'backfill': False
}

with DAG(
        dag_id='submit_schema_dag',
        default_args=default_args,
        description='Submit schema to pinot controller',
        schedule_interval=timedelta(days=1),
        start_date=start_date, tags=['schema']
) as dag:
    start = EmptyOperator(task_id='start_task')
    submit_schema = PinotSubmitOperator(
        task_id='submit_schema_task',
        submit_type='schema',
        folder_path='/opt/airflow/dags/schemas',
        pinot_url='http://pinot-controller:9000/schemas'
    )
    end = EmptyOperator(task_id='end_task')

    start >> submit_schema >> end
