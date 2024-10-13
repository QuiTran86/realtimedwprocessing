from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from plugins.kafka_producer import KafkaProducerOperator

start_date = datetime(year=2024, month=10, day=11)
default_args = {
    'owner': 'quitran',
    'depends_on_past': False,
    'backfill': False
}

with DAG(
    dag_id='transaction_facts_generator',
    default_args=default_args,
    description='Generate transaction facts into Kafka',
    schedule_interval=timedelta(days=1),
    start_date=start_date, tags=['realtime']
) as dags:
    start = EmptyOperator(task_id='start_task')
    generate_transaction_facts_data = KafkaProducerOperator(
        task_id='generate_transaction_facts_data',
        broker='kafka_broker:9092', topic='transaction_facts', num_records=10
    )
    end = EmptyOperator(task_id='end_task')

    start >> generate_transaction_facts_data >> end