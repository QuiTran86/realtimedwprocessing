from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import urllib.parse

start_date = datetime(year=2024, month=10, day=12)
default_args = {
    'onwer': 'quitran',
    'depends_on_past': False,
    'backfill': False
}

ACCOUNT_INGEST_URI = '''http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&
batchConfigMapStr={
    "inputFormat":"csv",
    "recordReader.prop.delimiter":"|"
}'''

BRANCH_INGEST_URI = '''"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&
batchConfigMapStr={
    "inputFormat":"csv",
    "recordReader.prop.delimiter":"|"
}"'''

CUSTOMER_INGEST_URI = '''"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&
batchConfigMapStr={
    "inputFormat":"csv",
    "recordReader.prop.delimiter":"|"
}"'''

print(urllib.parse.quote(ACCOUNT_INGEST_URI))

with DAG(
        dag_id='dimension_batch_ingestor',
        default_args=default_args,
        description='Ingest batch dimension to Pinot',
        schedule_interval=timedelta(days=1),
        start_date=start_date
) as dag:
    ingest_account_dim = BashOperator(
        task_id='ingest_account_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/data/account_dim_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=account_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )

    ingest_branch_dim = BashOperator(
        task_id='ingest_branch_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/data/branch_dim_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=branch_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )

    ingest_customer_dim = BashOperator(
        task_id='ingest_customer_dim',
        bash_command=(
            'curl -X POST -F file=@/opt/airflow/data/customer_dim_data.csv '
            '-H "Content-Type: multipart/form-data" '
            '"http://pinot-controller:9000/ingestFromFile?tableNameWithType=customer_dim_OFFLINE&batchConfigMapStr=%7B%22inputFormat%22%3A%22csv%22%2C%22recordReader.prop.delimiter%22%3A%22%2C%22%7D"'
        )
    )
