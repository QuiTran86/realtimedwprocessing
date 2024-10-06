import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import pandas as pd

start_date = datetime(year=2024, month=10, day=6)
default_args = {
    'owner': 'quitran',
    'depends_on_past': False,
    'backfill': False
}
rows_limit = 50
output_file = './account_dim_data.csv'

account_ids = []
account_types = []
customer_ids = []
opening_dates = []
statuses = []
balances = []


def generate_random_data(row_num):
    account_id = f'A{row_num:05d}'
    account_type = random.choice(['SAVINGS', 'CHECKING'])
    status = random.choice(['ACTIVE', 'INACTIVE'])
    customer_id = f'C{random.randint(1, 1000):05d}'
    balance = round(random.uniform(100.00, 10000.00), 2)
    random_date = datetime.now() - timedelta(days=random.randint(0, 365))
    opening_dates = random_date.timestamp() * 1000
    return account_id, account_type, customer_id, opening_dates, status, balance


def generate_account_dim_data():
    num_row = 1
    while num_row <= rows_limit:
        account_id, account_type, customer_id, opening_date, status, balance = generate_random_data(num_row)
        account_ids.append(account_id)
        account_types.append(account_type)
        statuses.append(status)
        customer_ids.append(customer_id)
        balances.append(balance)
        opening_dates.append(opening_date)
        num_row += 1

    data = pd.DataFrame(
        {
            'account_id': account_ids,
            'account_type': account_types,
            'status': statuses,
            'customer_id': customer_ids,
            'balance': balances,
            'opening_date': opening_dates
        }
    )
    data.to_csv(output_file, index=False)
    print(f'Generated file: {output_file} with {num_row} rows successfully!')


with DAG('account_dim_generator',
         default_args=default_args,
         description='Generate account dimension data in CSV file',
         schedule_interval=timedelta(days=1),
         start_date=start_date, tags=['schema']) as dag:
    start = EmptyOperator(task_id='start_task')
    generate_account_dimension_data = PythonOperator(
        task_id='generate_account_dim_data',
        python_callable=generate_account_dim_data
    )
    end = EmptyOperator(task_id='end_task')
    start >> generate_account_dimension_data >> end