import glob
from typing import Any

import requests
from airflow.models import BaseOperator
from airflow.utils.context import Context
from airflow.utils.decorators import apply_defaults


class PinotSubmitOperator(BaseOperator):

    @apply_defaults
    def __init__(self, submit_type, folder_path, pinot_url, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.submit_type = submit_type
        if self.submit_type not in ('schema', 'table'):
            raise Exception(f'Only accepted type schema, type. Got: {self.submit_type}')
        self.folder_path = folder_path
        self.pinot_url = pinot_url

    def execute(self, context: Context) -> Any:
        files = glob.glob(f'{self.folder_path}/*.json')
        headers = {'Content-Type': 'Application/json'}
        for file in files:
            with open(file) as f:
                data = f.read()
                resp = requests.post(self.pinot_url, headers=headers, data=data)
                if resp.status_code == 200:
                    self.log.info(f'{self.submit_type} has been submitted to pinot controller successfully!')
                else:
                    self.log.error(f'Failed to submit {self.submit_type} to pinot controller.')
                    raise Exception(
                        f'Failed to submit {self.submit_type} to pinot controller due to: {resp.status_code} - {resp.text}')
