import os
import yaml
import sys

from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir)))

class AirflowOperatorFactory():
    
    @staticmethod
    def get_empty_operator(dag, task_id):
        task = EmptyOperator(
            dag = dag,
            task_id = task_id
        )
        return task

    @staticmethod
    def get_python_operator(dag, task_id, args):
        task = PythonOperator(
            dag = dag,
            task_id = task_id,
            **args
        )
        return task
    
    @staticmethod
    def get_bq_insert_job_operator(dag, task_id, args):
        task = BigQueryInsertJobOperator(
            dag = dag,
            task_id = task_id,
            **args
        )
        return task