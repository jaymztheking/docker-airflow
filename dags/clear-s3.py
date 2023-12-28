from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
from datetime import datetime, timedelta
import boto3

def empty_s3_bucket(**kwargs):
    bucket_name = 'jamesmedaugh'
    s3_hook = S3Hook(aws_conn_id='my_aws')
    s3_client = s3_hook.get_conn()
    files = s3_hook.list_keys(bucket_name)

    s3_hook.delete_objects(bucket=bucket_name, keys=files)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('S3_Bucket_Emptier',
          default_args=default_args,
          description='Clear Out S3 Bucket',
          schedule_interval=timedelta(days=1))

empty_bucket_task = PythonOperator(
    task_id = 'empty-s3-bucket',
    python_callable = empty_s3_bucket,
    dag=dag,
)
