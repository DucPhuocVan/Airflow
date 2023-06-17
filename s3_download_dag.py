from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import os

def download_from_s3(local_path: str, key: str, bucket_name: str) -> str:
    hook = S3Hook('s3_conn')
    filename = hook.download_file(local_path=local_path, key=key, bucket_name=bucket_name)
    return filename

def rename_file(ti, new_name: str) -> None:
    downloaded_file_name = ti.xcom_pull(task_ids=['download_from_s3'])
    downloaded_file_path = '/'.join(downloaded_file_name[0].split('/')[:-1])
    os.rename(src=downloaded_file_name[0], dst=f"{downloaded_file_path}/{new_name}")

with DAG(
    dag_id='s3_download_dag',
    schedule_interval='@daily',
    start_date=datetime(2023,6,12),
    catchup=False
) as dag:

    # Download a file
    task_download_from_s3 = PythonOperator(
        task_id='download_from_s3',
        python_callable=download_from_s3,
        op_kwargs={
            'local_path': '/home/vdp/airflow/data/',
            'key': 'posts.json',
            'bucket_name': 'alivebook-test-bucket'
        }
    )

    # Rename the file
    task_rename_file = PythonOperator(
        task_id='rename_file',
        python_callable=rename_file,
        op_kwargs={
            'new_name': 's3_downloaded_posts.json'
        }
    )

    task_download_from_s3 >> task_rename_file

# connection: id: s3_conn, type: amazon redshift, extra: {"aws_access_key_id": "xxx", "aws_secret_access_key": "xxx"}