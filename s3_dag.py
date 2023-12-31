from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.S3_hook import S3Hook

def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('s3_conn')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)

with DAG(
    dag_id='s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2023,6,12),
    catchup=False
) as dag:

    # Upload the file
    task_upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3,
        op_kwargs={
            'filename': '/home/vdp/airflow/data/posts.json',
            'key': 'posts.json',
            'bucket_name': 'alivebook-test-bucket'
        }
    )

# connection: id: s3_conn, type: amazon redshift, extra: {"aws_access_key_id": "xxx", "aws_secret_access_key": "xxx"}