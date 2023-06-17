import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from airflow.hooks.S3_hook import S3Hook
import requests


def get(url:str, bucket_name: str):
    endpoint = url.split('/')[-1]
    now=datetime.now()
    now=f"{now.year}-{now.month}-{now.day}T{now.hour}-{now.minute}-{now.second}"
    res=requests.get(url)
    res=json.loads(res.text)
    dt=res
    if not dt:
        raise Exception('no data') 
    df=pd.DataFrame(dt)
    df.to_csv('file.csv', index=False)
    hook = S3Hook('s3_conn')
    hook.load_file(filename='file.csv', key=f"{endpoint}-{now}.csv", bucket_name=bucket_name)

with DAG(
    dag_id='api_s3_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 12),
    catchup=False
) as dag:
    # 2. get data
    task_get_posts=PythonOperator(
        task_id='get_posts',
        python_callable=get,
        op_kwargs={'url':'https://gorest.co.in/public/v2/posts',
                   'bucket_name': 'alivebook-test-bucket'}
    )
    #pineline
    task_get_posts 
