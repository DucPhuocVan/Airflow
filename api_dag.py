import json
from datetime import datetime
from airflow.models import DAG
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.python import PythonOperator

def save_posts(ti):
    posts = ti.xcom_pull(task_ids=['get_posts'])
    with open('/home/vdp/airflow/data/posts.json', 'w') as f:
        json.dump(posts, f)

with DAG(
    dag_id='api_dag',
    schedule_interval='@daily',
    start_date=datetime(2023, 6, 12),
    catchup=False
) as dag:
    # check api active
    task_check_api=HttpSensor(
        task_id='check_api',
        http_conn_id='api_posts',
        endpoint='posts/'
    )
    # get data
    task_get_posts=SimpleHttpOperator(
        task_id='get_posts',
        http_conn_id='api_posts',
        endpoint='posts/',
        method='GET',
        response_filter=lambda reponse: json.loads(reponse.text),
        log_response=True,
        do_xcom_push=True
    )
    # 3. Save the posts
    task_save_posts = PythonOperator(
        task_id='save_posts',
        python_callable=save_posts
    )
    #pineline
    task_get_posts >> task_save_posts
