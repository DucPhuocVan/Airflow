from datetime import datetime
from airflow.models import DAG
from airflow.operators.python import PythonOperator


def save_date(ti):
    dt=ti.xcom_pull(task_ids=['get_date'])
    if not dt:
        raise Exception('no data')
    with open('/home/vdp/airflow/data/date.txt', 'w') as f:
        f.write(dt[0])

def get_date() -> str:
    return(str(datetime.now()))

with DAG(
    dag_id='xcom_dag',
    schedule_interval='@daily',
    start_date=datetime(2023,6,12),
    catchup=False
) as dag:
    # 1. get date
    task_get_date = PythonOperator(
        task_id='get_date',
        python_callable=get_date,
        do_xcom_push=True
    )

    # 2. save date
    task_save_date = PythonOperator(
        task_id='save_date',
        python_callable=save_date
    )

    # pineline
    task_get_date >> task_save_date