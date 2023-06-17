import os
import pandas as pd
from datetime import datetime
from airflow.models import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

def get_datetime():
    return str(datetime.now())

def process_datetime():
    dt = get_datetime()
    if not dt:
        raise Exception('No datetime value.')

    dt = dt.split()
    return {
        'date': dt[0],
        'time': dt[1]
    }

def save_datetime():
    dt_processed = process_datetime()
    if not dt_processed:
        raise Exception('No datetime value.')
    
    df = pd.DataFrame(dt_processed, index=[0])
    csv_path = Variable.get('first_dag_csv_path')
    if os.path.exists(csv_path):
        df_header = False
        df_mode = 'a'
    else:
        df_header = True
        df_mode = 'w'

    df.to_csv(csv_path, index=False, mode=df_mode, header=df_header)

with DAG(
    dag_id='first_airflow_dag',
    schedule_interval='* * * * *',
    start_date=datetime(year=2023, month=6, day=11),
    catchup=False
) as dag:
    
    # 1. Get current datetime
    task_get_datetime = PythonOperator(
        task_id='get_datetime',
        #bash_command='date'
        python_callable=get_datetime
    )
    
    # 2. Process current datetime
    task_process_datetime = PythonOperator(
        task_id='process_datetime',
        python_callable=process_datetime
    )

    # 3. Save processed datetime
    task_save_datetime = PythonOperator(
        task_id='save_datetime',
        python_callable=save_datetime
    )

    # pineline
    task_get_datetime >> task_process_datetime >> task_save_datetime
    
#test task: airflow tasks test <dag_name> <task_name> <date_in_the_past>
#variable on Aiflow Gui: {first_dag_csv_path : /home/vdp/Documents/datetimes.csv}
