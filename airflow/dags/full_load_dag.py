from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from sqlalchemy import create_engine
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'backfill_dag_employees', 
    default_args=default_args,
    description='Move DataFrame to Google Cloud Storage and BigQuery',
    schedule_interval=None,
)

def create_dataframe(**kwargs):
        # SQLAlchemy connection parameters
    engine = create_engine('mysql+pymysql://root@0.0.0.0:3306/employees')

    # Query to extract data
    query = "SELECT * FROM employees.employees"

    df = pd.read_sql(query, engine)
    csv_filename = '/tmp/data.csv'
    df.to_csv(csv_filename, index=False)

    kwargs['ti'].xcom_push(key='csv_filename', value=csv_filename)
    return csv_filename

def _move_dataframe_to_gcs(**kwargs): 
    csv_filename = kwargs['ti'].xcom_pull(task_ids='create_dataframe', key='csv_filename')
    bucket_name = 'mysql-to-gbq-test'
    destination_blob_name = 'data.csv'
    upload_command = f"gsutil cp {csv_filename} gs://{bucket_name}/{destination_blob_name}"
    
    task = BashOperator(
        task_id='move_dataframe_to_gcs_task',
        bash_command=upload_command,
        dag=dag,
    )
    task.execute(context=kwargs)

def _load_data_to_bigquery(**kwargs):
    dataset_id = 'mysql_to_gbq'
    table_id = 'employees'
    bucket_name = 'mysql-to-gbq-test'
    csv_filename = 'data.csv' 


    load_command = f"bq load --autodetect --source_format=CSV {dataset_id}.{table_id} gs://{bucket_name}/{csv_filename}"
    
    task = BashOperator(
        task_id='load_data_to_bigquery_task',
        bash_command=load_command,
        dag=dag,
    )
    task.execute(context=kwargs)

create_dataframe_task = PythonOperator(task_id='create_dataframe', python_callable=create_dataframe, provide_context=True, dag=dag)
move_dataframe_task = PythonOperator(task_id='move_dataframe_to_gcs', python_callable=_move_dataframe_to_gcs, provide_context=True, dag=dag)
load_data_task = PythonOperator(task_id='load_data_to_bigquery', python_callable=_load_data_to_bigquery, provide_context=True, dag=dag)


create_dataframe_task >> move_dataframe_task >> load_data_task

if __name__ == "__main__":
    dag.cli()
