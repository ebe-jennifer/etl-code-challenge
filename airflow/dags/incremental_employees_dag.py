from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from sqlalchemy import create_engine
from google.cloud import bigquery
import pandas as pd

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 18),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'incremental_dag_employees', 
    default_args=default_args,
    description='Move Incremental DataFrame to Google Cloud Storage and BigQuery',
    schedule_interval=timedelta(days=1),  # Run daily
)

def create_incremental_dataframe(**kwargs):
    # SQLAlchemy connection parameters
    engine = create_engine('mysql+pymysql://root@0.0.0.0:3306/employees')

    # Query to extract incremental data for one day
    # query = """
    # SELECT * 
    # FROM employees.employees 
    # WHERE hire_date >= DATE_SUB(NOW(), INTERVAL 1 DAY)
    # """
    
    # Query to extract incremental data for 3 days as I suggested
    # query = """
    # SELECT * 
    # FROM employees.employees 
    # WHERE hire_date >= DATE_SUB(NOW(), INTERVAL 3 DAY)
    # """
    
    query = """
    SELECT * 
    FROM employees.employees 
    WHERE hire_date >= '2000-01-28'
    """

    df = pd.read_sql(query, engine)
    csv_filename = '/tmp/incremental_data.csv'
    df.to_csv(csv_filename, index=False)

    kwargs['ti'].xcom_push(key='csv_filename', value=csv_filename)
    return csv_filename

def _move_incremental_dataframe_to_gcs(**kwargs): 
    csv_filename = kwargs['ti'].xcom_pull(task_ids='create_incremental_dataframe', key='csv_filename')
    bucket_name = 'mysql-to-gbq-test'
    destination_blob_name = 'incremental_data.csv'
    upload_command = f"gsutil cp {csv_filename} gs://{bucket_name}/{destination_blob_name}"
    
    task = BashOperator(
        task_id='move_incremental_dataframe_to_gcs_task',
        bash_command=upload_command,
        dag=dag,
    )
    task.execute(context=kwargs)

def _load_incremental_data_to_bigquery_staging(**kwargs):
    dataset_id = 'mysql_to_gbq'
    table_id = 'employees_staging'
    bucket_name = 'mysql-to-gbq-test'
    csv_filename = 'incremental_data.csv' 

    load_command = f"bq load --autodetect --source_format=CSV {dataset_id}.{table_id} gs://{bucket_name}/{csv_filename}"
    
    task = BashOperator(
        task_id='load_incremental_data_to_bigquery_staging_task',
        bash_command=load_command,
        dag=dag,
    )
    task.execute(context=kwargs)

def _insert_new_employees_from_staging(**kwargs):
    client = bigquery.Client()
    # Define dataset and table names
    staging_table_id = 'employees_staging'
    dataset_id = 'mysql_to_gbq'
    target_table_id = 'employees'

    
    query = f"""
    INSERT INTO `{dataset_id}.{target_table_id}` (emp_no, birth_date, first_name, last_name, gender, hire_date)
    SELECT emp_no, birth_date, first_name, last_name, gender, hire_date
    FROM `{dataset_id}.{staging_table_id}`
    WHERE emp_no > (SELECT IFNULL(MAX(emp_no), 0) FROM `{dataset_id}.{staging_table_id}`)
    """

    # Construct bq query command
    load_command = f"bq query --use_legacy_sql=false '{query}'"
    
    task = BashOperator(
        task_id='insert_new_employees_from_staging',
        bash_command=load_command,
        dag=dag,
    )
    task.execute(context=kwargs)

create_incremental_dataframe_task = PythonOperator(
    task_id='create_incremental_dataframe',
    python_callable=create_incremental_dataframe,
    provide_context=True,
    dag=dag
)
move_incremental_dataframe_task = PythonOperator(
    task_id='move_incremental_dataframe_to_gcs',
    python_callable=_move_incremental_dataframe_to_gcs,
    provide_context=True,
    dag=dag
)
load_incremental_data_to_staging_task = PythonOperator(
    task_id='load_incremental_data_to_bigquery',
    python_callable=_load_incremental_data_to_bigquery_staging,
    provide_context=True,
    dag=dag
)

insert_to_main_tabletask = PythonOperator(
    task_id='insert_new_employees_from_staging_task',
    python_callable=_insert_new_employees_from_staging,
    dag=dag,
)

create_incremental_dataframe_task >> move_incremental_dataframe_task >> load_incremental_data_to_staging_task >> insert_to_main_tabletask

if __name__ == "__main__":
    dag.cli()
