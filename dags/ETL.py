from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="ETL",
    schedule_interval="@daily",  # Chạy hàng ngày
    start_date=datetime(2024, 3, 1),
    catchup=False
) as dag:
    
    Staging = BashOperator(
        task_id="Staging_Data_daily",
        bash_command="dbt run --project-dir /home/cao-ky/MY_FOLDER/Project/JOB_pipeline/ETL --profiles-dir /home/cao-ky/MY_FOLDER/Project/JOB_pipeline/ETL --select staging"
    )
        
    Mart = BashOperator(
        task_id="Mart_Data_daily",
        bash_command="dbt run --project-dir /home/cao-ky/MY_FOLDER/Project/JOB_pipeline/ETL --profiles-dir /home/cao-ky/MY_FOLDER/Project/JOB_pipeline/ETL --select mart"
    )
    
    Staging >> Mart