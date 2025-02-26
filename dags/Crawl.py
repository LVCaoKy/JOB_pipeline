from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Khai báo DAG
with DAG(
    dag_id="Crawl_BDS",
    schedule_interval="@daily",  # Chạy hàng ngày
    start_date=datetime(2024, 3, 1),
    catchup=False
) as dag:

    # Task đơn giản in ra "Hello, Airflow!"
    Crawl_DBS = BashOperator(
        task_id="Crawl_Data_daily",
        bash_command="python3 /home/cao-ky/MY_FOLDER/Project/JOB_pipeline/data_raw/Crawl.py"
    )

    Crawl_DBS
