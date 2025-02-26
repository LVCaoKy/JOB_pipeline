from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

# Hàm Python để in thông báo
def print_hello():
    print("Hello, Airflow!")

# Khai báo DAG
with DAG(
    dag_id="test_dag",
    schedule_interval="@daily",  # Chạy hàng ngày
    start_date=datetime(2024, 2, 21),
    catchup=False
) as dag:

    # Task đơn giản in ra "Hello, Airflow!"
    task_hello = PythonOperator(
        task_id="say_hello",
        python_callable=print_hello
    )

    task_hello  # Kích hoạt task
