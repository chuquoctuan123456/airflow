import datetime
import pendulum
import os
import requests
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
def get_data():
    data_path = "/opt/airflow/data/employees.csv"
    os.makedirs(os.path.dirname(data_path), exist_ok=True)

    url = "https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/tutorial/pipeline_example.csv"

    response = requests.request("GET", url)

    with open(data_path, "w") as file:
        file.write(response.text)

    postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    with open(data_path, "r") as file:
        cur.copy_expert(
            "COPY learn.employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
            file,
        )
    conn.commit()

def merge_data():
    query = """
        INSERT INTO learn.employees
        SELECT *
        FROM (
            SELECT DISTINCT *
            FROM learn.employees_temp
        ) t
        ON CONFLICT ("Serial Number") DO UPDATE
        SET
          "Employee Markme" = excluded."Employee Markme",
          "Description" = excluded."Description",
          "Leave" = excluded."Leave";
    """
    try:
        postgres_hook = PostgresHook(postgres_conn_id="postgres_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        cur.execute(query)
        conn.commit()
        return 0
    except Exception as e:
        print(f"Error: {e}")
        return 1

with DAG(
    dag_id="PROCESS_EMPLOYEE",
    schedule_interval="0 0 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
) as dag:

    # Task 1: Tạo bảng employees
    create_employees_table = PostgresOperator(
        task_id="create_employees_table",
        postgres_conn_id="postgres_default",
        sql=""" 
            CREATE TABLE IF NOT EXISTS learn.employees (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )

    # Task 2: Tạo bảng employees_temp
    # Task 2: Tạo bảng employees_temp
    create_employees_temp_table = PostgresOperator(
        task_id="create_employees_temp_table",
        postgres_conn_id="postgres_default",
        sql=""" 
            DROP TABLE IF EXISTS learn.employees_temp;
            CREATE TABLE learn.employees_temp (
                "Serial Number" NUMERIC PRIMARY KEY,
                "Company Name" TEXT,
                "Employee Markme" TEXT,
                "Description" TEXT,
                "Leave" INTEGER
            );""",
    )


    # Task 3: Lấy dữ liệu và lưu vào bảng tạm
    get_data_task = PythonOperator(
        task_id="get_data",
        python_callable=get_data,
    )

    # Task 4: Merge dữ liệu vào bảng chính
    merge_data_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data,
    )

    # Xác định thứ tự thực thi các task
    [create_employees_table, create_employees_temp_table] >> get_data_task >> merge_data_task
