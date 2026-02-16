from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator

from functions import write_to_csv, max_write_time
# from functions import 

default_args = {
    "owner": "eli",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id = "hourly_write_to_csv",
    default_args = default_args,
    description = "test dag to write the time to a csv every hour",
    start_date = datetime(2026,2,1),
    schedule = "0 * * * *",
    catchup=False,
    max_active_runs = 1,
    tags = ["etl","csv","logging"]
) as dag:
    
    write_csv_task = PythonOperator(
        task_id = "write_to_csv_task",
        python_callable=write_to_csv
    )

    max_write_task = PythonOperator(
        task_id = "find_latest_write",
        python_callable=max_write_time
    )

    notify_success = EmailOperator(
        task_id="airflow_notify",
        to=["eaj.12.web@gmail.com"],
        subject="Airflow success: hourly_write_to_csv",
        html_content="""
        <p>DAG <b>hourly_write_to_csv</b> succeeded.</p>
        <p>Run ID: {{ run_id }}</p>
        <p>Logical date: {{ ds }}</p>
        """
    )

    write_csv_task >> max_write_task >> notify_success