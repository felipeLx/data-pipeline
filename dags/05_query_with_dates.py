import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="05_query_with_dates",
    schedule_interval=dt.timedelta(days=3), # to run every 3 days
    start_date=dt.datetime(2023,12,25),
    end_date=dt.datetime(2023,12,27)
)


fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "https://localhost:5000/events?"
        "start_date=2023-12-25&"
        "end_date=2023-12-27"
    ),
    dag = dag
)
