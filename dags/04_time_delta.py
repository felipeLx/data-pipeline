import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="04_time_delta",
    schedule_interval=dt.timedelta(days=3), # to run every 3 days
    start_date=dt.datetime(2023,12,25),
    end_date=dt.datetime(2023,12,27)
)


fetch_events = BashOperator(
    task_id = "fetch_events",
    bash_command = (
        "mkdir -p /data && "
        "curl -o /data/events.json "
        "https://localhost:5000/events?start_date=2023-12-25&end_date=2023-12-27"
    ),
    dag = dag
)

# function to PythonOperator
def _calculate_stats(input_path, output_path):
    """ Calculate Event Statistics """
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    Path(output_path).parent.mkdir(exist_ok=True)
    stats.to_csv(output_path, index="False")
    
calculate_stats = PythonOperator(
    task_id="calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs= {
        "input_path": "/data/events.json",
        "output_path": "/data/stats.csv",
    },
    dag = dag
)

fetch_events >> calculate_stats
