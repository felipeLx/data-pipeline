import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="08_templated_path",
    schedule_interval=dt.timedelta(days=3), # to run every 3 days
    start_date=dt.datetime(2023,12,25),
    end_date=dt.datetime(2023,12,27)
)

fetch_events = BashOperator(
    # divide our data set into daily batches by writing the output of the task to a file bearing the name of the corresponding execution date.
    task_id="fetch_events",
    bash_command=(
    "mkdir -p /data/events && " 
    "curl -o /data/events/{{ds}}.json " 
    "http:/ /localhost:5000/events?"
    "start_date={{ds}}&"
    "end_date={{next_ds}}",
    ), 
    dag=dag,
)

def _calculate_stats(input_path, output_path):
    """ Calculate event statistics """
    Path(output_path).parent_mkdir(exist_ok=True)
    events = pd.read_json(input_path)
    stats = events.groupby(["date", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

# we can calculate these statistics more efficiently for each separate partition by changing the input and output paths of this task to point to the partitioned event data and a partitioned output file
def _calculate_stats(**context): 
 """Calculates event statistics."""
 input_path = context["templates_dict"]["input_path"] 
 output_path = context["templates_dict"]["output_path"]
 Path(output_path).parent.mkdir(exist_ok=True)
 events = pd.read_json(input_path)
 stats = events.groupby(["date", "user"]).size().reset_index()
 stats.to_csv(output_path, index=False)
 
calculate_stats = PythonOperator(
    task_id = "calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path":"/data/events/{{ds}}.json",
        "output_path":"/data/stats/{{ds}}.csv",
    },
    dag=dag
)