"""
two of the most important properties of proper
Airflow tasks: atomicity and idempotency.

atomicity is frequently used in database systems, where an atomic transaction
is considered an indivisible and irreducible series of database operations such that
either all occur or nothing occurs. 
"""
import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="09_no_catchup",
    schedule_interval="@daily",
    start_date=dt.datetime(2023,12,25),
    end_date=dt.datetime(2023,12,31),
)

def _calculate_stats(**context):
    """Calculate event statistic"""
    input_path = context["template_dict"]["input_path"]
    output_path = context["template_dict"]["output_path"]
    
    events = pd.read_json(input_path)
    stats = events.groupby(["data", "user"]).size().reset_index()
    stats.to_csv(output_path, index=False)

def _send_stats(email, **context):
    stats = pd.read_csv(context["template_dict"]["stats_path"])
    email_stats(stats, email=email)


calculate_stats = PythonOperator(
    task_id = "calculate_stats",
    python_callable=_calculate_stats,
    op_kwargs={
        "input_path":"/data/events/{{ds}}.json",
        "output_path":"/data/stats/{{ds}}.csv",
    },
    dag=dag
)

send_stats = PythonOperator(
    task_id="send_stats",
    python_callable=_send_stats,
    op_kwargs={"email": "user@use.com"},
    templates_dict={"stats_path": "/data/stats/{{ds}}.csv"},
    dag=dag,
)

calculate_stats >> send_stats