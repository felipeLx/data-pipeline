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
    
    # break the atomicity to send email after writing to CSV creates 2 pieces of work in a single function, which breaks the atomicity of the task.
    email_stats(stats, email="user@user.com")