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
    catchup=False # FAlse to break the default and will avoid Airflow to schedule and run any past schedule intervals that have not been run.
    # Backfilling in Airflow. By default, Airflow will run tasks for all past intervals up to the current time.
)


