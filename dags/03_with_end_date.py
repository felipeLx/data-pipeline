import datetime as dt
from pathlib import Path

import pandas as pd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="03_with_end_date",
    schedule_interval="@daily",
    start_date=dt.datetime(2023,12,25),
    end_date=dt.datetime(2023,12,28),
)

"""
0 * * * * = hourly (running on the hour)
0 0 * * * = daily (running at midnight)
0 0 * * 0 = weekly (running at midnight on Sunday)
Besides this, we can also define more complicated expressions such as the following:
0 0 1 * * = midnight on the first of every month
45 23 * * SAT = 23:45 every Saturday

@once Schedule once and only once.
@hourly Run once an hour at the beginning of the hour.
@daily Run once a day at midnight.
@weekly Run once a week at midnight on Sunday morning.
@monthly Run once a month at midnight on the first day of the month.
@yearly Run once a year at midnight on January 1.
"""