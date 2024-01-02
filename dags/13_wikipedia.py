import datetime as dt
from pathlib import Path

import pandas as pd
import airflow.utils.dates
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id="chapter4_stocksense_bashoperator",
    start_date=airflow.utils.dates.days_ago(3),
    schedule_interval="@hourly"
)

"""
execution_date. Airflow uses the Pendulum (https://
pendulum.eustace.io) library for datetimes, and execution_date is such a Pendulum
datetime object
"""
get_data = BashOperator(
    task_id="get_data",
    bash_command=(
        "curl -o /tmp/wikipageviews.gz "
        "https://dumps.wikimedia.org/other/pageviews/"
        "{{ execution_date.year }}/"  
    	"{{ execution_date.year }}-"
        "{{ '{:02}'.format(execution_date.month) }}/"
        "pageviews-{{ execution_date.year }}"
        "{{ '{:02}'.format(execution_date.month) }}"
        "{{ '{:02}'.format(execution_date.day) }}-"
        "{{ '{:02}'.format(execution_date.hour) }}0000.gz"
    ),
    dag=dag
)
"""
The Wikipedia pageviews URL requires zero-padded months, days, and hours (e.g.,
“07” for hour 7). Within the Jinja-templated string we therefore apply string format￾ting for padding:
{{ '{:02}'.format(execution_date.hour) }}
"""