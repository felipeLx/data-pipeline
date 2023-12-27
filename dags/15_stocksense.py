from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator

dag = DAG(
 dag_id="stocksense",
 start_date=airflow.utils.dates.days_ago(1),
 schedule_interval="@hourly",
)

#  _get_data function is called with all context variables as keyword arguments:
# (conf=..., dag=..., dag_run=..., execution_date=.
def _get_data(**context): # task context variables
    year, month, day, hour, *_ =  context["execution_date"].timetuple() # extract datetime
    url = ("https://dumps.wikimedia.org/other/pageviews/" f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz") # format URL with datetime components
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path) # retrieve data

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag
)
"""
To indicate to your future self and to other readers of your Airflow code about your
intentions of capturing the Airflow task context variables in the keyword arguments, a
good practice is to name this argument appropriately (e.g., “context”).
"""
def _print_context(**context):
    print(context)
    start = context["execution_date"]
    end = context["next_execution_date"]
    print(f"Start: {start}, end: {end}")

print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    dag=dag
)