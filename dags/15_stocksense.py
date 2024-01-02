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

def _get_data_op_useless(execution_date, **context):
    year, month, day, hour, *_ = execution_date.timetuple()
 # This tells Python we expect to receive an argument named execution_date. It will not be captured in the context argument
 # Now, we can directly use the execution_date variable instead of having to extract it from **context with context["execution_date"]

def _get_data_op(output_path, **context):
    year, month, day, hour, *_ = context["execution_date"].timetuple()
    url = (
    "https://dumps.wikimedia.org/other/pageviews/"
    f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz"
    )
    request.urlretrieve(url, output_path)
    # output_path now configurable via argument

get_data_op = PythonOperator(
    task_id="get_data_op",
    python_callable=_get_data_op_useless,
    op_args=["/tmp/wikipageviews.gz"],  # Provide additional variables to the callable with op_args.
    dag=dag,
)

get_data_oth = PythonOperator(
 task_id="get_data_oth",
 python_callable=_get_data_op,
 op_kwargs={"output_path": "/tmp/wikipageviews.gz"}, # A dict given to op_kwargs will be passed as keyword arguments to the callable.
 dag=dag,
)

get_data_oth