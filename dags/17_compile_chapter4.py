from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG(
 dag_id="chapter4",
 start_date=airflow.utils.dates.days_ago(1),
 schedule_interval="@hourly",
)

extract_gz = BashOperator(
 task_id="extract_gz",
 bash_command="gunzip --force /tmp/wikipageviews.gz",
 dag=dag,
)

def _fetch_pageviews(pagenames):
    result = dict.fromkeys(pagenames, 0) 
    # Open the file written in previous task.
    with open(f"/tmp/wikipageviews", "r") as f: 
        for line in f:
            # Extract the elements on a line.
            domain_code, page_title, view_counts, _ = line.split(" ") 
            # Extract the elements on a line. Check if page_ title is in given pagenames.
            if domain_code == "en" and page_title in pagenames: 
                result[page_title] = view_counts
                print(result)

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={
    "pagenames": {
    "Google",
    "Amazon",
    "Apple",
    "Microsoft",
    "Facebook",
    }
    },
    dag=dag,
)
