from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

dag = DAG(
 dag_id="creating_table",
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

#  statement for storing output
"""
CREATE TABLE pageview_counts (
 pagename VARCHAR(50) NOT NULL,
 pageviewcount INT NOT NULL,
 datetime TIMESTAMP NOT NULL
);

INSERT statement storing output in the pageview_counts table
INSERT INTO pageview_counts VALUES ('Google', 333, '2019-07-17T00:00:00');

 In Airflow, there are two ways of passing data between tasks:
1) By using the Airflow metastore to write and read results between tasks. This is
called XCom and covered in chapter 5.
2) By writing results to and from a persistent location (e.g., disk or database)
between tasks.

 In order to decide how to store the intermediate data, we must know where and
how the data will be used again. Since the target database is a Postgres, we’ll use the
PostgresOperator to insert data. First, we must install an additional package to import
the PostgresOperator class in our project:
pip install apache-airflow-providers-postgres
"""