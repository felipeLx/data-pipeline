from urllib import request
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

dag = DAG(
 dag_id="postgres_operator",
 start_date=airflow.utils.dates.days_ago(1),
 schedule_interval="@hourly",
)

def _get_data(**context): # task context variables
    print('context', context)
    year, month, day, hour, *_ =  context["execution_date"].timetuple() # extract datetime
    url = ("https://dumps.wikimedia.org/other/pageviews/" f"{year}/{year}-{month:0>2}/pageviews-{year}{month:0>2}{day:0>2}-{hour:0>2}0000.gz") # format URL with datetime components
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path) # retrieve data

get_data = PythonOperator(
    task_id="get_data",
    python_callable=_get_data,
    dag=dag
)

extract_gz = BashOperator(
 task_id="extract_gz",
 bash_command="gunzip --force /tmp/wikipageviews.gz",
 dag=dag,
)
 # template_searchpath="/tmp",  Path to search for sql file

def _fetch_pageviews(pagenames):
    print('pagenames', pagenames)
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
"""
# Writing INSERT statements to feed to the PostgresOperator
def _fetch_pageviews_oth(pagenames, execution_date, **_):
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r")as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result["page_title"] = view_counts
    
    with open("/tmp/postgres_query.sql", "w") as f:
        f.write(
            "CREATE TABLE IF NOT EXISTS pageview_counts (pagename VARCHAR(50) NOT NULL, pageviewcount INT NOT NULL, datetime TIMESTAMP NOT NULL);"
        )
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"`{pagename}`. {pageviewcount}, `{execution_date}`"
                ");\n"
            )

fetch_pageviews_second = PythonOperator(
    task_id = "fetch_pageviews_second",
    python_callable=_fetch_pageviews_oth,
    op_kwargs={"pagenames": {"Google", "Amazon", "Microsoft", "Facebook"}},
    dag=dag,
)

get_data >> extract_gz >> fetch_pageviews_second