import json
import pathlib

import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

dag = DAG(
    dag_id = "download_rocket_launches",
    start_date = airflow.utils.dates.days_ago(14),
    schedule_interval = "@daily", # airflow alias to midnight
)   

download_launches = BashOperator(
    task_id = "download_launches",
    bash_command = "curl -o /tmp/launches.json -L https://ll.thespacedevs.com/2.0.0/launch/upcoming/",
    dag = dag,
)

def _get_pictures():
    """" Python function will parse the response and download the pictures of the rocket launches."""
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        # read like a dict so we can mingle the data
        launches = json.load(f)
        image_urls = [launch["image"] for launch in launches["results"]]
        # for every, fetch the element "image"
        for image_url in image_urls:
            # each image URL is called to download the image and save it in /tmp/images
            try:
                response = requests.get(image_url)
                image_filename = image_url.split("/")[-1]
                target_file = f"/tmp/images/{image_filename}"
                
                with open(target_file, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {image_url} to {target_file}")
            except requests_exceptions.MissingSchema:
                print(f"{image_url} appears to be an invalid URL.")
            except requests_exceptions.ConnectionError:
                print(f"Could not connect to {image_url}.")
    
""" Call the Python function in the DAG with a PythonOperator """
get_pictures = PythonOperator(
    task_id = "get_pictures",
    python_callable = _get_pictures,
    dag = dag,
)

notify = BashOperator(
    task_id = "notify",
    bash_command = 'echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag = dag,
)

# Set the order of tasks in the DAG
download_launches >> get_pictures >> notify
