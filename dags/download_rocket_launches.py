import json
import pathlib

import airflow
import requests
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(
    dag_id="download_rocket_launches",
    start_date=airflow.utils.dates.days_ago(14),
    schedule_interval="@daily",
    catchup=False
)


check_current_place = BashOperator(
    task_id="check_current_place",
    bash_command="echo `pwd`",
    dag=dag
)


download_launches = BashOperator(
    task_id="download_launches",
    bash_command="curl -o /tmp/launches.json 'https://launchlibrary.net/1.4/launch?next=5&mode=verbose'",
    dag=dag,
)


def _get_pictures():
    # Ensure directory exists
    pathlib.Path("/tmp/images").mkdir(parents=True, exist_ok=True)
    # Download all pictures in launches.json
    with open("/tmp/launches.json") as f:
        launches = json.load(f)
        image_urls = [launch["rocket"]["imageURL"] for launch in launches["launches"]]
        for index, image_url in enumerate(image_urls):
            print(f"image_url : {image_url}")
            # response = requests.get(image_url, verify=False)
            # print(response)
            image_filename = image_url.split("/")[-1]
            target_file = f"/tmp/images/{index}_{image_filename}"
            print(f"target_file : {target_file}")
            with open(target_file, "wb") as f:
                # f.write(response.content)
                f.write(bytearray(target_file, encoding="utf-8"))
            print(f"Downloaded {image_url} to {target_file}")


get_pictures = PythonOperator(
    task_id="get_pictures",
    python_callable = _get_pictures,
    dag = dag,
)

notify = BashOperator(
    task_id="notify",
    bash_command='echo "There are now $(ls /tmp/images/ | wc -l) images."',
    dag=dag,
)

check_current_place >> download_launches >> get_pictures >> notify
