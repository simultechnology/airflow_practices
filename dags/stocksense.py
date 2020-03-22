from urllib import request

import airflow
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

dag = DAG(dag_id="stocksense", start_date=airflow.utils.dates.days_ago(1), schedule_interval="@hourly")


def _get_data(execution_date, **_):
    year, month, day, hour, *_ = execution_date.timetuple()
    url = (
        "https://dumps.wikimedia.org/other/pageviews/"
        f"{year}/{year}-{month:02}/pageviews-{year}{month:02}{day:02}-{hour:02}0000.gz"
    )
    print(url)
    output_path = "/tmp/wikipageviews.gz"
    request.urlretrieve(url, output_path)

get_data = PythonOperator(task_id="get_data", python_callable=_get_data, provide_context=True, dag=dag)

extract_gz = BashOperator(task_id="extract_gz", bash_command="gunzip --force /tmp/wikipageviews.gz", dag=dag)


def _fetch_pageviews(pagenames, execution_date, **_):
    print('start!!')
    result = dict.fromkeys(pagenames, 0)
    with open("/tmp/wikipageviews", "r") as f:
        for line in f:
            domain_code, page_title, view_counts, _ = line.split(" ")
            if domain_code == "en" and page_title in pagenames:
                result[page_title] = view_counts
            print(result)

    with open("/tmp/postgres_query.sql", "w") as f:
        for pagename, pageviewcount in result.items():
            f.write(
                "INSERT INTO pageview_counts VALUES ("
                f"'{pagename}', '{pageviewcount}', '{execution_date}'"
                ");\n"
            )

fetch_pageviews = PythonOperator(
    task_id="fetch_pageviews",
    python_callable=_fetch_pageviews,
    op_kwargs={"pagenames": {"Google", "Amazon", "Apple", "Microsoft", "Facebook"}},
    dag=dag,
)

# get_data >> extract_gz >> fetch_pageviews
fetch_pageviews
