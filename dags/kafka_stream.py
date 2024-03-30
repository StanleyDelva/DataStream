import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from redfin_scraper import RedfinScraper


default_args = {"owner": "airscholar", "start_date": datetime(2024, 3, 29, 10, 00)}

# Get path of current directory, then get csv file from sibling directory
path = os.path.realpath(__file__)
dir = os.path.dirname(path)

dir = dir.replace("dags", "zip_codes")

# changes the current directory to
# zip_codes folder
os.chdir(dir)

zip_code_database_path = os.path.join(dir, "zip_code_database.csv")


def stream_data():
    # code to stream data from API
    import json

    scraper = RedfinScraper()

    scraper.setup(zip_database_path=zip_code_database_path, multiprocessing=False)

    res = scraper.scrape(sold=False, sale_period=None, lat_tuner=1.5, lon_tuner=1.5)

    print(res)


with DAG(
    "user_automation",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        taskid="stream_data_from_api", python_callable=stream_data
    )


stream_data()
