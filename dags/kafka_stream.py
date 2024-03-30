import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from cdp_scrapers.legistar_utils import get_legistar_events_for_timespan


default_args = {"owner": "airscholar", "start_date": datetime(2024, 3, 29, 10, 00)}


# necessary zip codes for ATL area
zip_codes = [str(item) for item in range (30301, 30340)]


def get_data():
    # code to stream data from API
    import json

    # Get legistar events from beginning of month to current date
    fulton_legistar_events = get_legistar_events_for_timespan(
    client="fulton",
    begin=datetime(2024, datetime.now().month, 1),
    end=datetime(2024, datetime.now().month, datetime.now().day),
    )

    fulton_events = json.dumps(fulton_legistar_events, indent=2)

    return fulton_events

def stream_data():
    print(get_data())


with DAG(
    "user_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", 
        python_callable=stream_data
    )


stream_data()
