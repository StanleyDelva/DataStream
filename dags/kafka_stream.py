import os
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

from cdp_scrapers.legistar_utils import get_legistar_events_for_timespan


default_args = {"owner": "airscholar", "start_date": datetime(2024, 3, 29, 10, 00)}


def get_data():
    # code to stream data from API
    

    # Get legistar events from beginning of month to current date
    fulton_legistar_events = get_legistar_events_for_timespan(
    client="fulton",
    begin=datetime(2024, datetime.now().month, 10),
    end=datetime(2024, datetime.now().month, datetime.now().day),
    # datetime.now().day
    )

    # fulton_events = json.dumps(fulton_legistar_events, indent=2)

    # single object (one event)
    #fulton_event = json.dumps(fulton_legistar_events[0], indent=2)

    # Writing to sample.json
    #with open("sample.json", "w") as outfile:
    #    outfile.write(fulton_event)

    #return fulton_event
    return fulton_legistar_events[0]

def stream_data():
    import json
    from kafka import KafkaProducer

    res = get_data()

    producer = KafkaProducer(bootstrap_servers=['broker:9092'], max_block_ms=6000, api_version=(1, 0, 0))
    producer.send('board_of_commission_events', json.dumps(res).encode('utf-8')) 


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
