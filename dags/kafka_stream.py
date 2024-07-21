import uuid
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
    #print(fulton_legistar_events[0])
    return fulton_legistar_events

def format_data(res):

    data = {}
    data['id'] = str(uuid.uuid4())
    data['event_id'] = res['EventId']
    data['event_date'] = res['EventDate']
    data['event_time'] = res['EventTime']
    data['event_in_site_url'] = res['EventInSiteURL']

    # turn json content into string of bytes and store
    #res_bytes = res.encode('utf-8')
    data['event_data'] = res

    #with open("sampleData1.json", "w") as outfile:
    #    outfile.write(json.dumps(data, indent=2))

    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import logging

    res = get_data()

    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=300000, compression_type='lz4', 
                             acks=1, max_request_size=2000000, metadata_max_age_ms=600000,api_version=(1, 0, 0))

    try:
        # Find and send all meetings in response
        for x in range(len(res)):
            meeting_message = format_data(res[x])

            logging.basicConfig(level=logging.DEBUG)
            producer.send('board_of_commission_events', json.dumps(meeting_message).encode('utf-8'))
            print("\n\nMESSAGE SENT\n\n") 
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        
            


with DAG("BoC_automation",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:

    streaming_task = PythonOperator(
        task_id="stream_data_from_api", 
        python_callable=stream_data
    )