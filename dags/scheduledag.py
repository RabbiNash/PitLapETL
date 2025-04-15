from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1
from datetime import datetime
import pytz
import numpy

CACHE_DIR = '/opt/airflow/cache/f1'

MONGO_CONN_ID = 'mongo_default'
YEAR = 2025

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='f1_schedule_pipeline_mongodb',
         default_args=default_args,
         schedule_interval=None,
         catchup=False) as dag:

    @task()
    def extract_schedule():
        fastf1.Cache.enable_cache(CACHE_DIR)
        schedule = fastf1.get_event_schedule(YEAR)

        timezone = pytz.UTC

        combined_data = []

        for _, event in schedule.iterrows():
            def format_date(date):
                """Helper function to format date fields into strings."""
                if numpy.isnat(numpy.datetime64(str(date))):
                    return ""

                return date.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(date, datetime) else str(date)

            event_data = {
                'key': str(event['RoundNumber']) + "-" + str(YEAR),
                'round': int(event['RoundNumber']),
                'country': event['Country'],
                'officialEventName': event['OfficialEventName'],
                'eventName': event['EventName'],
                'eventFormat': event['EventFormat'],
                'year': str(YEAR),
                'session1': event['Session1'],
                'session1DateUtc': format_date(event.get('Session1DateUtc', None)),
                'session2': event['Session2'],
                'session2DateUtc': format_date(event.get('Session2DateUtc', None)),
                'session3': event['Session3'],
                'session3DateUtc': format_date(event.get('Session3DateUtc', None)),
                'session4': event['Session4'],
                'session4DateUtc': format_date(event.get('Session4DateUtc', None)),
                'session5': event['Session5'],
                'session5DateUtc': format_date(event.get('Session5DateUtc', None)),
            }

            # Append to combined data
            combined_data.append(event_data)

        return combined_data

    @task()
    def load_schedule(schedule):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['schedule']

        for weekend in schedule:
            weekend['round'] = int(weekend['round'])

            collection.update_one(
                {'key': str(weekend['round']) + "-" + str(YEAR)},
                {'$set': weekend},
                upsert=True
            )

    schedules = extract_schedule()
    load_schedule(schedules)
