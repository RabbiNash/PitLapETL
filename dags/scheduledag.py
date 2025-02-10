from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1
import json
from datetime import datetime
import pytz
import numpy


# Enable caching for faster future queries
# fastf1.Cache.enable_cache('/Users/tinashe.makuti/Work/Code/f1')  # Replace 'cache_folder' with your desired cache directory

MONGO_CONN_ID = 'mongo_default'
YEAR = 2024

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

with DAG(dag_id='f1_schedule_pipeline_mongodb',
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False) as dag:

    @task()
    def extract_schedule():
        schedule = fastf1.get_event_schedule(YEAR)

        timezone = pytz.UTC

        combined_data = []

        # Iterate through each event in the schedule
        for _, event in schedule.iterrows():
            def format_date(date):
                """Helper function to format date fields into strings."""
                if numpy.isnat(numpy.datetime64(str(date))):
                    return ""

                return date.strftime('%Y-%m-%dT%H:%M:%SZ') if isinstance(date, datetime) else str(date)

            event_data = {
                'key': event['RoundNumber'] + YEAR,
                'round': int(event['RoundNumber']),
                'country': event['Country'],
                'officialEventName': event['OfficialEventName'],
                'eventName': event['EventName'],
                'eventFormat': event['EventFormat'],
                'year': "2025",
                'session1': event['Session1'],
                'Session1DateUtc': format_date(event.get('Session1DateUtc', None)),
                'session2': event['Session2'],
                'session2DateUtc': format_date(event.get('Session2DateUtc', None)),
                'session3': event['Session3'],
                'session3DateUtc': format_date(event.get('Session3DateUtc', None)),
                'session4': event['Session4'],
                'session4DateUtc': format_date(event.get('Session4DateUtc', None)),
                'session5': event['Session5'],
                'session5DateUtc': format_date(event.get('Session5DateUtc', None)),
            }

            try:
                # Load the race session for the event
                session = fastf1.get_session(YEAR, event['EventName'], 'Race')
                session.load()  # Load the session to ensure data is available

                # Retrieve race results (Top 20)
                results = session.results.iloc[0:].loc[:, ['Abbreviation', 'HeadshotUrl', 'Position', 'Status', 'Points', 'GridPosition', 'FullName','TeamColor', 'BroadcastName']]

                # Format the results for JSON output
                event_data['results'] = [
                    {
                        'position': int(row['Position']),
                        'driver': row['Abbreviation'],
                        'headshotUrl': row['HeadshotUrl'],
                        'points': int(row['Points']),
                        'status': row['Status'],
                        'gridPosition': row['GridPosition'],
                        'teamColor': row['TeamColor'],
                        'broadcastName': row['BroadcastName'],
                        'fullName': row['FullName']
                    }
                    for _, row in results.iterrows()
                ]
            except Exception as e:
                # Handle cases where results are not available
                print(f"Unable to load results for {event['EventName']} (Round {event['RoundNumber']}): {str(e)}")
                event_data['results'] = None
                event_data['error'] = str(e)

            # Append to combined data
            combined_data.append(event_data)

        return combined_data

    @task()
    def load_schedule(schedule):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['constructor_standings']

        for weekend in schedule:
            weekend['round'] = int(weekend['round'])

            # Upsert the document using the correct key ('round')
            collection.update_one(
                {'round': weekend['round']},  # Correct the typo here
                {'$set': weekend},
                upsert=True
            )

    # Set task dependencies
    schedules = extract_schedule()
    load_schedule(schedules)
    