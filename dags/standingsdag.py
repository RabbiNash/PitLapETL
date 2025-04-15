from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
from fastf1.ergast import Ergast
import pandas as pd
import json

MONGO_CONN_ID = 'mongo_default'
YEAR = 2024

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

## DAG
with DAG(dag_id='f1_standings_etl_pipeline_mongodb',
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False) as dag:

    @task()
    def extract_standings():
        ergast = Ergast()
        standings = ergast.get_driver_standings(YEAR).content
        combined_standings = pd.concat(standings, ignore_index=True)

        combined_standings['constructorName'] = combined_standings['constructorNames'].apply(lambda name: name[0] if name else None)
        current_standings = combined_standings.loc[:, ['position', 'positionText', 'givenName', 'familyName', 'points', 'wins', 'driverId', 'driverNumber', 'constructorName']].dropna(subset=['driverNumber'])
        current_standings = current_standings.drop_duplicates(subset=['driverNumber'], keep='first')

        return current_standings.to_json(orient="records")

    @task()
    def load_standings(current_standings_json):
        current_standings = json.loads(current_standings_json)  # Convert JSON string to list of dictionaries
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['driver_standings']

        for standing in current_standings:
            # Convert numeric fields to appropriate types
            standing['position'] = int(standing['position'])
            standing['points'] = float(standing['points'])
            standing['wins'] = int(standing['wins'])
            standing['driverNumber'] = int(standing['driverNumber'])

            # Add timestamp
            standing['timestamp'] = pd.Timestamp.now()

            # Upsert the document
            collection.update_one(
                {'driverNumber': standing['driverNumber']},
                {'$set': standing},
                upsert=True
            )

    ## DAG Workflow - ETL Pipeline
    current_standings = extract_standings()
    load_standings(current_standings)
