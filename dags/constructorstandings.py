from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import json
import requests
from datetime import datetime

MONGO_CONN_ID = 'mongo_default'
API_URL = 'https://api.jolpi.ca/ergast/f1/2025/constructorstandings/'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

def map_data(original_data):
    target_data = []

    for standing in original_data["MRData"]["StandingsTable"]["StandingsLists"][0]["ConstructorStandings"]:
        constructor = standing["Constructor"]

        # Safely retrieve values with defaults
        position = standing.get("position", "Unknown")
        position_text = standing.get("positionText", "Unknown")
        points = standing.get("points", 0)
        wins = standing.get("wins", 0)

        result = {
            "constructorId": constructor.get("constructorId"),
            "constructorName": constructor.get("name", "Unknown"),
            "points": int(points),
            "position": int(position) if position != "Unknown" else 0,
            "positionText": position_text,
            "timestamp": datetime.utcnow().isoformat(),  # Use current UTC timestamp
            "wins": int(wins)
        }

        target_data.append(result)

    return target_data

with DAG(
    dag_id='f1_constructor_standings_etl_pipeline_mongodb',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=["f1", "etl", "mongodb"]
) as dag:

    @task()
    def extract_standings():
        response = requests.get(API_URL)
        response.raise_for_status()  # Raise exception on bad response
        raw_data = response.json()

        # Transform the data using your mapping logic
        mapped_data = map_data(raw_data)
        return json.dumps(mapped_data)  # Return as JSON string for XCom

    @task()
    def load_standings(mapped_data_json):
        mapped_data = json.loads(mapped_data_json)
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['constructor_standings']

        for standing in mapped_data:
            collection.update_one(
                {'constructorId': standing['constructorId']},
                {'$set': standing},
                upsert=True
            )

    data = extract_standings()
    load_standings(data)
