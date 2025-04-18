from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

MONGO_CONN_ID = 'mongo_default'

def get_param(context, key):
    value = context['dag_run'].conf.get(key)
    if not value:
        raise ValueError(f"Missing required parameter: {key}")
    return int(value)

def get_string_param(context, key):
    value = context['dag_run'].conf.get(key)
    if not value:
        raise ValueError(f"Missing required parameter: {key}")
    return str(value)

with DAG(
    dag_id='f1_top_speeds_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "year": 2025,
        "round": 4,
        "session_name": "Practice 1"
    }
) as dag:

    @task()
    def validate_params(**context):
        return {
            'year': get_param(context, 'year'),
            'round': get_param(context, 'round'),
            'session_name': get_string_param(context, 'session_name')
        }

    @task()
    def extract_top_speeds(parameters: dict):
        try:
            session = fastf1.get_session(parameters["year"], parameters["round"], parameters["session_name"])
            session.load()

            top_speeds = []

            for drv in session.drivers:
                driver_info = session.get_driver(drv)
                abbreviation = driver_info['Abbreviation']

                driver_laps = session.laps.pick_drivers(drv).pick_not_deleted()

                max_speed = 0

                for lap in driver_laps.iterlaps():
                    try:
                        telemetry = lap[1]
                        speed = telemetry['SpeedST']
                        if speed > max_speed:
                            max_speed = speed
                    except Exception as e:
                        print(f"Error processing telemetry for {abbreviation}: {e}")

                top_speeds.append({
                    'driver': abbreviation,
                    'topSpeed': max_speed
                })

            return {
                "year": parameters["year"],
                "round": parameters["round"],
                "sessionName":  parameters["session_name"],
                "eventFormat": session.event['EventFormat'],
                "speeds": top_speeds
            }

        except Exception as e:
            raise RuntimeError(f"Failed to load session: {str(e)}")

    @task()
    def load_to_mongodb(data: dict):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['top_speeds']

        # Upsert based on year + round + session
        query = {
            "year": data['year'],
            "round": data['round'],
            "sessionName": data['sessionName']
        }
        collection.update_one(query, {"$set": data}, upsert=True)

    lap_data = extract_top_speeds(validate_params())
    load_to_mongodb(lap_data)
