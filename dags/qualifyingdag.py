from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1
import pandas as pd

MONGO_CONN_ID = 'mongo_default'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

def get_param(context, key):
    value = context['dag_run'].conf.get(key)
    if not value:
        raise ValueError(f"Missing required parameter: {key}")
    return int(value)

with DAG(
    dag_id='f1_quali_results_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['f1', 'qualification'],
    params={
        "year": 2025,
        "round": 4
    }
) as dag:

    @task
    def validate_params(**context):
        return {
            'year': get_param(context, 'year'),
            'round': get_param(context, 'round')
        }

    @task
    def extract_quali_results(parameters: dict):
        try:
            schedule = fastf1.get_event_schedule(parameters['year'])
            event = schedule.loc[schedule['RoundNumber'] == parameters['round']].iloc[0]

            session = fastf1.get_session(parameters['year'], event['EventName'], 'Q')
            session.load()

            def format_laptime(td):
                if pd.isna(td): return None
                return f"{td.seconds // 60:02}:{td.seconds % 60:02}.{td.microseconds // 1000:03}"

            results = session.results[[
                'FullName', 'HeadshotUrl', 'Position',
                'TeamName', 'ClassifiedPosition', 'Q1', 'Q2', 'Q3'
            ]].copy()

            for col in ['Q1', 'Q2', 'Q3']:
                results[col] = results[col].apply(format_laptime)

            return {
                'key': f"{parameters['year']}_{parameters['round']}",
                'eventName': event['EventName'],
                'results': [{
                    'fullName': row['FullName'],
                    'teamName': row['TeamName'],
                    'headshotUrl': row['HeadshotUrl'],
                    'position': int(row['Position']),
                    'q1': row['Q1'],
                    'q2': row['Q2'],
                    'q3': row['Q3']
                } for _, row in results.iterrows()]
            }

        except IndexError:
            raise ValueError(f"No event found for {parameters['year']} Round {parameters['round']}")
        except Exception as e:
            raise RuntimeError(f"Quali data extraction failed: {str(e)}")

    @task
    def load_to_mongodb(data: dict):
        try:
            MongoHook(conn_id=MONGO_CONN_ID).get_conn() \
                .pitlap.quali_results.update_one(
                    {"key": data["key"]},
                    {"$set": data},
                    upsert=True
                )
        except Exception as e:
            raise ConnectionError(f"MongoDB update failed: {str(e)}")

    quali_data = extract_quali_results(validate_params())
    load_to_mongodb(quali_data)
