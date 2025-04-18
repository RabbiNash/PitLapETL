from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

MONGO_CONN_ID = 'mongo_default'

def get_param(context, key):
    value = context['dag_run'].conf.get(key)
    if not value:
        raise ValueError(f"Missing required parameter: {key}")
    return value

with DAG(
    dag_id='f1_race_results_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags=['f1', 'etl'],
    params={
        "year": 2025,
        "round": 4,
    }
) as dag:

    @task
    def validate_params(**context):
        return {
            'year': int(get_param(context, 'year')),
            'round': int(get_param(context, 'round'))
        }

    @task
    def extract_race_results(parameters: dict, **context):
        try:
            schedule = fastf1.get_event_schedule(parameters['year'])
            event = schedule.loc[schedule['RoundNumber'] == parameters['round']].iloc[0]

            session = fastf1.get_session(parameters['year'], event['EventName'], 'R')
            session.load()

            return {
                'key': f"{parameters['year']}_{parameters['round']}",
                'eventName': event['EventName'],
                'eventFormat': event['EventFormat'],
                'results': [{
                    'teamName': row['TeamName'],
                    'headshotURL': row['HeadshotUrl'],
                    'position': int(row['Position']),
                    'fullName': row['FullName'],
                    'classifiedPosition': row['ClassifiedPosition'],
                    'points': int(row['Points']),
                    'gridPosition': int(row['GridPosition'])
                } for _, row in session.results.iterrows()]
            }

        except Exception as e:
            raise RuntimeError(f"Extraction failed: {str(e)}")

    @task
    def load_to_mongodb(data: dict):
        MongoHook(conn_id=MONGO_CONN_ID).get_conn() \
            .pitlap.race_results.update_one(
                {"key": data["key"]},
                {"$set": data},
                upsert=True
            )

    load_to_mongodb(extract_race_results(validate_params()))
