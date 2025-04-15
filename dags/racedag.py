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
CACHE_DIR = '/opt/airflow/cache/f1'

with DAG(
    dag_id='f1_race_results_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
) as dag:

    @task()
    def validate_params(**context):
        year = context['dag_run'].conf.get('year')
        round_ = context['dag_run'].conf.get('round')

        if not year or not round_:
            raise ValueError(f"Missing required parameters: {'year' if not year else ''} {'round' if not round_ else ''}")

        return year, round_

    @task()
    def extract_race_results(year, round_, **context):
        try:
            fastf1.Cache.enable_cache(CACHE_DIR)

            schedule = fastf1.get_event_schedule(year)
            event = schedule.loc[schedule['RoundNumber'] == round_].iloc[0]

            event_data = {
                'key': f"{year}_{round_}",
                'eventName': event['EventName'],
                'eventFormat': event['EventFormat']
            }

            session = fastf1.get_session(year, event['EventName'], 'R')
            session.load()

            results = session.results.loc[:, ['FullName', 'HeadshotUrl', 'Position', 'TeamName', 'ClassifiedPosition', 'GridPosition', 'Points']]

            event_data['results'] = [
                {
                    'teamName': row['TeamName'],
                    'headshotURL': row['HeadshotUrl'],
                    'position': int(row['Position']),
                    'fullName': row['FullName'],
                    'classifiedPosition': row['ClassifiedPosition'],
                    'points': int(row['Points']),
                    'gridPosition': int(row['GridPosition'])
                }
                for _, row in results.iterrows()
            ]

            return event_data

        except Exception as e:
            raise RuntimeError(f"Unable to load race results for Round {round_} in {year}: {str(e)}")

    @task()
    def load_to_mongodb(data: dict):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['race_results']

        query = {"key": data["key"]}
        collection.update_one(query, {"$set": data}, upsert=True)

    year, round_ = validate_params()

    race_data = extract_race_results(year, round_)
    load_to_mongodb(race_data)
