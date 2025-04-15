from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago
from airflow.providers.mongo.hooks.mongo import MongoHook
import fastf1
import pandas as pd

CACHE_DIR = '/opt/airflow/cache/f1'

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

MONGO_CONN_ID = 'mongo_default'

with DAG(
    dag_id='f1_quali_results_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,  # Run manually
    catchup=False,
    params={
        "year": 2025,
        "round": 4,
    }
) as dag:


    @task()
    def validate_params(**context):
        year = context['dag_run'].conf.get('year')
        round_ = context['dag_run'].conf.get('round')

        if not year or not round_:
            raise ValueError(f"Missing required parameters: {'year' if not year else ''} {'round' if not round_ else ''}")

        return year, round_

    @task()
    def extract_quali_results(year, round_):
        try:
            fastf1.Cache.enable_cache(CACHE_DIR)

            schedule = fastf1.get_event_schedule(year)
            event = schedule.loc[schedule['RoundNumber'] == round_].iloc[0]

            session = fastf1.get_session(year, round_, 'Q')
            session.load()

            def format_timedelta(td):
                if pd.isna(td): return None
                total_seconds = td.total_seconds()
                minutes = int(total_seconds // 60)
                seconds = int(total_seconds % 60)
                milliseconds = int((total_seconds - int(total_seconds)) * 1000)
                return f"{minutes:02}:{seconds:02}.{milliseconds:03}"

            results = session.results[['FullName', 'HeadshotUrl', 'Position', 'TeamName', 'ClassifiedPosition', 'Q1', 'Q2', 'Q3']]

            for col in ['Q1', 'Q2', 'Q3']:
                results[col] = results[col].apply(format_timedelta)

            event_data = {
                'key': f"{year}_{round_}",
                'eventName': event['EventName'],
                'results': [
                    {
                        'teamName': row['TeamName'],
                        'headshotUrl': row['HeadshotUrl'],
                        'q1': row['Q1'],
                        'q2': row['Q2'],
                        'q3': row['Q3'],
                        'position': int(row['Position']),
                        'fullName': row['FullName']
                    }
                    for _, row in results.iterrows()
                ]
            }

            return event_data

        except Exception as e:
            raise RuntimeError(f"Failed to load qualifying results for year {year}, round {round_}: {str(e)}")

    @task()
    def load_to_mongodb(data: dict):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['quali_results']

        query = { "key": data["key"] }
        collection.update_one(query, {"$set": data}, upsert=True)

    year, round_ = validate_params()

    quali_data = extract_quali_results(year, round_)
    load_to_mongodb(quali_data)
