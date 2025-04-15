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

CACHE_DIR = '/opt/airflow/cache/f1'
MONGO_CONN_ID = 'mongo_default'

with DAG(
    dag_id='f1_practice_laps_etl_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    params={
        "year": 2025,
        "round": 4,
        "session_name": "Practice 3"
    }
) as dag:


    @task()
    def validate_params(**context):
        year = context['dag_run'].conf.get('year')
        round_ = context['dag_run'].conf.get('round')
        session_name = context['dag_run'].conf.get('session_name', 'Practice 3')

        if not year or not round_:
            raise ValueError(f"Missing required parameters: {'year' if not year else ''} {'round' if not round_ else ''}")

        return year, round_, session_name

    @task()
    def extract_practice_laps(year, round_, session_name):
        try:
            fastf1.Cache.enable_cache(CACHE_DIR)            
            session = fastf1.get_session(year, round_, session_name)
            session.load()

            def format_timedelta(td):
                if pd.isna(td): return None
                total_seconds = td.total_seconds()
                minutes = int(total_seconds // 60)
                seconds = int(total_seconds % 60)
                milliseconds = int((total_seconds - int(total_seconds)) * 1000)
                return f"{minutes:02}:{seconds:02}.{milliseconds:03}"

            laps = session.laps[['Driver', 'LapTime', 'Compound', 'IsPersonalBest', 'LapNumber']]
            laps['lapTimeStr'] = laps['LapTime'].apply(format_timedelta)
            laps = laps.dropna(subset=['lapTimeStr'])

            lap_data = []
            for _, row in laps.iterrows():
                driver_info = session.get_driver(row['Driver'])
                lap_data.append({
                    "driver": row['Driver'],
                    "headshotUrl": driver_info.get('HeadshotUrl'),
                    "compound": row['Compound'],
                    "lapTime": row['lapTimeStr'],
                    "lapNumber": row['LapNumber'],
                    "isPersonalBest": row['IsPersonalBest'],
                    "fullName": driver_info.get('FullName'),
                })

            return {
                "year": year,
                "round": round_,
                "sessionName": session_name,
                "eventFormat": session.event['EventFormat'],
                "laps": lap_data
            }

        except Exception as e:
            raise RuntimeError(f"Failed to load session: {str(e)}")

    @task()
    def load_to_mongodb(data: dict):
        mongo_hook = MongoHook(conn_id=MONGO_CONN_ID)
        client = mongo_hook.get_conn()
        db = client['pitlap']
        collection = db['practice_laps']

        # Upsert based on year + round + session
        query = {
            "year": data['year'],
            "round": data['round'],
            "sessionName": data['sessionName']
        }
        collection.update_one(query, {"$set": data}, upsert=True)

    year, round_, session_name = validate_params()

    lap_data = extract_practice_laps(year, round_, session_name)
    load_to_mongodb(lap_data)
