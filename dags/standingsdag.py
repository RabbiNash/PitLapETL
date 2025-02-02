from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from fastf1.ergast import Ergast
import pandas as pd

POSTGRES_CONN_ID='postgres_default'
YEAR = 2024

default_args={
    'owner':'airflow',
    'start_date':days_ago(1)
}

## DAG
with DAG(dag_id='f1_standings_etl_pipeline',
         default_args=default_args,
         schedule_interval='@weekly',
         catchup=False) as dags:

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
        current_standings = pd.read_json(current_standings_json)  # Convert back to DataFrame
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS f1_standings (
            position INT NOT NULL,
            positionText TEXT,
            givenName TEXT,
            familyName TEXT,
            points FLOAT,
            wins INT DEFAULT 0,
            driverId TEXT,
            driverNumber INT NULL,
            constructorName TEXT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (driverNumber)
            );
        """)

        insert_query = """INSERT INTO f1_standings (position, positionText, givenName, familyName, points, wins, driverId, driverNumber, constructorName)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""

        standings = [
            (
                int(row["position"]),
                str(row["positionText"]),
                str(row["givenName"]),
                str(row["familyName"]),
                float(row["points"]),
                int(row["wins"]),
                str(row["driverId"]),
                int(row["driverNumber"]),
                str(row["constructorName"]) if pd.notna(row["constructorName"]) else None
            )
            for _, row in current_standings.iterrows()
        ]

        cursor.executemany(insert_query, standings)

        conn.commit()
        cursor.close()


    ## DAG Worflow- ETL Pipeline
    current_standings= extract_standings()
    load_standings(current_standings)
