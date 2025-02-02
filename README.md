# PitLap ETL Pipeline Project

## Overview
This project is an **ETL (Extract, Transform, Load) pipeline** built using **Apache Airflow** to fetch, process, and store data from various sources into a **PostgreSQL database**.

## Features
- Extracts data from **various APIs or databases**.
- Transforms data by cleaning and formatting relevant fields.
- Loads the processed data into a **PostgreSQL database**.
- Scheduled execution using Airflow.

## Technologies Used
- **Apache Airflow** (DAG scheduling & task orchestration)
- **Pandas** (Data transformation & processing)
- **PostgreSQL** (Data storage)

## Project Structure
```
.
├── dags/
│   ├── etl_pipeline.py                # Airflow DAG for ETL
├── requirements.txt                    # Project dependencies
├── README.md                           # Project documentation
```

## Installation & Setup
### Prerequisites
Ensure you have the following installed:
- Python 3.8+
- Apache Airflow
- PostgreSQL (local or cloud instance)

### Install Dependencies
```bash
pip install -r requirements.txt
```

### Configure Airflow
Set up Airflow if not already installed:
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
```

### Configure PostgreSQL Connection
Update the **Airflow PostgreSQL connection** in the **Airflow UI**:
- Go to `Admin` > `Connections`
- Add a connection with:
  - Conn ID: `postgres_default`
  - Conn Type: `Postgres`
  - Host, Schema, Login, Password as per your setup

### Run Airflow Scheduler & Webserver
```bash
airflow scheduler &
airflow webserver --port 8080
```

## Running the DAG
1. Start the Airflow webserver and scheduler.
2. Go to `http://localhost:8080`.
3. Enable the `etl_pipeline` DAG.
4. Trigger the DAG manually or wait for the scheduled run.

## Database Schema
| Column        | Type      | Description                 |
|--------------|----------|-----------------------------|
| field1       | TYPE     | Description of field1      |
| field2       | TYPE     | Description of field2      |
| field3       | TYPE     | Description of field3      |
| timestamp    | TIMESTAMP | Record insert timestamp   |

## Future Enhancements
- Add support for **multiple data sources**.
- Implement **data validation & error handling**.
- Create a **dashboard** to visualize processed data.
- Store historical data for trend analysis.

## License
This project is licensed under the **MIT License**.

## Contributors
- [Tinashe](https://github.com/rabbinash)

