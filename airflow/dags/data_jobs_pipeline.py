from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
import sys

# Make sure /opt/airflow project root on PYTHONPATH

PROJECT_ROOT = "/opt/airflow"
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.data_pipeline import main as etl_main


def run_etl():
    # keep your env vars for DB
    os.environ.setdefault("POSTGRES_HOST", "postgres")
    os.environ.setdefault("POSTGRES_DB", "data_jobs_db")
    os.environ.setdefault("POSTGRES_USER", "ldj_user")
    os.environ.setdefault("POSTGRES_PASSWORD", "Sul123")
    os.environ.setdefault("POSTGRES_PORT", "5432")

    etl_main()


with DAG(
    dag_id="data_jobs_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    template_searchpath=["/opt/airflow/schema"],
) as dag:

    apply_schema = PostgresOperator(
        task_id="apply_schema",
        postgres_conn_id="postgres_default",
        sql="schema.sql",
    )

    run_etl_task = PythonOperator(
        task_id="run_etl",
        python_callable=run_etl,
    )

    apply_schema >> run_etl_task
