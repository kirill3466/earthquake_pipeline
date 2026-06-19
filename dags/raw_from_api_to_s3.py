import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

# конфиг
OWNER = "airflow"
DAG_ID = "raw_from_api_to_s3"

# таблицы
LAYER = "raw"
SOURCE = "earthquake"

LONG_DESCRIPTION = """
Сырые данные из API в S3
"""

SHORT_DESCRIPTION = "Сырые данные из API в S3"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 5, 1, tz="Europe/Moscow"),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1),
}


def get_dates(**context) -> tuple[str, str]:
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date


def get_and_transfer_api_data_to_s3(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Загрузка данных по датам с/по: {start_date}/{end_date}")

    conn = BaseHook.get_connection("minio_s3")
    access_key = conn.login
    secret_key = conn.password

    con = duckdb.connect()

    con.sql(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{access_key}';
        SET s3_secret_access_key = '{secret_key}';
        SET s3_use_ssl = FALSE;

        COPY
        (
            SELECT
                *
            FROM
                read_csv_auto('https://earthquake.usgs.gov/fdsnws/event/1/query?format=csv&starttime={start_date}&endtime={end_date}') AS res
        )
            TO 's3://prod/{LAYER}/{SOURCE}/{start_date}/{start_date}_00-00-00.gz.parquet'
            (OVERWRITE TRUE)
        ;

        """,
    )

    con.close()
    logging.info(f"✅ Данные загрузились с даты: {start_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    get_and_transfer_api_data_to_s3 = PythonOperator(
        task_id="get_and_transfer_api_data_to_s3",
        python_callable=get_and_transfer_api_data_to_s3,
    )
