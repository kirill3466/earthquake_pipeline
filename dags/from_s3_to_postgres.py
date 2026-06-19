import logging

import duckdb
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

OWNER = "airflow"
DAG_ID = "from_s3_to_postgres"

LAYER = "raw"
SOURCE = "earthquake"

LONG_DESCRIPTION = """
Сырые данные из S3 в Postgres
"""

SHORT_DESCRIPTION = "Сырые данные из S3 в Postgres"

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


def get_and_transfer_s3_data_to_postgres(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Загрузка данных по датам с/по: {start_date}/{end_date}")

    conn = BaseHook.get_connection("minio_s3")
    access_key = conn.login
    secret_key = conn.password

    pg_conn = BaseHook.get_connection("postgres")

    # обязательно без переносов
    pg_str = (
        f"host={pg_conn.host} port={pg_conn.port} dbname={pg_conn.schema} "
        f"user={pg_conn.login} password={pg_conn.password}"
    )

    con = duckdb.connect()

    con.execute(f"ATTACH '{pg_str}' AS pg (TYPE POSTGRES)")

    con.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{LAYER}")
    con.execute(
        f"""
        CREATE TABLE IF NOT EXISTS pg.{LAYER}.{SOURCE} (
            time            TIMESTAMP,
            latitude        DOUBLE,
            longitude       DOUBLE,
            depth           DOUBLE,
            mag             DOUBLE,
            magType         VARCHAR,
            nst             INTEGER,
            gap             DOUBLE,
            dmin            DOUBLE,
            rms             DOUBLE,
            net             VARCHAR,
            id              VARCHAR,
            updated         TIMESTAMP,
            place           VARCHAR,
            type            VARCHAR,
            horizontalError DOUBLE,
            depthError      DOUBLE,
            magError        DOUBLE,
            magNst          INTEGER,
            status          VARCHAR,
            locationSource  VARCHAR,
            magSource       VARCHAR
        )
        """
    )

    con.execute(
        f"DELETE FROM pg.{LAYER}.{SOURCE} "
        f"WHERE time >= '{start_date}'::timestamp AND time <= '{end_date}'::timestamp"
    )

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

        INSERT INTO pg.{LAYER}.{SOURCE}
        SELECT * FROM read_parquet('s3://prod/{LAYER}/{SOURCE}/{start_date}/*.gz.parquet');
        """
    )
    
    logging.info(f"Первые 5 строк: {con.execute(f'SELECT * FROM pg.{LAYER}.{SOURCE} LIMIT 5').fetchall()}")
    con.close()
    logging.info(f"✅ Данные загрузились с/по: {start_date}/{end_date}")


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

    get_and_transfer_s3_data_to_postgres = PythonOperator(
        task_id="get_and_transfer_s3_data_to_postgres",
        python_callable=get_and_transfer_s3_data_to_postgres,
    )
