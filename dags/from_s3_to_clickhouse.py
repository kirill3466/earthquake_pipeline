import logging

import clickhouse_connect
import duckdb
import pendulum
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.python import PythonOperator

OWNER = "airflow"
DAG_ID = "from_s3_to_clickhouse"

LAYER = "raw"
SOURCE = "earthquake"

LONG_DESCRIPTION = """
Сырые данные из S3 в ClickHouse
"""

SHORT_DESCRIPTION = "Сырые данные из S3 в ClickHouse"

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


def get_and_transfer_s3_data_to_clickhouse(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Загрузка данных по датам с/по: {start_date}/{end_date}")

    s3_conn = BaseHook.get_connection("minio_s3")
    ch_conn = BaseHook.get_connection("clickhouse")

    con = duckdb.connect()
    con.execute(
        f"""
        SET TIMEZONE='UTC';
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_url_style = 'path';
        SET s3_endpoint = 'minio:9000';
        SET s3_access_key_id = '{s3_conn.login}';
        SET s3_secret_access_key = '{s3_conn.password}';
        SET s3_use_ssl = FALSE;
        """
    )

    rows = con.execute(
        f"""
        SELECT
            time::TIMESTAMP AS time,
            latitude::DOUBLE AS latitude,
            longitude::DOUBLE AS longitude,
            depth::DOUBLE AS depth,
            mag::DOUBLE AS mag,
            magType::VARCHAR AS magType,
            nst::INTEGER AS nst,
            gap::DOUBLE AS gap,
            dmin::DOUBLE AS dmin,
            rms::DOUBLE AS rms,
            net::VARCHAR AS net,
            id::VARCHAR AS id,
            updated::TIMESTAMP AS updated,
            place::VARCHAR AS place,
            type::VARCHAR AS type,
            horizontalError::DOUBLE AS horizontalError,
            depthError::DOUBLE AS depthError,
            magError::DOUBLE AS magError,
            magNst::INTEGER AS magNst,
            status::VARCHAR AS status,
            locationSource::VARCHAR AS locationSource,
            magSource::VARCHAR AS magSource
        FROM read_parquet(
            's3://prod/{LAYER}/{SOURCE}/{start_date}/*.gz.parquet'
        )
        """
    ).df()
    con.close()

    client = clickhouse_connect.get_client(
        host=ch_conn.host,
        port=ch_conn.port or 8123,
        username=ch_conn.login,
        password=ch_conn.password or "",
        database=ch_conn.schema or LAYER,
    )

    client.command(
        f"ALTER TABLE {LAYER}.{SOURCE} DELETE "
        f"WHERE time >= toDateTime64('{start_date} 00:00:00', 3, 'UTC') "
        f"AND time <= toDateTime64('{end_date} 23:59:59', 3, 'UTC')"
    )

    if not rows.empty:
        client.insert_df(f"{LAYER}.{SOURCE}", rows)

    sample = client.query(f"SELECT * FROM {LAYER}.{SOURCE} LIMIT 5").result_rows
    logging.info(f"Первые 5 строк: {sample}")
    logging.info(f"✅ Данные загрузились с/по: {start_date}/{end_date}")


with DAG(
    dag_id=DAG_ID,
    schedule_interval="0 5 * * *",
    default_args=args,
    tags=["s3", "raw", "clickhouse"],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1,
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    get_and_transfer_s3_data_to_clickhouse = PythonOperator(
        task_id="get_and_transfer_s3_data_to_clickhouse",
        python_callable=get_and_transfer_s3_data_to_clickhouse,
    )
