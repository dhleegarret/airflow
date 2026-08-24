import pendulum
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator #빅쿼리에 쿼리 작업을 던지는 오퍼레이터

default_args = dict(
    owner = 'donghee',
    email = ['dhleegarret@gmail.com'],
    email_on_failure = False,
    retries = 3
)

with DAG(
    dag_id="03_bigquery_job_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = [''],
    default_args = default_args,
    catchup=False
):

    SQL_QUERY = """
    CREATE TABLE airflow_test.grass_pokemon AS
    SELECT * FROM bq_data.pokemon WHERE `Type 1`='Grass'
    """
    bq_job = BigQueryInsertJobOperator(
        task_id='bq_job',
        gcp_conn_id='gcp_conn',
        project_id='arched-vigil-506205-t1',
        location='asia-northeast3',
        configuration={
            "query": {
                "query": SQL_QUERY,
                "useLegacySql": False,
                "priority": "BATCH",
            }
        }
    )

    bq_job