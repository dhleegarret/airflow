import pendulum
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

default_args = dict(
    owner = 'donghee',
    email = ['dhleegarret@gmail.com'],
    email_on_failure = False,
    retries = 3
)

with DAG(
    dag_id="03-1_gcstobugquery_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = [''],
    default_args = default_args,
    catchup=False
):
    load_csv_to_bq = GCSToBigQueryOperator(
        task_id='load_csv_to_bq',
        gcp_conn_id='gcp_conn',
        bucket='airflow_test_bucket_dhlee',
        source_objects=['source/pokemon.csv'],
        destination_project_dataset_table='arched-vigil-506205-t1.bq_data.pokemon',
        write_disposition='WRITE_TRUNCATE',
        source_format='CSV',
        skip_leading_rows=1,
        autodetect=True,
    )

    load_csv_to_bq