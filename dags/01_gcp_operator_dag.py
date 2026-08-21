import pendulum
import pandas as pd
from airflow.models import Variable
from airflow.sdk import DAG, task
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator # 특정 버킷에 어떤 객체들이 저장되어있는지 리스트업

default_args = dict(
    owner = 'donghee',
    email = ['dhleegarret@gmail.com'],
    email_on_failure = False,
    retries = 3
)

with DAG(
    dag_id="01_gcp_operator_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = [''],
    default_args = default_args,
    catchup=False
):
    list_objects = GCSListObjectsOperator(
        task_id='list_objects',
        gcp_conn_id='gcp_conn',         # Airflow > 관리자 메뉴 > 커넥션 에 생성한 GCP 커넥션 ID
        bucket="airflow_test_bucket_dhlee",
        prefix="source"     # 버킷 하위 경로 폴더명
    )

list_objects