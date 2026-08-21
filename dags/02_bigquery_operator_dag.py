import pendulum
import pandas as pd
from airflow.sdk import DAG, task
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator #

# 빅쿼리 계층 구조
# Project -> Dataset -> Table
# BigQueryCreateEmptyDatasetOperator: 상단 'Dataset'을 생성하는 오퍼레이터

default_args = dict(
    owner = 'donghee',
    email = ['dhleegarret@gmail.com'],
    email_on_failure = False,
    retries = 3
)

with DAG(
    dag_id="02_bigquery_operator_dag",
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule="30 10 * * *",
    tags = [''],
    default_args = default_args,
    catchup=False
):
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        gcp_conn_id='gcp_conn',         # Airflow > 관리자 메뉴 > 커넥션 에 생성한 GCP 커넥션 ID
        dataset_id='airflow_test',      # 데이터셋 ID 지정
        project_id='My Project 20860',
        location='asia-northeast3',
        if_exists='ignore'              # 동일한 명칭의 데이터셋이 있는 경우에 무시함
    )

create_dataset