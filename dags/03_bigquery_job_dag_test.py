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
    dag_id="03_bigquery_job_dag_test",
    start_date=pendulum.datetime(2026, 1, 1, tz='Asia/Seoul'),
    schedule="30 8 * * *",     # 매일 08:30 실행
    tags = [''],
    default_args = default_args,
    catchup=False
    # catchup: DAG가 활성화(UNPAUSED)될 때, 과거에 실행하지 못한 구간(interval)을 소급해서 실행할지 여부 결정
    # catchup=True  
    #  - 동작 방식: start_date가 과거날짜이고, DAG를 나중에 활성화하면, 스케줄러는 start_date부터 현재 시점까지 놓쳤던 모든 과거 실행 주기(Interval)을 계산하여 순차 또는 동시다발적으로 실행
    #  - 사용 목적: 과거 특정 시점부터 빠진 데이터(로그, 일별 집계 등)를 누락없이 채워 넣고 싶을 때 사용
    #  - 주의점: start_date가 몇 개월 전이라면, DAG가 켜지는 순간 수많은 과거 실행이 한꺼번에 돌면서 서버에 부하줄 수 있음
    # catchup=False
    #  - 동작 방식: start_date가 과거이거나 언제로 설정되든 상관없이, DAG가 활성화된 시점 이후에 도래하는 가장 첫 번째 스케줄 주기부터만 실행 (이전 누락된 구간은 무시)
    #  - 사용 목적: 실시간 모니터링 또는 최신 데이터 처리용 DAG여서 과거 데이터가 불필요할 때, 또는 과거 실행으로 인한 과부하를 방지하고 싶을 때 사용
    #  - 참고: Airflow 최신 버전에서는 별도 미설정할 경우, 기본값(Default)으로 False가 설정됨
):
    # CREATE OR REPLACE : 매일 중복 데이터가 쌓이지 않도록 TRUNCATE & INSERT 수행
    SQL_QUERY = """
    CREATE OR REPLACE TABLE airflow_test.grass_pokemon_test AS
    SELECT * FROM bq_data.pokemon WHERE `Type 1`='Fire'
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