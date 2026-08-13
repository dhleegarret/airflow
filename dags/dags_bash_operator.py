from airflow.sdk import DAG, chain
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_operator", #일반적으로 해당 파이썬 파일명과 일치시켜 줌
    schedule="0 0 * * *",       # cron 스케쥴 (분 시 일 월 요일) -> 현재는 매일 00시00분에 실행하는 DAG
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), # DAG이 2021년 1월 1일부터 돌아감 (tz=Asia/Seoul로 세팅)
    catchup=False,  # False: start_date 이후부터 오늘 날짜까지 누락된 구간을 돌리지 않음 (True: 누락된 구간을 돌림, 단 Task들이 한꺼번에 돌기 때문에 DAG 설계 구조에 따라 문제가 발생할 수 있음)
    #dagrun_timeout=datetime.timedelta(minutes=60), #DAG 실행 타임아웃 세팅: 기본 60분
    #tags=["example", "example2"],   # 각 DAG의 태그값 설정 
    #params={"example_key": "example_value"}, #Task들에 공통적으로 넘겨줄 파라미터
) as dag:
    # Task 정의: BashOperator
    bash_t1 = BashOperator(
        task_id="bash_t1",   #DAG의 '그래프 보기' 메뉴를 통해 확인 가능한 Task 이름 (그러나 오퍼레이터 객체명과 동일하게 세팅)
        bash_command="echo whoami", #수행할 쉘스크립트
    )

    bash_t2 = BashOperator(
            task_id="bash_t2",   
            bash_command="echo $HOSTNAME", 
        )

    bash_t1 >> bash_t2 #Task 수행 절차: bash_t1 이후에 bash_t2 실행
    