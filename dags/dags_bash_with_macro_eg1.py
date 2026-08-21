from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 L * *",              # 매월 마지막날짜 00시10분 수행 
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), 
    catchup=False,  
) as dag:
    # START_DATE : 전월 말일, END_DATE: 1일 전
    bash_task1 = BashOperator(
        task_id='bash_task1',
        env={'START_DATE': '{{}}',
             'END_DATE': '{{}}'
        },                                 #BashOperator: Jinja 템플릿을 env에 입력함
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )