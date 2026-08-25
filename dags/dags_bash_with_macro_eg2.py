from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_macro_eg1",
    schedule="10 0 * * 6#2",              # 매월 둘째주 토요일 00시10분 수행 
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"), 
    catchup=False,  
) as dag:
    # START_DATE : 2주전 월요일, END_DATE: 2주전 토요일
    bash_task2 = BashOperator(
        task_id='bash_task2',
        env={'START_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=19)) | ds}}',
             'END_DATE': '{{ (data_interval_end.in_timezone("Asia/Seoul") - macros.dateutil.relativedelta.relativedelta(days=14)) | ds}}'
        },                                 #BashOperator: Jinja 템플릿을 env에 입력함
        bash_command='echo "START_DATE: $START_DATE" && echo "END_DATE: $END_DATE"'
    )