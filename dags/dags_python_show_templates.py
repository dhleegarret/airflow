from airflow.sdk import DAG, task
import datetime
import pendulum

with DAG(
    dag_id="dags_python_show_templates",
    schedule="30 9 * * *",  # 매일 
    start_date=pendulum.datetime(2026, 8, 1, tz="Asia/Seoul"), #시작 날짜: 2026-08-01
    catchup=True,   #catchup=True: 2026-08-01 ~ 오늘 날짜(2026-08-21) 사이 구간을 모두 수행하겠다.
) as dag:
    
    @task(task_id='python_task')
    def show_templates(**kwargs):
        from pprint import pprint   # pprint: print 결과를 가시적으로 보여주는 구문 
        pprint(kwargs)              # Jinja template에서 제공하는 파라미터 확인 가능

    show_templates()