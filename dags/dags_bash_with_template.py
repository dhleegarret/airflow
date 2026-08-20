from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_with_template",
    schedule="10 0 * * *", 
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    bash_t1 = BashOperator(
        task_id = 'bash_t1',
        bash_command = 'echo "data_interval_end: {{data_interval_end}}"'
    )

    bash_t2 = BashOperator(
        task_id = 'bash_t2',
        env = {
            'START_DATE': '{{data_interval_start | ds}}',   # ds: timestamp 형식의 data_interval_start를 'YYYY-MM-DD' 형태로 변환
            'END_DATE': '{{data_interval_end | ds}}'
        },
        bash_command = 'echo $START_DATE && echo $END_DATE' #&& : 앞에있는 조건이 성공하면, 뒤에있는 조건을 수행하겠다.
    )

    bash_t1 >> bash_t2
    