from airflow.sdk import DAG, chain
import datetime
import pendulum
from airflow.providers.standard.operators.bash import BashOperator

with DAG(
    dag_id="dags_bash_select_fruit",
    schedule="10 0 * * 6#1",            #매월 첫 번째주 토요일 00시10분 수행
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:

    t1_orange = BashOperator(
            task_id="t1_orange",   
            bash_command="/opt/airflow/plugins/shell/select_fruit.sh ORANGE",   #/opt/airflow/plugins: 도커 worker 컨테이너 플러그인 경로 (docker-compose.yaml 파일에서 정의함)
        )

    t1_avocado = BashOperator(
            task_id="t1_avocado",   
            bash_command="/opt/airflow/plugins/shell/select_fruit.sh AVOCADO",   
        )

    t1_orange >> t1_avocado