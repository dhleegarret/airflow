from airflow.sdk import DAG, chain
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_task_decorator_origin",
    schedule="0 2 * * 1",   #매주 월요일 02:00 시작
    start_date=pendulum.datetime(2023, 3, 1, tz="Asia/Seoul"),
    catchup=False
) as dag:
    def print_context(some_input):
        print(some_input)

    python_task_1 = PythonOperator(
        task_id = 'python_task_1',
        python_callable=print_context('task_decorator 실행')
    )