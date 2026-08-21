from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator

with DAG(
    dag_id="dags_python_template",
    schedule="30 9 * * *",
    start_date=pendulum.datetime(2023, 3, 10, tz="Asia/Seoul"), 
    catchup=False,  
) as dag:

    # 첫 번째 방법: op_kwargs
    def python_function1(start_date, end_date, **kwargs):
        print(start_date)
        print(end_date)

    python_t1 = PythonOperator(
        task_id='python_t1',
        python_callable=python_function1,
        op_kwargs={'start_date':'{{data_interval_start | ds}}', 'end_date': '{{data_interval_end | ds}}'}
    )

    # 두 번째 방법: 이미 정의되어있는 kwargs
    @task(task_id='python_t2')
    def python_function2(**kwargs):
        print(kwargs)
        print('ds:' + kwargs['ds'])
        print('ts:' + kwargs['ts'])
        print('data_interval_start:' + str(kwargs['data_interval_start']))
        print('data_interval_end:' + str(kwargs['data_interval_end']))
        print('task_instance:' + str(kwargs['ti']))

    python_t1 >>  python_function2()    #python_function2(): 데코레이터 함수명으로만 지정해도 실행 가능한 Task로 생성됨