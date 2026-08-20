from airflow.sdk import DAG, task
import datetime
import pendulum
from airflow.providers.standard.operators.python import PythonOperator
from common.common_func import regist2   # common 하위 regist2 함수 사용

with DAG(
    dag_id="dags_python_with_op_kwargs",
    schedule="30 6 * * *", 
    start_date=pendulum.datetime(2023, 1, 1, tz="Asia/Seoul"),
    catchup=False,
) as dag:
    
    regist2_t1 = PythonOperator(
        task_id = 'regist2_t1',
        python_callable=regist2,
        op_args=['dhlee', 'man', 'kr', 'seoul'],
        op_kwargs={'email':'dhleeegarret@gmail.com', 'phone':'010'}
    )

    regist2_t1