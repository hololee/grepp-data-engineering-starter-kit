from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
import requests
from datetime import datetime, timedelta

# dag 객체 생성.
dag_fullrefresh_gender = DAG(
    dag_id='week4-assignment',
    catchup=False,
    start_date=datetime(2022, 9, 3),
    schedule_interval='0 2 * * *',  # 매일 2시에 동작.
    max_active_runs=1,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    },
    tags=["assignment"],
)

# redshift 접속.
def get_Redshift_connection(autocommit):
    # connection 정보는 airflow admin-connections에서 지정할 수 있다.
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


# E: 파일을 추출 과정.
def extract(**context):
    link = context['params']['url']
    task_instance = context['task_instance']
    execution_date = context['execution_date']

    f = requests.get(link)
    return f.text


def transform(**context):
    text = context['task_instance'].xcom_pull(key='return_value', task_ids='extract')
    lines = text.split('\n')[1:]
    return lines


def load(**context):
    schema = context['params']['schema']
    table = context['params']['table']

    lines = context['task_instance'].xcom_pull(key='return_value', task_ids='transform')

    cur = get_Redshift_connection(autocommit=True)
    # transaction으로 묶기.
    sql = f"BEGIN;DELETE FROM {schema}.{table};"
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql += f"INSERT INTO {schema}.{table} VALUES ('{name}', '{gender}');"
            print(sql)
    sql += "END;"
    cur.execute(sql)


'''Variables masking: https://airflow.apache.org/docs/apache-airflow/stable/security/secrets/mask-sensitive-values.html?highlight=api_key
    It will also mask the value of a Variable, or the field of a Connection’s extra 
    JSON blob if the name contains any words in (‘access_token’, ‘api_key’, ‘apikey’,
    ’authorization’, ‘passphrase’, ‘passwd’, ‘password’, ‘private_key’, ‘secret’,
    ‘token’). This list can also be extended:
'''
extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    # airflow admin-Variables 에서 추가해둘 수 있음.
    params={'url': Variable.get('csv_secret')},
    provide_context=True,
    dag=dag_fullrefresh_gender,
)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag_fullrefresh_gender,
)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={'schema': 'zayden', 'table': 'name_gender'},
    provide_context=True,
    dag=dag_fullrefresh_gender,
)

extract >> transform >> load
