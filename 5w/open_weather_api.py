from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook

from datetime import datetime
from datetime import timedelta
import time
import requests
import logging
import psycopg2
import pprint


def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id='redshift_dev_db') # connections에 미리 등록해놓음

    return hook.get_conn().cursor()


def extract(**context):
    base_url = context["params"]["base_url"]
    lat = context["params"]["lat"]
    lon = context["params"]["lon"]
    part = context["params"]["part"]
    api_key = context["params"]["api_key"]

    task_instance = context['task_instance']
    execution_date = context['execution_date']

    logging.info(execution_date)

    link = base_url.format(lat=lat, lon=lon, part=part, api_key=api_key)

    res = requests.get(link)
    result = res.json()

    return result


def transform(**context):
    lines = []
    data = context["task_instance"].xcom_pull(key="return_value", task_ids="extract")

    for day in data["daily"]:
        # pprint(day) # 하루 데이터
        # print(time.strftime("%Y/%m/%d", time.localtime(day['dt'])))  # 날짜
        # pprint(day['temp']['day'])  # 낮 온도
        # pprint(day['temp']['min'])  # 최저 온도
        # pprint(day['temp']['max'])  # 최고 온도

        date = str(time.strftime("%Y/%m/%d", time.localtime(day['dt'])))
        temp = str(day['temp']['day'])
        min_temp = str(day['temp']['min'])
        max_temp = str(day['temp']['max'])

        lines.append(','.join([date, temp, min_temp, max_temp]))

    return lines


def load(**context):
    schema = context["params"]["owa_schema"]
    table = context["params"]["owa_table"]
    ddl = context["params"]["owa_ddl"]

    cur = get_Redshift_connection()
    lines = context["task_instance"].xcom_pull(key="return_value", task_ids="transform")
    lines = iter(lines)
    next(lines)
    sql = "BEGIN; DELETE FROM {schema}.{table}; {ddl};".format(schema=schema, table=table, ddl=ddl)
    for line in lines:
        if line != "":
            (date, temp, min_temp, max_temp) = line.split(",")
            logging.info(f"{date} - {temp} - {min_temp} - {max_temp}")
            sql += f"""INSERT INTO {schema}.{table} VALUES ('{date}', '{temp}', '{min_temp}' , '{max_temp}', default);"""
    sql += "END;"
    logging.info(sql)
    cur.execute(sql)


dag_open_weather_api = DAG(
    dag_id='dag_open_weather_api',
    start_date=datetime(2021, 12, 1),  # 날짜가 미래인 경우 실행이 안됨
    schedule_interval='1 * * * *',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
)

extract = PythonOperator(
    task_id='extract',
    python_callable=extract,
    params={
        'base_url': Variable.get("base_url"),
        'lat': Variable.get("lat"),
        'lon': Variable.get("lon"),
        'part': Variable.get("part"),
        'api_key': Variable.get("api_key")
    },
    provide_context=True,
    dag=dag_open_weather_api)

transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    params={
    },
    provide_context=True,
    dag=dag_open_weather_api)

load = PythonOperator(
    task_id='load',
    python_callable=load,
    params={
        'owa_schema': Variable.get("owa_schema"),
        'owa_table': Variable.get("owa_table"),
        'owa_ddl' : Variable.get("owa_ddl")
    },
    provide_context=True,
    dag=dag_open_weather_api)

extract >> transform >> load


"""
<Connections>
Conn Id : redshift_dev_db
Conn Type : Amazon Redshift
Host : learnde ~~ redshift.amazonaws.com
Port : 5439

<Variables>
(key, val)
api_key, ********(open_weather_api key)
base_url, https://api.openweathermap.org/data/2.5/onecall?lat={lat}&lon={lon}&exclude={part}&appid={api_key}&units=metric
lat, 37.5683
lon, 126.9778
owa_ddl, CREATE TABLE IF NOT EXISTS seonmin1219.weather_forecast ( date date primary key, temp float, min_temp float, max_temp float, created_at timestamp default sysdate );
owa_schema, seonmin1219
owa_table, weather_forecast
part, current,minutely,hourly,alerts

"""