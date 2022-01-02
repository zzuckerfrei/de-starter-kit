from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.operators import PythonOperator
from airflow.models import Variable
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime
from datetime import timedelta

from airflow import AirflowException

import requests
import logging
import psycopg2

from airflow.exceptions import AirflowException

def get_Redshift_connection():
    hook = PostgresHook(postgres_conn_id = 'redshift_dev_db')
    return hook.get_conn().cursor()


def execSQL(**context):

    schema = context['params']['schema']
    table = context['params']['table']
    src_table = context['params']['src_table']
    select_sql = context['params']['sql']

    logging.info(schema)
    logging.info(table)
    logging.info(select_sql)

    cur = get_Redshift_connection()

    sql = """
               DROP TABLE IF EXISTS {schema}.temp_{src_table};
             CREATE TABLE {schema}.temp_{src_table} AS        
          """.format(schema=schema, table=table, src_table=src_table)
    sql += select_sql
    cur.execute(sql)

    cur.execute("SELECT COUNT(1) FROM {schema}.temp_{src_table}""".format(schema=schema, src_table=src_table))
    count = cur.fetchone()[0]
    if count == 0:
        raise ValueError("{schema}.{src_table} didn't have any record".format(schema=schema, src_table=src_table))

    try:
        sql = """
                  DROP TABLE IF EXISTS {schema}.{table};
                  CREATE TABLE {schema}.{table} (
                        POSITIVE REAL,
                        NEGATIVE REAL,
                        NPS INT
                  );
        INSERT INTO {schema}.{table} 
        SELECT (
                 (SELECT count(*)
                    FROM {schema}.TEMP_{src_table}
                   WHERE score >= 9
                 ) / 
                 (SELECT count(*)
                    FROM {schema}.TEMP_{src_table}
                 ) :: REAL * 100
                ) AS POSITIVE
               ,
               (
                 (SELECT count(*)
                    FROM {schema}.TEMP_{src_table}
                   WHERE score <= 6 
                 ) / 
                 (SELECT count(*)
                    FROM {schema}.TEMP_{src_table}
                 ) :: REAL * 100
               )  AS NEGATIVE
               ,
               POSITIVE - NEGATIVE AS NPS
        ;
        """.format(schema=schema, table=table, src_table=src_table)
        sql += "COMMIT;"
        cur.execute(sql)
        logging.info(sql)
        logging.info("success")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise AirflowException("")


dag = DAG(
    dag_id = "Build_NPS_Summary",
    start_date = datetime(2021,12,30),
    schedule_interval = '@once',
    catchup = False
)

execsql = PythonOperator(
    task_id = 'execsql',
    python_callable = execSQL,
    params = {
        'schema' : 'seonmin1219',
        'src_table' : 'nps',
        'table': 'nps_summary',

        'sql' : """
                SELECT *
                  FROM seonmin1219.nps
                """
    },
    provide_context = True,
    dag = dag
)