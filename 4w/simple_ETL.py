import requests
import psycopg2


ddl = """
DROP TABLE IF EXISTS seonmin1219.name_gender;
CREATE TABLE seonmin1219.name_gender (
    name varchar(32),
    gender varchar(8)
);
"""

# Redshift connection 함수
def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "seonmin1219"
    redshift_pass = "password"
    port = 5439
    dbname = "dev"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(autocommit=True) # sql 실행 후 자동 commit
    return conn.cursor()


def extract(url):
    f = requests.get(url)

    return f.text

def transform(text):
    lines = text.split("\n")[1:]  # lines 데이터의 1번째 값이 header로 포함되므로 의도적으로 첫 번째 데이터를 삭제하고 진행함

    return lines

def load(lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection()

    # 여기 DELETE SQL 을 추가하면 멱등성 자체는 해결되는데.. 아래 FOR문 실행 도중 에러발생해서 멈추면??
    # 그래서 DELETE와 INSERT가 부분적으로 실행되면 안됨. 항상 정합성이 깨질 여지가 있는지 찾아봐야함
    # 트랜잭션을 걸어서 실행하면.. 문제 생겨도 다시 돌아갈 수 있음ROLLBACK,
    # 트랜잭션 : 다수의 쿼리를 묶어서 하나처럼 쓴다.
    # 오토커밋 : 트랜잭션

    sql = "BEGIN; DELETE FROM seonmin1219.name_gender;"
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql += "INSERT INTO seonmin1219.name_gender VALUES ('{n}', '{g}');".format(n=name, g=gender)
        sql += "END;"

        """
        BEGIN;                                                      -- 트랜잭션 시작
         
        DELETE FROM seonmin1219.name_gender;
        
        INSERT INTO seonmin1219.name_gender VALUES (Seonmin, M);
        INSERT INTO seonmin1219.name_gender VALUES (Max, M);
        INSERT INTO seonmin1219.name_gender VALUES (Santa, F);
        INSERT INTO seonmin1219.name_gender VALUES (Son, M);
        
        ...
        
        END;                                                        -- 트랜잭션 끝
        """

        cur.execute(sql)


def run_etl():
    link = "https://s3-geospatial.s3-us-west-2.amazonaws.com/name_gender.csv"
    data = extract(link)

    lines = transform(data)

    load(lines)


