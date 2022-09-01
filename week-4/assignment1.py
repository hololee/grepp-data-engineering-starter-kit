"""ETL을 python을 통해서 구현합니다.
   구현한 ETL이 Full refresh작업이라고 가정하고,
   여러번 실행해도 값이 변하지 않도록 (이전 결과와 중복되지 않도록, Idempotent) 수정합니다.
"""
import psycopg2
import requests

# 로그인 정보 가져오기.
with open('./week-4/config', 'r') as credential:
    credential_info = credential.readline()
    id, pw, host, port, db, download_csv_link = credential_info.replace('\n', '').split(',')
    db_info = (host, port, db, id, pw)

# redshift 접속.
def get_Redshift_connection(host, port, dbname, redshift_user, redshift_pass):
    conn = psycopg2.connect(f'dbname={dbname} user={redshift_user} host={host} password={redshift_pass} port={port}')
    conn.set_session(autocommit=True)
    return conn.cursor()


# E: 파일을 추출 과정.
def extract(url):
    f = requests.get(url)
    return f.text


# T: 파일을 변환 하는 과정.
def transform(text):
    # 헤더는 버리기.
    lines = text.split("\n")[1:]
    return lines


# L: 파일을 적재하는 과정.
# TODO: 아래와 같이 짜는 경우 여러번 반복했을때 동일한 데이터가 들어가서 계속 쌓인다.
def load(db_info, lines):
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    # BEGIN;DELETE FROM (본인의스키마).name_gender;INSERT INTO TABLE VALUES ('KEEYONG', 'MALE');....;END;
    cur = get_Redshift_connection(*db_info)
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql = "INSERT INTO {id}.name_gender VALUES ('{n}', '{g}');".format(id=db_info[3], n=name, g=gender)
            print(sql)
            cur.execute(sql)


def load_fix(db_info, lines):

    # transaction으로 묶기.
    sql = f"BEGIN;DELETE FROM {db_info[3]}.name_gender;"
    cur = get_Redshift_connection(*db_info)
    for r in lines:
        if r != '':
            (name, gender) = r.split(",")
            print(name, "-", gender)
            sql += "INSERT INTO {id}.name_gender VALUES ('{n}', '{g}');".format(id=db_info[3], n=name, g=gender)
            print(sql)
    sql += "END;"
    cur.execute(sql)


def get(db_info):
    cur = get_Redshift_connection(*db_info)
    sql = f"SELECT COUNT(1) FROM {db_info[3]}.name_gender"
    cur.execute(sql)
    return cur.fetchall()


# ETL 동작.
data = extract(download_csv_link)
lines = transform(data)
# load(db_info, lines)
load_fix(db_info, lines)

# 결과 확인. ➔ 출력을 보면 실행할때마다 데이터가 중첩되서 쌓이는 것을 볼 수 있다.
print(get(db_info))
