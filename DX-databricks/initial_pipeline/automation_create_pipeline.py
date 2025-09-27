env = "dev"      #첨단 초기적재는 무조건 품질 서버에서 진행 하므로 dev로 설정할 것, 기초는 prd
catalog = "lam"  #lam(첨단) or lcc(기초)
system = "mes"   #시스템 : mes, wms, lims..
factory = "yeosu" #yeosu, lcjx, lchu..


#초기적재는 initial 증분적재는 incremental 아래에 저장
target_path = f"/Workspace/Shared/initial/{catalog}/bronze_{system}_{factory}"  # 노트북이 생성될 경로


import requests
import base64
import json

# Databricks workspace 정보
workspace_url = "https://dbc.cloud.databricks.com"  #워크스페이스
token = "dddddddddddddd" #개인 유저 토큰, 만료 730일


# API endpoint
url = f"{workspace_url}/api/2.0/workspace/import"
headers = {"Authorization": f"Bearer {token}"}

table_list = [row['tableName'] for row in spark.sql(f"show tables in {catalog}_{env}.bronze_{system}_{factory}").collect()]

#################################################################################################################

for table in table_list:
    # 노트북에 들어갈 초기 코드    
    code = f"""
import boto3
import json
from datetime import datetime, timedelta
import pytz
from pyspark.sql.functions import lit
import gc

#초기 적재는 품질로 옮겨서 진행, 해외 및 지방 공장 모두 (가산) 품질임
env = "{env}" 
catalog = "{catalog}"
system = "{system}"
factory = "{factory}"
table = "{table}"

target_catalog = "{catalog}_{env}"

#secret manager 에서 온프레미스 연결 접속 정보 가져오기
try:
    access_key = dbutils.secrets.get(f"{{env}}-aws-secrets", "access_key")
    secret_key = dbutils.secrets.get(f"{{env}}-aws-secrets", "secret_key")

    session = boto3.session.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="ap-northeast-2"
    )

    #Secrets Manager 클라이언트 생성
    client = session.client("secretsmanager")


    if catalog == "lam": #첨단은 공장 별로 품질 서버 아이피가 다름
        #Secret 값 가져오기
        secret_name = f"dev_{{catalog}}_{{system}}_{{factory}}"   
        response = client.get_secret_value(SecretId=secret_name)
    
    else:  #기초는 울산,대산,여수 하나의 품질 서버 아이피 사용
        #Secret 값 가져오기
        secret_name = f"dev_{{catalog}}_{{system}}_yeosu"
        response = client.get_secret_value(SecretId=secret_name)    

    secret_string = response['SecretString']
    secret_dict = json.loads(secret_string)

    #MSSQL 정보 추출
    ip = secret_dict['ip']
    user = secret_dict['user']
    password = secret_dict['password']
    port = secret_dict['port']
    database = secret_dict['db']
    schema = secret_dict['schema']
    
except Exception as err:
    raise err


try :
    # #초기 적재 이므로 스키마 정보 역시 무조건 브론즈에서 가져오게 하드코딩
    cols = spark.catalog.listColumns(f"{{target_catalog}}.bronze_{{system}}_{{factory}}.{{table}}")
    select_col = ", ".join([col.name for col in cols[:-1]])

    query = f"(select {{select_col}} from {{schema}}.{{table}} WITH(NOLOCK)) tmp" 

    # 증분 데이터 가져오기    
    df = (
            spark.read.format("jdbc")
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                .option("url", f"jdbc:sqlserver://{{ip}}:{{port}};databaseName={{database}}")
                .option("dbtable", query)
                .option("user", user)
                .option("password", password)
                .option("encrypt", "false")
                .load()
        )        

    load_dtm = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #UTC
    df = df.withColumn("etl_load_dtm", lit(load_dtm))

    #초기데이터ㅣ무조건 overwrite
    target_table = f"{{target_catalog}}.bronze_{{system}}_{{factory}}.{{table}}"
    df.write.mode("overwrite").saveAsTable(target_table)

    # 메모리에서 df 제거
    df.unpersist()
    del df
    gc.collect() #참조하고 있을 수 있는 값들 모두 삭제되도록..

except Exception as err:
    raise err
"""

    encoded_code = base64.b64encode(code.encode("utf-8")).decode("utf-8")
    notebook_path = f"{target_path}/task-{table}"

    payload = {
        "path": notebook_path,
        "language": "PYTHON",
        "format": "SOURCE",
        "overwrite": True,
        "content": encoded_code
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print(f" 생성 완료 : {notebook_path}")
    else:
        print(f"생성 실패함... {notebook_path}: {response.text}")



######################### 개별 파이프라인 실행을 연결하는 Workflow 자동 생성

existing_cluster_id = "000000000"  #작업 실행 담당할 컴퓨팅 붙여주기 : 컴퓨팅 ID
size = 20   #job에서 한번에 실행할 병렬 파이프라인 개수

job_name = f"{env}_{catalog}_{system}_{factory}_initial_etl"   # job 이름

tasks = []

for i, table in enumerate(table_list):
    task = {
            "task_key": f"task-{table}",
            "notebook_task": {"notebook_path": f"{target_path}/task-{table}"},
            "existing_cluster_id": existing_cluster_id,
            "run_if" : "ALL_DONE",
            "email_notifications": {
                                    "on_start": [],
                                    "on_success": [],
                                    "on_failure": ["ssssss@mmmm.co.kr"]
                                    }
            }
    
    if i >= size :
        parent_table = table_list[i-size]
        task["depends_on"] = [{"task_key": f"task-{parent_table}"}]
        task["run_if"] = "ALL_DONE"
        
    tasks.append(task)

job_payload = {
    "name": f"{job_name}",
    "tasks" : tasks
}

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.post(f"{workspace_url}/api/2.2/jobs/create", headers=headers, json=job_payload) #job 생성하는 api
print(response.json())
    