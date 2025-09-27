env = "dev"      
catalog = "aaa"  
system = "mes"   
factory = "yeosu" #공장 : lctj, lcdg...
stage = "bronze"  #bronze, silver, gold


#초기적재는 initial 증분적재는 incremental
target_path = f"/Workspace/Shared/incremental/{catalog}/{stage}_{system}_{factory}"  # 노트북이 생성될 경로

import requests
import base64
import json

if env == "dev":
    workspace_catalog = "dev"
else:
    workspace_catalog = "prd"

# Databricks workspace 정보
workspace_url = "https://dbc.cloud.databricks.com"  #워크스페이스
token = "ddddddddddddd" #개인 유저 토큰, 만료 730일

# API endpoint
url = f"{workspace_url}/api/2.0/workspace/import"
headers = {"Authorization": f"Bearer {token}"}

table_list = [row['tableName'] for row in spark.sql(f"show tables in {catalog}.{stage}_{system}_{factory}").collect()]

for table in table_list:
    # 노트북에 들어갈 초기 코드
    cell1=f"""env = "{env}"  #prd or dev
stage = "{stage}"  #bronze silver gold
catalog = "{catalog}"   #lam or lcc
system = "{system}"    #mes wms lims ..
factory = "{factory}"  #yeosu lcid lctj ..
table = "{table}"   
"""
    
    cell2 = f"""%run /Workspace/Shared/Common/CommonUtils"""

    cell3  ="""
import boto3
import json
from datetime import datetime, timedelta
import pytz
from pyspark.sql.functions import lit
import gc
from delta.tables import DeltaTable
"""

    cell4 = """# 조건에 맞는 연결 정보 가져오기 ㅣ ip, port, db, user, password, schema
get_connection_info(env)"""

    cell5 = """# 증분 데이터 수집 select
ingest_data()"""

    cell6 = """# 수집한 데이터 적재하기
load_data()"""

    cell7 = """# 수집한 데이터 개수 VS 적재한 데이터 개수 ㅣ 검증
check_etl_cnt()"""

    cell8 = """#### 로그 적재를 위해 실행 결과 넘겨주기
dbutils.jobs.taskValues.set(f"task_result", {
    "sel_cnt": sel_cnt,
    "int_cnt": int_cnt,
    "src_path": src_path,
    "tgt_path": target_table,
    "task_step": stage,
    "batch_start_dtm": batch_start_dtm,
    "batch_end_dtm": batch_end_dtm,
    "system": system
})"""

    # 노트북 JSON 구조
    notebook_content = {
                            "cells": [
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell1.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell2.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell3.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell4.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell5.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell6.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell7.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                },
                                {
                                    "cell_type": "code",
                                    "metadata": {},
                                    "source": cell8.splitlines(True),
                                    "outputs": [],
                                    "execution_count": None
                                }
                            ],
                            "metadata": {},
                            "nbformat": 4,
                            "nbformat_minor": 0
                        }

    notebook_path = f"{target_path}/task-{table}"
    content = base64.b64encode(json.dumps(notebook_content).encode("utf-8")).decode("utf-8")

    # API 호출 (노트북 업로드)
    payload = {
        "path": notebook_path,
        "format": "JUPYTER",  # Jupyter 형식으로 업로드해야 셀이 여러개 생김
        "language": "PYTHON",
        "content": content
    }

    response = requests.post(url, headers=headers, json=payload)
    
    if response.status_code == 200:
        print(f" 생성 완료 : {notebook_path}")
    else:
        print(f"생성 실패함... {notebook_path}: {response.text}")

######################## Workflow 생성 ##################
job_name = f""  # job 이름
existing_cluster_id = "000000000000" #task에 적용할 클러스터 ID
size = 20   #job에서 한번에 실행할 task 개수 : 병렬 실행 개수

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
        
    tasks.append(task)

task = {
            "task_key": f"task-log",
            "notebook_task": {"notebook_path": f"/Workspace/Shared/Common/task-log"},
            "existing_cluster_id": existing_cluster_id,
            "depends_on" : [{"task_key" : f"task-{table}"} for table in table_list],
            "run_if" : "ALL_DONE"
            }

tasks.append(task)

job_payload = {
    "name": job_name,
    "tasks": tasks,
    "parameters": [
                    {
                        "name": "job_id",
                        "default": "{{job.id}}"
                    }
                ]
}

headers = {
    "Authorization": f"Bearer {token}",
    "Content-Type": "application/json"
}

response = requests.post(f"{workspace_url}/api/2.2/jobs/create", headers=headers, json=job_payload)
print(response.json())