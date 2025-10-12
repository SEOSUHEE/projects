import pandas as pd
from datetime import datetime, timedelta 
from pyspark.sql.functions import lit
import pytz
import requests

env = dbutils.widgets.get("env")
workspace_url = dbutils.secrets.get(f"{env}-workspace-information", "workspace_url")
token = dbutils.secrets.get(f"{env}-workspace-information", "token")

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true") #컬럼명 기준으로 자동 매칭되도록 설정

api_url = f'{workspace_url}/api/2.2/jobs/runs/list' #실행한 job 리스트를 가져오는 데이터브릭스 api
params = {'active_only' : 'true'} #접근 활성화
headers = {'Authorization' : f'Bearer {token}','Content-Type' : 'application/json'} #데이터브릭스 api 호출 시 헤더 인증

job_id = dbutils.widgets.get("job_id") #내가 실행한 job에 해당하는 정보만 가져옴

try:
    response = requests.get(api_url, headers=headers, params=params)
    response_json = response.json()

    running_jobs = response_json['runs']
    for running_job in running_jobs:
        if str(running_job['job_id']) == job_id:
            job_run_id = running_job['run_id']   #해당 JOB의 금번 실행 ID 추출
            
except Exception as error: 
    raise error 



all_tasks = []
page_token = None

while True:
    url = f"{workspace_url}/api/2.2/jobs/runs/get?run_id={job_run_id}" if page_token is None else f"{workspace_url}/api/2.2/jobs/runs/get?run_id={job_run_id}&page_token={page_token}"
    response = requests.get(url, headers=headers)

    data = response.json()
    all_tasks.extend(data.get("tasks", []))

    # 다음 페이지 토큰 있으면 계속, 없으면 종료
    page_token = data.get("next_page_token")
    if not page_token:
        break

seoul = pytz.timezone('Asia/Seoul')
now_seoul = datetime.now(seoul)

rows = []

for task in all_tasks:
    task_key = task.get("task_key")
    
    if task_key == "task-log":
        continue #log 태스크는 제외

    task_result = dbutils.jobs.taskValues.get(
        taskKey=task_key,
        key="task_result",
        default={}
    )

    rows.append({
        "task_run_id": str(task.get("run_id")),
        "task_nm": task_key,
        "run_start_dtm": datetime.fromtimestamp(task.get("start_time", 0)/1000).strftime("%Y-%m-%d %H:%M:%S"),
        "run_end_dtm": datetime.fromtimestamp(task.get("end_time", 0)/1000).strftime("%Y-%m-%d %H:%M:%S"),
        "run_sec": str(task.get("execution_duration", 0)/1000),
        "task_retry": str(task.get("attempt_number")),
        "log_dtm": now_seoul.strftime("%Y-%m-%d %H:%M:%S"),
        "job_nm": data['run_name'],
        "execution_type": data["trigger"],
        "job_id": str(job_id),
        "job_run_id": str(job_run_id),
        "err_msg": task.get("status", {}).get("termination_details", {}).get("message", ""),
        "sel_cnt": str(task_result.get("sel_cnt")),
        "int_cnt": str(task_result.get("int_cnt")),
        "src_path": task_result.get("src_path"),
        "tgt_path": task_result.get("tgt_path"),
        "task_step": task_result.get("task_step"),
        "batch_start_dtm": str(task_result.get("batch_start_dtm")),
        "batch_end_dtm": str(task_result.get("batch_end_dtm")),
        "system": task_result.get("system")
    })

df = spark.createDataFrame(rows)
first_job_nm = df.select("job_nm").first()["job_nm"]  # 첫 행의 job_nm 값 가져오기

if env == "dev":
    log_catalog = "lcc_dap_dev"
else :
    log_catalog = "lcc_dap_prd"

if "lam" in first_job_nm :
    log_table = "lam_log_table"
    log_schema = "lam_etl_log"
elif "lcc" in first_job_nm :
    log_table = "lcc_log_table"
    log_schema = "lcc_etl_log"
elif "common" in first_job_nm :
    log_table = "common_log_table"
    log_schema = "common_etl_log"

df.write.mode("append").saveAsTable(f"{log_catalog}.{log_schema}.{log_table}")
