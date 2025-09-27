env = ""      #첨단 초기적재는 무조건 품질 서버에서 진행 하므로 dev로 설정할 것, 기초는 prd
catalog = ""  #lam_dev/prd(첨단) or lcc_dev/prd(기초) or common_dev/prd(공통:lims, pld 적재됨)

stage = ""  #bronze, silver, gold
system = ""   #시스템 : mes, wms, lims
factory = "" #공장 : lctj, lcdg...

table = ""

%run /Workspace/Shared/Common/CommonUtils


import boto3
import json
from datetime import datetime, timedelta
import pytz
from pyspark.sql.functions import lit
import gc
from delta.tables import DeltaTable

# 조건에 맞는 연결 정보 가져오기 ㅣ ip, port, db, user, password, schema
get_connection_info(env)

# 증분 데이터 수집 select
ingest_data() 

# 수집한 데이터 적재하기
load_data()

# 수집한 데이터 개수 VS 적재한 데이터 개수 ㅣ 검증
check_etl_cnt()

#### 로그 적재를 위해 실행 결과 넘겨주기
dbutils.jobs.taskValues.set(f"task_result", {
    "sel_cnt": sel_cnt,
    "int_cnt": int_cnt,
    "src_path": src_path,
    "tgt_path": target_table,
    "task_step": stage,
    "batch_start_dtm": batch_start_dtm,
    "batch_end_dtm": batch_end_dtm,
    "system": system
})