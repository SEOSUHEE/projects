# 전역 함수 선언
class GlobalConfig:
    date_info = {}

#################################################################################################################

def get_connection_info(env, catalog, system, factory):
    """
    온프레미스 연결 접속 추출 함수
    - 환경, 첨단 or 기초, 데이터 시스템 (mes, wms, lims 등), 공장에 따라 접속 정보가 모두 다름
    - 해당 정보에 따라 aws secret manager에서 정보를 가져옴 -> 보안을 위해 모두 변수로 처리

    env : dev or prd 데이터 브릭스 및 온프레미스 접속 환경
    catalog : 증분 키 타입
    system : world_time 함수의 리턴값
    factory : 공장
    """
    try:
        access_key = dbutils.secrets.get(f"{env}-aws-secrets", "access_key")
        secret_key = dbutils.secrets.get(f"{env}-aws-secrets", "secret_key")

        session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ap-northeast-2"
        )

        client = session.client("secretsmanager")
        secret_name = f"{env}_{catalog}_{system}_{factory}"
        response = client.get_secret_value(SecretId=secret_name)
        mssql_info = json.loads(response['SecretString'])

        return mssql_info

    except Exception as e:
        raise RuntimeError(f"MSSQL Secret 조회 실패: {e}")

#################################################################################################################

def check_world_time(factory):
    """
    증분 적재를 위한 세계시 추출 함수
    - 각 공장 별 시간 측정을 위해 증분 적재 시간 설정
    - 단, 수동 적재인 경우에는 입력한 값만을 참조함

    factory : 공장 정보
    """
    trigger_type = dbutils.widgets.get("trigger_type") #job 파라미터에서 이벤트 타입 가져오기

    if trigger_type == "one_time":
        start_dtm = datetime.strptime(dbutils.widgets.get("start_dtm"), "%Y%m%d")
        end_dtm   = datetime.strptime(dbutils.widgets.get("end_dtm"), "%Y%m%d")
        
        return {
            "yesterday": start_dtm.strftime("%Y-%m-%d"),
            "today": (end_dtm + timedelta(days=1)).strftime("%Y-%m-%d"),
            "batch_start_dtm": start_dtm.strftime("%Y-%m-%d 00:00:00"),
            "batch_end_dtm": end_dtm.strftime("%Y-%m-%d 23:59:59")
        }

    else:
        target_city = pytz.timezone(timezone_map[factory])            
        now = datetime.now(target_city)

        return {
            "yesterday": (now - timedelta(days=1)).strftime("%Y-%m-%d"),
            "today": now.strftime("%Y-%m-%d"),
            "batch_start_dtm": (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00"), #로그용
            "batch_end_dtm": (now - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59") #로그용
        }

#################################################################################################################

def make_world_condition(std_col, std_structure, date_info):
    """
    증분 적재 조건 생성 함수
    - 증분 적재인 경우, 증분 키 컬럼을 고려하여 적재 조건문 생성

    std_col : 증분 키 컬럼
    std_structure : 증분 키 타입
    date_info : world_time 함수의 리턴값
    """
    yesterday = config.date_info["yesterday"]
    today = config.date_info["today"]

    if std_structure == "yyyymmdd":
        return " or ".join(
            f"({col.strip()} like '{yesterday.replace('-', '')}%')" for col in std_col.split(",")
        )
    else:
        return " or ".join(
            f"({col.strip()} >= '{yesterday}' and {col.strip()} < '{today}')" for col in std_col.split(",")
        )

#######################################################################################################################

def ingest_data(mssql_info):
    """
    데이터 수집 함수 
    - 증분컬럼이 없는 경우 전체 데이터 수집
    - 증분컬럼이 있는 경우 전일자 데이터 수집 : 증분 컬럼 >= 전일자 and 증분 컬럼 < 오늘일자

    mssql_info : 접속할 온프레미스 DB 정보
    """
    if env == "dev":
        workspace = "lcc_dap_dev"    
    else:    
        workspace = "lcc_dap_prd"

    try:
        row = spark.read.table(f"{workspace}.{catalog}_etl_master.{system}_master_table").where(f"table = '{table}' AND factory = '{factory}'").first()

        std_col = row['std_col']
        std_structure = row['std_structure']
        
        # * 사용하지 않고, 데이터브릭스에 저장된 컬럼만을 가져오도록 
        target_table = f"{target_catalog}.{stage}_{system}_{factory}.{table}"
        cols = spark.catalog.listColumns(target_table)
        select_col = ", ".join([col.name for col in cols[:-1]])

        src_path = f"{mssql_info['schema']}.{table}" #로그에도 활용 예정

        if std_col is None : #전체 재적재, overwrite 테이블인 경우
            print("증분 키 컬럼이 없으므로 전체 데이터를 수집합니다..")
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK)) AS t"
            print(query)

            load_mode = "overwrite"

        else:  #전체 재적재가 아닌 경우 : 공장(지역) 시간에 맞춘 증분 적재 조건문 생성
            print("증분 적재 조건 생성 중....")

            config.date_info = check_world_time(factory)
            condition = make_world_condition(std_col, std_structure, config.date_info)
            
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK) WHERE {condition}) AS t"
            print(query)
            load_mode = None

        # 증분 데이터 가져오기    
        df = (
                spark.read.format("jdbc")
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .option("url", f"jdbc:sqlserver://{mssql_info['ip']}:{mssql_info['port']};databaseName={mssql_info['db']}")
                    .option("dbtable", query)
                    .option("user", mssql_info['user'])
                    .option("password", mssql_info['password'])
                    .option("encrypt", "false")
                    .load()
            )       
        
        # 적재 일자 추가 : UTC
        load_dtm = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #브릭스에 적재 후, 개수 count에 추가 활용
        df = df.withColumn("etl_load_dtm", to_timestamp(lit(load_dtm)))
        select_cnt = df.count()

        print(f"{table}에서 총 {select_cnt}개 데이터를 가져왔습니다.")
        print(f"적재할 테이블 : {target_table}")

        return df, select_cnt, src_path, target_table, load_mode, load_dtm

    except Exception as e:
        print(f"데이터 수집 중에 오류가 발생했습니다. {e}")
        raise

################################################################################################################  

def load_data(df, target_table, load_mode, load_dtm) :
    """
    데이터 적재 함수 
    - 증분컬럼이 없는 경우 전체 데이터를 가져온 후, 전체 재적재
    - 증분컬럼이 있는 경우 전일자 데이터를 가져온 후,pk를 기준으로 merge(update or insert), pk가 없다면 only insert

    df : 수집한 데이터
    target_table : 데이터 브릭스 상에 적재할 테이블
    load_mode : 적재 방법 overwrite or None(append, merge)
    load_dtm : 적재 일자
    """
    try: 
        if load_mode == 'overwrite' : #전체 재적재, overwrite 테이블인 경우
            df.write.mode("overwrite").saveAsTable(target_table)
        
        else :
            delta_target = DeltaTable.forName(spark, f"{target_table}")

            pk_df = spark.sql(f"""SELECT kcu.column_name
                                    FROM {target_catalog}.information_schema.key_column_usage kcu
                                    JOIN {target_catalog}.information_schema.table_constraints tc
                                    ON kcu.constraint_name = tc.constraint_name
                                    WHERE tc.table_name = '{table}'
                                    AND tc.constraint_type = 'PRIMARY KEY'
                                    ORDER BY kcu.ordinal_position
                                    """)
            
            if pk_df.count() == 0: #pk가 없다면 무조건 insert
                df.write.mode("append").saveAsTable(target_table)

            else:   #pk가 있으므로 update or insert
                pk_col = [row['column_name'] for row in pk_df.collect()]
                on_condition = " AND ".join([f"source.{col} = target.{col}" for col in pk_col])
    
                print(f"merge(upsert) 조건 : {on_condition}")
                (delta_target.alias("target")
                            .merge(source=df.alias("source"),condition=on_condition)
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute())
        
        insert_cnt = spark.sql(f"select count(*) as cnt from {target_table} where etl_load_dtm = '{load_dtm}'").collect()[0]['cnt']
        
        print(f"{target_table}에 {insert_cnt}개 데이터를 저장했습니다.")

        return insert_cnt
    
    except Exception as e:
        print(f"데이터 적재 중에 에러가 발생했습니다. {e}")
        raise
        
################################################################################################################  

def check_etl_cnt(select_cnt, insert_cnt):
    """
    수집 및 적재 검증 함수
    - 수집한 데이터 개수와 적재된 데이터 개수 일치 확인

    select_cnt : 수집한 데이터 건수
    insert_cnt : 적재된 데이터 건수, etl_load_dtm 기준
    """

    if insert_cnt == select_cnt:
        print(f"수집 데이터 개수 : {insert_cnt} / 적재 데이터 개수 : {select_cnt}")
        print("데이터 수집 및 적재가 정상적으로 완료되었습니다.")   
    else:
        print(f"수집 데이터 개수 : {insert_cnt} / 적재 데이터 개수 : {select_cnt}")
        raise Exception("수집한 데이터와 적재된 데이터 개수가 일치하지 않습니다. 확인 필요!")
