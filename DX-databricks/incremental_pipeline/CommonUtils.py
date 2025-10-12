def get_connection_info(env):
    """
    온프레미스 MSSQL 서버 정보 추출 함수 
    - 각 공장의 데이터 서버 정보를 AWS Secret Manager에서 가져온다 : 보안을 위해 변수로 활용

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """
    try:
        access_key = dbutils.secrets.get(f"{env}-aws-secrets", "access_key")
        secret_key = dbutils.secrets.get(f"{env}-aws-secrets", "secret_key")

        session = boto3.session.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="ap-northeast-2"
        )

        #Secrets Manager 클라이언트 생성
        client = session.client("secretsmanager")

        #Secret 값 가져오기
        secret_name = f"{env}_{catalog}_{system}_{factory}"   
        response = client.get_secret_value(SecretId=secret_name)

        secret_string = response['SecretString']
        secret_dict = json.loads(secret_string)
        print(secret_dict)

        # 전역 변수 선언 
        global ip, user, password, port, database, schema, target_catalog 

        #MSSQL 정보 추출
        ip = secret_dict['ip']
        user = secret_dict['user']
        password = secret_dict['password']
        port = secret_dict['port']
        database = secret_dict['db']
        schema = secret_dict['schema']

        target_catalog = f"{catalog}_{env}"

    except Exception as e:
        print(f"MSSQL에 접근이 불가합니다. AWS Secret Manager에 입력된 정보를 확인하세요. {e}")
        raise

######################################################################################################################

def check_world_time():
    """
    데이터 조건 생성 함수 
    - 증분컬럼이 있는 경우 전일자 데이터 수집을 위해 각 공장 별 시간에 맞춰 전일자 데이터 계산

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """
    # 전역 변수 선언 : 증분키 형태에 따라 변형하는 로직 추가 예정 
    global yesterday, today, batch_start_dtm, batch_end_dtm

    trigger_type = dbutils.widgets.get("trigger_type")


    if std_type == 'initial' :
        batch_start_dtm = "total"
        batch_end_dtm = "total"
    
    else :
        if trigger_type == 'one_time' : #수동으로 실행된 작업 : 시간을 매개변수로 받음
            start_dtm = datetime.strptime(dbutils.widgets.get("start_dtm"), "%Y%m%d")
            end_dtm   = datetime.strptime(dbutils.widgets.get("end_dtm"), "%Y%m%d")

            yesterday = start_dtm.strftime("%Y-%m-%d")
            today = (end_dtm + timedelta(days=1)).strftime("%Y-%m-%d")

            batch_start_dtm = start_dtm.strftime("%Y-%m-%d 00:00:00") #로그용
            batch_end_dtm = end_dtm.strftime("%Y-%m-%d 23:59:59") #로그용

        else: #트리거에 의해 자동으로 실행된 작업 : 자동 시간 계산
            if factory == 'yeosu' or factory == "ulsan" or factory == "daesan":
                target_city = pytz.timezone('Asia/Seoul') # 한국 시간 
            
            elif factory == 'lcjx' or factory == 'lctj' or factory == 'lcdg': #중국 가흥, 천진, 동관 : 상하이와 동일, -1h
                target_city = pytz.timezone('Asia/Shanghai')

            elif factory == 'lchu': #유럽 헝가리 : -7h
                target_city = pytz.timezone('Europe/Budapest')

            elif factory == 'lcal':  #미국 알리바마 : 시카고 동일 : -14h
                target_city = pytz.timezone('America/Chicago')

            elif factory == 'lcmx': #멕시코 멕시코시티 : -15h
                target_city = pytz.timezone('America/Mexico_City')

            elif factory == 'lcid': #인도네시아 : bekasi 공장 : 자카르타 동일, -2h
                target_city = pytz.timezone('Asia/Jakarta')

            elif factory == 'lchr': #인도 : -3.5h
                target_city = pytz.timezone('Asia/Kolkata')

            elif factory == 'lcvn' : #베트남 : -2h
                target_city = pytz.timezone('Asia/Ho_Chi_Minh')
            
            now = datetime.now(target_city)

            yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
            today = now.strftime("%Y-%m-%d")

            batch_start_dtm = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00") #로그용
            batch_end_dtm = (now - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59") #로그용

    print(f"증분 작업 대상 : {batch_start_dtm} ~ {batch_end_dtm}")

######################################################################################################################

def make_world_condition():
    """
    온프레미스 MSSQL에서 증분 적재할 조건문을 생성하는 함수
    - 해당 함수는 std_type(증분 적재 작업 형태)가 initial이 아닌 경우에만 사용
    - 즉, 전체 재적재(=overwrite) 가 아닌 테이블이 대상

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """

    # 전역 변수 선언 : 원천 DB에서 가져올 데이터 조건절 
    global condition

    # 기본 조건 : CRD_DT, UPT_DT : 15일 이후 추가 예정
    # condition = f"(CRD_DT >= '{yesterday}' and CRD_DT < '{today}') or (UPT_DT >= '{yesterday}' and UPT_DT < '{today}')"

    if std_structure == "yyyymmdd":
        condition = " or ".join(f"({std.strip()} like '{yesterday.replace("-", "")}%')" for std in std_col.split(","))

    else: #증분키가 yyyy-mm-dd 인 경우
        condition = " or ".join(f"({std.strip()} >= '{yesterday}' and {std.strip()} < '{today}')" for std in std_col.split(","))

######################################################################################################################

def ingest_data():
    """
    데이터 수집 함수 
    - 증분컬럼이 없는 경우 전체 데이터 수집
    - 증분컬럼이 있는 경우 전일자 데이터 수집 : 증분 컬럼 >= 전일자 and 증분 컬럼 < 오늘일자

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """
    if env == "dev":
        workspace = "lcc_dap_dev"    
    else:    
        workspace = "lcc_dap_prd"

    try:
        # 전역 변수 선언 
        global mode, load_dtm, df, sel_cnt, src_path, target_table, std_col, std_structure, std_type

        row = spark.read.table(f"{workspace}.{catalog}_etl_master.{system}_master_table").where(f"table = '{table}' AND factory = '{factory}'").first()

        std_col = row['std_col']
        std_structure = row['std_structure']
        std_type = row['std_type']
        
        # * 사용하지 않고, 데이터브릭스에 저장된 컬럼만을 가져오도록 
        target_table = f"{target_catalog}.{stage}_{system}_{factory}.{table}"
        cols = spark.catalog.listColumns(target_table)
        select_col = ", ".join([col.name for col in cols[:-1]])

        src_path = f"{schema}.{table}" #로그에도 활용 예정

        if std_type == 'initial' : #전체 재적재, overwrite 테이블인 경우
            print("primary key 혹은 증분 기준 컬럼이 없으므로 전체 데이터 수집 진행")
            check_world_time()
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK)) AS t"
            print(query)

        else:  #전체 재적재가 아닌 경우 : 공장(지역) 시간에 맞춘 증분 적재 조건문 생성
            print("증분 적재 조건 생성 중....")
            check_world_time()
            make_world_condition()
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK) WHERE {condition}) AS t"
            print(query)

        # 증분 데이터 가져오기    
        df = (
                spark.read.format("jdbc")
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
                    .option("url", f"jdbc:sqlserver://{ip}:{port};databaseName={database}")
                    .option("dbtable", query)
                    .option("user", user)
                    .option("password", password)
                    .option("encrypt", "false")
                    .load()
            )       
        
        # 적재 일자 추가 : UTC
        load_dtm = datetime.now().strftime("%Y-%m-%d %H:%M:%S") #적재 개수 count에 활용
        df = df.withColumn("etl_load_dtm", to_timestamp(lit(load_dtm)))

        sel_cnt = df.count()
        print(f"{table}에서")
        print(f"총 {sel_cnt}개 데이터를 가져왔습니다.")
        print(f"적재할 테이블 : {target_table}")

    except Exception as e:
        print(f"데이터 수집 중에 오류가 발생했습니다. {e}")
        raise

######################################################################################################################    

def load_data() :
    """
    데이터 적재 함수 
    - 증분컬럼이 없는 경우 전체 데이터를 가져온 후, 전체 재적재
    - 증분컬럼이 있는 경우 전일자 데이터를 가져온 후,pk를 기준으로 merge(update or insert)적재

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """
    try: 
        if std_type == 'initial' : #전체 재적재, overwrite 테이블인 경우
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
                print(on_condition)

                print(f"merge(upsert) 조건 : {on_condition}")
                (delta_target.alias("target")
                            .merge(source=df.alias("source"),condition=on_condition)
                            .whenMatchedUpdateAll()
                            .whenNotMatchedInsertAll()
                            .execute())
        
        global int_cnt  # 전역 변수 선언 
        int_cnt = spark.sql(f"select count(*) as cnt from {target_table} where etl_load_dtm = '{load_dtm}'").collect()[0]['cnt']
        
        print(f"{target_table}에 {int_cnt}개 데이터를 저장했습니다.")
    
    except Exception as e:
        print(f"데이터 적재 중에 에러가 발생했습니다. {e}")
        raise
    
######################################################################################################################

def check_etl_cnt():
    """
    수집 및 적재 검증 함수
    - 수집한 데이터 개수와 적재된 데이터 개수 일치 확인

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """

    if int_cnt == sel_cnt:
        print(f"수집 데이터 개수 : {int_cnt} / 적재 데이터 개수 : {sel_cnt}")
        print("데이터 수집 및 적재가 정상적으로 완료되었습니다.")   
    else:
        print(f"수집 데이터 개수 : {int_cnt} / 적재 데이터 개수 : {sel_cnt}")
        raise Exception("수집한 데이터와 적재된 데이터 개수가 일치하지 않습니다. 확인 필요!")
