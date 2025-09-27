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

def make_world_condition():
    """
    데이터 조건 생성 함수 
    - 증분컬럼이 있는 경우 전일자 데이터 수집을 위해 각 공장 별 시간에 맞춰 전일자 데이터 계산

    함수 내부에서 생성된 값을 전역 변수로 선언하여 하나의 노트북 내에서 공유가 되므로 변수 추가 생성 혹은 return을 사용하지 않음
    """

    # 한국 시간 
    if factory == 'yeosu' or factory == "ulsan" or factory == "daesan":
        target_city = pytz.timezone('Asia/Seoul')
    
    elif factory == 'lcjx' or factory == 'lctj' or factory == 'lcdg': #중국 가흥, 천진, 동관 : 상하이와 시간 동일
        target_city = pytz.timezone('Asia/Shanghai')

    elif factory == 'lchu': #유럽 헝가리
        target_city = pytz.timezone('Europe/Budapest')

    elif factory == 'lcal':  #미국 알리바마 : 시카고 동일
        target_city = pytz.timezone('America/Chicago')

    elif factory == 'lcmx': #멕시코
        target_city = pytz.timezone('Mexico/City_of_Mexico')

    elif factory == 'lcid': #인도네시아 : bekasi 공장 : 자카르타 동일
        target_city = pytz.timezone('Asia/jakarta')

    elif factory == 'lchr': #인도
        target_city = pytz.timezone('Asia/Kolkata')

    elif factory == 'lcvn' : #베트남
        target_city = pytz.timezone('Asia/Ho_Chi_Minh')

    now = datetime.now(target_city)

    # 전역 변수 선언 
    global yesterday, today, batch_start_dtm, batch_end_dtm

    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d")
    today = now.strftime("%Y-%m-%d")

    batch_start_dtm = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00") #로그용
    batch_end_dtm = (now - timedelta(days=1)).strftime("%Y-%m-%d 23:59:59") #로그용

    print(f"증분 대상 : {batch_start_dtm} ~ {batch_end_dtm}")

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
        global mode, load_dtm, df, sel_cnt, src_path, target_table

        row = spark.read.table(f"{workspace}.{catalog}_etl_master.{system}_master_table").where(f"table = '{table}' AND factory = '{factory}'").first()
        std_col = row['std_col']
        
        target_table = f"{target_catalog}.{stage}_{system}_{factory}.{table}"
        cols = spark.catalog.listColumns(target_table)
        select_col = ", ".join([col.name for col in cols[:-1]])

        src_path = f"{schema}.{table}" #로그에도 활용

        if std_col is None :
            print("증분 컬럼이 없으므로 전체 재적재 진행")
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK)) AS t"
            mode = "overwrite"

        else:
            make_world_condition()
            condition =  " or ".join(f"({std.strip(' ')} >= '{yesterday}' and {std.strip(' ')} < '{today}')" for std in std_col.split(",")) 
            query = f"(SELECT {select_col} FROM {src_path} WITH(NOLOCK) WHERE {condition}) AS t"
            mode = "merge"


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
        df = df.withColumn("etl_load_dtm", lit(load_dtm))

        sel_cnt = df.count()
        print(f"{table}에서")
        print(f"총 {sel_cnt}개 데이터를 가져왔습니다.")

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
        if mode == "overwrite":
            df.write.mode("overwrite").saveAsTable(target_table)
        
        elif mode == "merge":
            delta_target = DeltaTable.forName(spark, f"{target_table}")

            pk_df = spark.sql(f"""SELECT kcu.column_name
                                    FROM {target_catalog}.information_schema.key_column_usage kcu
                                    JOIN {target_catalog}.information_schema.table_constraints tc
                                    ON kcu.constraint_name = tc.constraint_name
                                    WHERE tc.table_name = '{table}'
                                    AND tc.constraint_type = 'PRIMARY KEY'
                                    ORDER BY kcu.ordinal_position
                                    """)


            pk_col = [row['column_name'] for row in pk_df.collect()]
            on_condition = " AND ".join([f"source.{col} = target.{col}" for col in pk_col])
            # on_condition = " AND ".join([f"source.{col.strip(' ')} = target.{col.strip(' ')}" for col in pk_col.split(",")])

            print(f"merge 조건 : {on_condition}")
            (delta_target.alias("target")
                        .merge(source=df.alias("source"),condition=on_condition)
                        .whenMatchedUpdateAll()
                        .whenNotMatchedInsertAll()
                        .execute())
        
        global int_cnt  # 전역 변수 선언 
        int_cnt = spark.sql(f"select count(*) as cnt from {target_table} where load_dtm = '{load_dtm}'").collect()[0]['cnt']
        
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
        raise Exception("수집 데이터와 적재 데이터 개수가 일치하지 않습니다. 확인 필요!")
