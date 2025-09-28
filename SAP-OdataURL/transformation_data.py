def transfer_data():
    '''
    get_sap_data 종속 되어 공통함수로 사용
    sap url 호출 상태 확인 및 데이터 타입 변환 작업
    '''
    if str(response.status_code).startswith("4"):
        raise Exception(f"{response.status_code} : odata url 상태 확인")
    elif str(response.status_code).startswith("5"):
        raise Exception(f"{response.status_code} : 가져올 데이터가 많거나 SAP 서버에서 응답 불가")
    else:
        # odata url 호출이 정상인 경우에만 데이터 변환/정제 작업 실행
        print(f"{response.status_code} : odata url 호출 정상")
        data = response.json()
        records = data.get("d", {}).get("results", [])

        try:
            if len(records) != 0:  # 데이터가 없을 수도 있음 - 있는 경우에만 변환 처리
                df = spark.createDataFrame(records).drop("__metadata")

                if self.seq_std_col != 'None':
                    df = df.drop(f"{self.seq_std_col}")

                if self.rename_col != "None" and self.rename_col is not None:
                    for old_col, new_col in self.rename_col.items():
                        df = df.withColumnRenamed(old_col, new_col)

                if self.int_col != "None" and self.int_col is not None:
                    for col_name in self.int_col:
                        df = df.withColumn(col_name, df[col_name].cast(IntegerType()))

                if self.double_col != "None" and self.double_col is not None:
                    for col_name in self.double_col:
                        df = df.withColumn(col_name, df[col_name].cast(DoubleType()))

                if self.decimal_col != "None" and self.decimal_col is not None:
                    for col_name, (precision, scale) in self.decimal_col.items():
                        df = df.withColumn(col_name, col(col_name).cast(DecimalType(precision, scale)))

                if self.float_col != "None" and self.float_col is not None:
                    for col_name in float_col:
                        df = df.withColumn(col_name, df[col_name].cast(FloatType()))

                if self.table == "zce11000_a12_b":
                    df = df.withColumn("posnr", trim(df["posnr"]))  # t_co_zce11000_a12_b 테이블만 공란 자르기 추가

                dfs.append(df)
                del df  # df 변수 자체를 삭제
            else:
                print("추출 데이터 0건 !!")
        except Exception as e:
            raise Exception(f"데이터 변환 작업 에러 : {e}") from e