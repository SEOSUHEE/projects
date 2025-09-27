def store_sap_data(self):
    '''
    get_sap_data에서 가져온 데이터를 적재하는 함수
    '''
    if self.std_col is None or self.std_col == "None":
        raise Exception("증분컬럼(std_col)이 None 이므로 작업 대상이 아닙니다.")
        return

    elif final_df == 0:
        print("추출된 데이터가 없어 적재할 대상이 없습니다... ")
        return

    print("데이터 적재 중.....")

    try:
        if self.job_type == 'oper':  # 증분키가 있다는 의미
            target_table = DeltaTable.forName(spark, f"{self.table_name}")

            if self.pk_col == "None":  # 증분키는 있지만 pk가 없다면 -> merge 할수 없는 delete - insert 작업
                if self.std_structure == "yyyy/mm":  # 월작업인데 year/month컬럼이 분리된 경우
                    condition_dtm = self.tmp_start_dtm

                    while int(condition_dtm) <= int(self.end_dtm):
                        year = condition_dtm[:4]
                        month = "0" + condition_dtm[4:6]
                        values = [year, month]
                        condition = " and ".join([f"{col.strip()} eq '{val}'" for col, val in zip(self.std_col.split(","), values)])
                        print(f"pk가 없어서 증분키 기준으로 데이터 delete : {condition}")
                        target_table.delete(condition)
                        condition_dtm = (datetime.strptime(condition_dtm, "%Y%m") + relativedelta(months=1)).strftime("%Y%m")

                elif self.std_structure == "yyyymm":  # 월작업인데 year/month컬럼이 하나인 경우
                    condition_dtm = self.tmp_start_dtm

                    while int(condition_dtm) <= int(self.end_dtm):
                        condition_dttm = condition_dtm[:4] + '0' + condition_dtm[4:6]
                        condition = f"{self.std_col} = {condition_dttm}"
                        print(f"pk가 없어서 증분키 기준으로 데이터 delete : {condition}")
                        target_table.delete(condition)
                        condition_dtm = (datetime.strptime(condition_dtm, "%Y%m") + relativedelta(months=1)).strftime("%Y%m")

                elif self.std_structure == "yyyymmdd":  # 월작업 or 일작업
                    condition_dtm = self.tmp_start_dtm

                    while int(condition_dtm) <= int(self.end_dtm):  # 년+월 / 년월 / 일자
                        condition = " or ".join([f"{col.strip()} = {condition_dtm}" for col in self.std_col.split(",")])
                        print(f"pk가 없어서 증분키 기준으로 데이터 delete : {condition}")
                        target_table.delete(condition)
                        condition_dtm = (datetime.strptime(condition_dtm, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

                final_df.write.format("delta").mode("append").saveAsTable(f"{self.table_name}")
                print("pk 없이 append(insert) 적재 완료")

            elif self.pk_col != "None":  # 증분키가 있고 pk도 있다면
                if isinstance(self.pk_col, str) and "," in self.pk_col:  # pk가 여러개 존재한다면
                    pk_col_list = [col.strip() for col in self.pk_col.split(",")]
                    condition = " AND ".join([f"target.{col} = source.{col}" for col in pk_col_list])
                    print(condition)
                else:  # pk가 1개라면
                    condition = f"target.{self.pk_col} = source.{self.pk_col}"
                    print(condition)

                self.target_table.alias("target").merge(
                    source=final_df.alias("source"),
                    condition=condition
                ).whenMatchedUpdateAll(
                ).whenNotMatchedInsertAll(
                ).execute()

                print("pk 기준으로 merge 적재 완료")

    except Exception as e:
        raise Exception(f"데이터 적재 작업 에러 : {e}") from e