def ingest_data():
    try:
        if self.sub_url == "None":  # 시퀀스컬럼 및 추가 url이 필요 없는 테이블 중에서
            if self.std_structure == "yyyy/mm" and self.period == "month":  # 월작업 + year컬럼/month컬럼이 분리된 경우
                while int(self.start_dtm) <= int(self.end_dtm):
                    year = self.start_dtm[:4]
                    month = '0' + self.start_dtm[4:6]
                    values = [year, month]
                    condition = " and ".join(
                        [f"{col.strip()} eq '{val}'" for col, val in zip(self.std_col.split(","), values)]
                    )
                    url = f"{self.base_url}{self.src_path}?$filter={condition}&$format=json"
                    print(url)
                    print(f"적재 대상 : {year}, {month}")
                    response = requests.get(url, headers=headers)
                    transfer_data()
                    print("=============================다음 적재================================")
                    self.start_dtm = (datetime.strptime(self.start_dtm, "%Y%m") + relativedelta(months=1)).strftime("%Y%m")

            elif self.std_structure == "yyyymm" and self.period == "month":  # 월작업 + yearmonth컬럼이 하나인 경우
                while int(self.start_dtm) <= int(self.end_dtm):
                    start_dttm = self.start_dtm[:4] + '0' + self.start_dtm[4:6]
                    condition = f"{self.std_col} eq '{start_dttm}'"
                    url = f"{self.base_url}{self.src_path}?$filter={condition}&$format=json"
                    print(url)
                    print(f"적재 대상 : {self.start_dtm}")
                    response = requests.get(url, headers=headers)
                    transfer_data()
                    print("==============================다음 적재================================")
                    self.start_dtm = (datetime.strptime(self.start_dtm, "%Y%m") + relativedelta(months=1)).strftime("%Y%m")

            elif self.std_structure == "yyyymmdd":  # yyyymmdd 형태인 경우 -> 일작업 or 월작업
                while int(self.start_dtm) <= int(self.end_dtm):
                    start_dttm = quote(f"'{self.start_dtm}'")
                    condition = " or ".join([f"{col.strip()} eq {start_dttm}" for col in self.std_col.split(",")])
                    url = f"{self.base_url}{self.src_path}?$filter={condition}&$format=json"
                    print(url)
                    print(f"적재 대상 : {self.start_dtm}")
                    response = requests.get(url, headers=headers)
                    transfer_data()
                    print("==============================다음 적재================================")
                    self.start_dtm = (datetime.strptime(self.start_dtm, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")

        else:  # 시퀀스컬럼 + 추가 url이 있는 경우
            start_dttm = quote(f"'{self.start_dtm}'")
            end_dttm = quote(f"'{self.end_dtm}'")
            sub_condition = ' or '.join(
                [f"({col.strip()} ge '{self.start_dtm}' and {col.strip()} le '{self.end_dtm}')" for col in
                    self.std_col.split(',')]
            )
            url = f"{self.base_url}{self.sub_url}?$filter={sub_condition}&$format=json"
            print(url)
            response = requests.get(url, headers=headers)
            print(response.status_code)
            data = response.json()
            conditions = data.get("d", {}).get("results", [])
            std_col_list = self.std_col + ", " + self.seq_std_col
            for cod in conditions:
                condition = " and ".join([f"{std.strip()} eq '{cod[std.strip()]}'" for std in std_col_list.split(',')])
                url = f"{self.base_url}{self.src_path}?$filter={condition}&$format=json"
                print(url)
                response = requests.get(url, headers=headers)
                transfer_data()

        if dfs:  # 추출된 데이터가 있다면 dfs(list)를 final_df(dataframe)로 변환 후, load_dtm 컬럼 추가
            load_dtm = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            final_df = reduce(lambda df1, df2: df1.unionByName(df2), dfs)
            final_df = final_df.selectExpr("*", f"'{load_dtm}' as load_dtm")
            row_count = final_df.count()
            print("데이터 추출 완료 !!")
        else:  # 추출된 데이터가 없다면 모든 값 '0'
            row_count = 0
            load_dtm = 0
            final_df = 0
            print("추출 데이터 0건 !!")

    except Exception as e:
        raise Exception(f"데이터 추출 작업 에러 : {e}") from e

    return final_df, row_count, load_dtm