import sys, json

def main():
    event = json.loads(sys.argv[1])  # EventBridge에서 전달된 이벤트 : s3 경로
    bucket = event["detail"]["bucket"]["name"]
    key = event["detail"]["object"]["key"]
    return bucket, key  # 반드시 반환

if __name__ == "__main__":
    try:
        bucket, key = main()  # 반환값 받아야 함

        s3_input_path = f"s3://{bucket}/{key}"
        table = key.split("/")[1]

        # 신규 데이터 읽기 (S3 → DataFrame)
        df = spark.read.format("csv").option("header", "true").load(s3_input_path)

        (
            df.write
            .mode("overwrite")  # 덮어쓰기
            .format("parquet")
            .saveAsTable(f"{bucket}.{table}")
        )

        print(f"신규 데이터로 마스터 {table} 테이블 overwrite 완료")
    except Exception as e:
        print(f"신규 마스터 {table} 테이블 데이터 적재 실패 !! : {e}")