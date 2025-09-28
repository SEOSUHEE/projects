import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from datetime import datetime

# MSSQL 연결 정보

db_ip = "10.0.1.1"
port = "1433"
database = "MESLAM"
username = "username"
password = "password!"
s3_root_path = "s3://bucket/gluebucket/MSSQL/aaaa"


table_list = ["ITF_M2E_MATERIAL_GOOD_ISSUE", "ITF_M2E_PRODUCTION_CONFIRM", "ITF_M2E_PRODUCTION_GOOD_RECEIPT"]

for table in table_list :
  spark = SparkSession.builder.appName("MSSQLGlueSpark").getOrCreate()
  
  # JDBC URL 설정
  jdbc_url = f"jdbc:sqlserver://{db_ip}:{port};databaseName={database}"
  today = datetime.now()
  s3_final_path = f"{s3_root_path}/{table}/loop_test/{today}"
  
  # 데이터 로드
  df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("query", f"SELECT * FROM {table}") \\
    .option("user", username) \\
    .option("password", password) \\
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \\
    .load()

  # S3에 저장
  df.write.mode("overwrite").csv(s3_final_path)