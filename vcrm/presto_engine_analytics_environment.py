#aws emr에서 presto 엔진으로 쿼리하여 분석 대상 데이터를 추출하는 작업 템플릿

from pyhive import presto

conn = presto.connect(
				host = 'ec2-3-200000-0000.ap-northeast-2.compute.amazonaws.com'
				port = 8889
				catalog = 'hive'
				schema = 'prd-ibd' )

presto_cursor = conn.cursor()

query = "select havin, timestamp, category, level1, level2 from a-ibd"

presto_cursor.execute(query)

batch_size = 10000

while True:
  rows = presto_cursor.fetchmany(batch_size)
  
  if not rows:
		  break

	df = pd.DataFrame(rows, columns=[desc[0] for desc in presto_cur.description])

  # 경로는 EMR에 연동된 s3 경로 아래로 parquet 객체로 저장
  s3_path = 's3://emr/jupyter/9871/output/202401014/presto/query_result'

 
  # DataFrame을 CSV로 S3에 저장
  df.to_csv(s3_path, index=False)