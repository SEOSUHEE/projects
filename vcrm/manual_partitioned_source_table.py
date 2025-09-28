# MSCK를 athena로 실행하면 해당 경로의 데이터(파일 객체)를 모두 읽어 비용 과다 
# 오직 신규 생성된 데이터/파일을 기준으로 파티셔닝 진행

import boto3


IBD_DATABASE = 'prd_ibd_raw'  
IBD_TABLE = ['a_new', 'b_new']

def lambda_handler(event, context):
    pt = event['pt']  # 20250318 : 파티셔닝해야 하는 일자를 수동으로 입력 받음

    print(date)
    
    for ibd_table in IBD_TABLE :
        ibd_query = f'ALTER TABLE {IBD_DATABASE}.{ibd_table} ADD IF NOT EXISTS'
            
        s3_client = boto3.client('s3')
        
        obj_list = s3_client.list_objects_v2(Bucket='prd-ibd-s3', Prefix=f'raw/{ibd_table}/pt={pt}/')
         
        
        file_list = []
            
        for obj in obj_list:
            key = obj['Key']
            file_list.append(key)
                
        for file in file_list :
            trip_pt = file.split('tripserialnumberpt=')[1][:8]
            
            partitioned_query = f" PARTITION (pt='{pt}', tripserialnumberpt='{trip_pt}')"
            ibd_query += partitioned_query
                
        
        ibd_query += ';'
            
        athena_client = boto3.client('athena')
            
        response_ibd = athena_client.start_query_execution(
            QueryString=ibd_query,
            QueryExecutionContext={'Database': IBD_DATABASE},
            ResultConfiguration={'OutputLocation': 's3://aws-athena-query-result-ap-northeast-2'}
        )