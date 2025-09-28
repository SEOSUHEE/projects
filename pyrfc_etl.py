import os
import awswrangler as aw
import sys
import pyrfc
import pandas as pd

# SAP 연결 설정
conn_params = {
    "ashost": "1.1.1.1",
    "sysnr": "01",
    "client": "100", 
    "user": "C1",
    "passwd": "in!",
    "lang": "KO" #한국어
}

def lambda_handler(event, context):
    try:
        params = {}
        for key, value in event.items():
            if value != "null":
                params[key] = value

        # SAP 연결
        conn = pyrfc.Connection(**conn_params)

        # RFC 함수 호출, 파라미터 키:값은 이벤트에서 추출
        result = conn.call("Z_MM_POC_MB52", **params)
        ot_result = result.get("OT_RESULT", [])
        df = pd.DataFrame(ot_result)
        aw.s3.to_parquet(df=df, path=f"s3://aata-bkt/gluebcket/SAP_RFC/test/Z_MM_POC_MB52/", dataset=True)
        
        # 연결 종료
        conn.close()

        return {"statusCode": 200, "body": result}

    except Exception as e:
        return {"statusCode": 500, "body": str(e)}