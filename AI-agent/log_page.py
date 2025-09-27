import os, sys, shutil, re, time
import boto3
import botocore
import streamlit as st
import streamlit_authenticator as stauth
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
from datetime import datetime, timedelta, timezone
import traceback
import io

def get_korea_time():
    # UTC 시간.
    utc_time = datetime.now(timezone.utc)
    # 한국 시간대 (+09:00)을 생성
    korea_offset = timedelta(hours=9)
    # UTC 시간에 한국 시간 더함
    korea_time = utc_time + korea_offset
    return korea_time


st.set_page_config(layout="wide")

# 로그인 상태 확인
if "authentication_status" not in st.session_state or not st.session_state["authentication_status"]:
    st.warning("📢 Please log in first.")
    st.stop()

else:
    username = st.session_state["username"]
    print("---------------로그인 성공-------------------------")
    print(username)

if st.session_state["username"] == 'admin' or st.session_state["username"] == 'ai-admin':
    # 로그인 상태 유지
    name = st.session_state["name"]
    username = st.session_state["username"]
    korea_time = get_korea_time()
    yyyymmdd = korea_time.strftime("%Y%m%d")

    try:
        st.title(f"관리자 님의 활동 이력")
        st.markdown('--------------------------------------------')
        st.markdown('️1️⃣ 이슈 생성 이력')

        final_ticket_list = []
        download_list = []

        source_bucket = "apdev-genai-log"
        ticket_prefix_s3_path = "ticket_log"

        s3_client = boto3.client('s3')
        obj_list = s3_client.list_objects(Bucket=source_bucket, Prefix=ticket_prefix_s3_path)
        full_name_file_list = [obj['Key'] for obj in obj_list['Contents']]
        filtered_files = [file for file in full_name_file_list if file.startswith("ticket_log/2025")]


        for i in filtered_files:
            response = s3_client.get_object(Bucket=source_bucket, Key=i)
            df = pd.read_csv(io.BytesIO(response['Body'].read()), sep="|")
            filtered_df = df[(df['ID'] == 'admin') | (df['ID'] == 'ai-admin')]
            again_df = filtered_df[['TIME', '이슈 생성자', '이슈 제목', '이슈 설명', '이슈 답변']]
            final_ticket_list.append(again_df)

        final_df = pd.concat(final_ticket_list)
        sorted_df = final_df.sort_values(by='TIME', ascending=False)
        st.dataframe(sorted_df, height=300, width=2000, hide_index=True)

        #################################################################################################

        st.markdown('--------------------------------------------')
        st.markdown('2️⃣ 인덱스 수정/롤백 이력')

        final_index_list = []
        index_prefix_s3_path = "index_log/"

        index_obj_list = s3_client.list_objects(Bucket=source_bucket, Prefix=index_prefix_s3_path)
        full_name_index_list = [obj['Key'] for obj in index_obj_list['Contents']]
        filtered_index_files = [file for file in full_name_index_list if file.startswith("index_log/")]


        #for i in full_name_index_list:
        for i in filtered_index_files:
            index_response = s3_client.get_object(Bucket=source_bucket, Key=i)
            index_df = pd.read_csv(io.BytesIO(index_response['Body'].read()), sep="|")

            filtered_index_df = index_df[(index_df['ID'] == 'admin') | (index_df['ID'] == 'ai-admin')]
            again_index_df = filtered_index_df[['TIME', 'ID', '인덱스명', '작업명', '파일명']]
            final_index_list.append(again_index_df)

        final_index_df = pd.concat(final_index_list)
        sorted_index_df = final_index_df.sort_values(by= 'TIME', ascending=False)
        st.dataframe(sorted_index_df, height=300, width=2000, hide_index=True)


    except Exception as e:
        st.write('📢 error > Agent 프로젝트 팀에게 문의하세요')
        print("----------------------------------------")
        print(f'{e}')
        err_str = traceback.format_exc()  # 스택 트레이스를 문자열로 반환
        print(err_str)

else:
    st.title("해당 페이지를 사용할 권한이 없습니다")