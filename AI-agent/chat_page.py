import io
import boto3
import streamlit as st
import warnings
warnings.filterwarnings("ignore")
import pandas as pd
import requests
import json
from datetime import datetime, timedelta, timezone
import traceback
from parameter.chat_parameters import *

def get_korea_time():
    # UTC 시간.
    utc_time = datetime.now(timezone.utc)
    # 한국 시간대 (+09:00)을 생성
    korea_offset = timedelta(hours=9)
    # UTC 시간에 한국 시간 더함
    korea_time = utc_time + korea_offset
    return korea_time


##질의를 (API-http post method) json 형태로 변환하기 위해
def convert_sets_to_lists(d):
    '''
    이슈 생성을 위해 입력 받은 값들(=set)를 처리 후, 최종적으로 딕셔너리를 반환
    Jira 티켓 형식 유지하는 것이 목적
    Args:
        d Dict{{set}}
    Returns:
        d Dict
    '''
    if isinstance(d, dict):  # 딕셔너리일 경우
        return {k: convert_sets_to_lists(v) for k, v in d.items()}
    elif isinstance(d, set):  # set일 경우
        # set을 list로 변환하고, 만약 list에 값이 하나만 있으면 그 값을 그대로 반환
        list_value = list(d)
        return list_value[0] if len(list_value) == 1 else list_value
    elif isinstance(d, list):  # list일 경우
        # 리스트 내 값이 하나일 경우 그 값을 그대로 반환
        if len(d) == 1:
            return d[0]
        return [convert_sets_to_lists(item) for item in d]
    else:
        return d

## 생성한 이슈에 대한 로그 생성
def make_log(id, user, title, description, answer):
   '''
   일자별 streamlit_ticket_log 파일에 이슈와 답변을 로그성으로 적재
   Args:
       id
       user
       title
       description
       answer
   Returns:
   '''
   log_bucket = s3_log_bucket 
   log_s3_path = "ticket_log"
   obj_list = s3_client.list_objects(Bucket=log_bucket, Prefix=log_s3_path)
   log_file_list = [obj['Key'] for obj in obj_list['Contents']]

   # 오늘 일자의 폴더명
   today_log_path = f"ticket_log/{yyyymmdd}/streamlit_ticket_log.csv"
   columns_name = ['TIME', 'ID', '이슈 생성자', '이슈 제목', '이슈 설명', '이슈 답변']

   # 경로 참조 = s3://apdev-genai-data/raw/confluence/GI7-3 신청서 양식 및 가이드 모음 파일/
   if today_log_path in log_file_list:
       new_data = {'TIME': [current_time], 'ID': [id], '이슈 생성자': [user], '이슈 제목': [title], '이슈 설명': [description],'이슈 답변': [answer]}
       new_df = pd.DataFrame(new_data)

       response = s3_client.get_object(Bucket=log_bucket, Key=today_log_path)
       s3_df = pd.read_csv(io.BytesIO(response['Body'].read()),sep = "|")
       s3_final_df = pd.concat([s3_df, new_df])

       csv_buffer = io.StringIO()
       s3_final_df.to_csv(csv_buffer, sep='|', index=False, encoding='utf-8')
       s3_client.put_object(Bucket=log_bucket, Key=f'{today_log_path}', Body=csv_buffer.getvalue())

   else:
       new_data = {'TIME': [current_time], 'ID': [id], '이슈 생성자': [user], '이슈 제목': [title], '이슈 설명': [description],
                   '이슈 답변': [answer]}
       new_df = pd.DataFrame(new_data)

       csv_buffer = io.StringIO()
       new_df.to_csv(csv_buffer, sep='|', index=False, encoding='utf-8')  # sep 인자 추가
       s3_client.put_object(Bucket=log_bucket, Key=f'{today_log_path}', Body=csv_buffer.getvalue())


## 답변에 참조해야 할 파일이 있는 경우 -> s3에서 파일 가져오는 함수
def get_file(response_file):
    '''
       AI-agent 답변에 첨부파일이 포함되어 있을 시에 해당 파일이 s3 내에 있는지 확인 후 답변에 포함
       Args:
           response_file[List]
       Returns:
           download_list[List]
    '''
    source_bucket = s3_data_bucket
    pdf_s3_path = "raw/conflu"
    add_s3_path = "raw/conflu/신청서 양식 및 가이드 모음 파일"
    download_list = []

    for i in response_file:
        if 'pdf' in i:
            pdf_file_list = f'{pdf_s3_path}/{i}'
            obj_list = s3_client.list_objects(Bucket=source_bucket, Prefix=pdf_s3_path)
            full_name_file_list = [obj['Key'] for obj in obj_list['Contents']]

            add_file_list = f'{add_s3_path}/{i}'
            object_list = s3_client.list_objects(Bucket=source_bucket, Prefix=add_s3_path)
            add_full_name_file_list = [obj['Key'] for obj in object_list['Contents']]

            if pdf_file_list in full_name_file_list:
                # 다운로드 생략 : s3_client.download_file(source_bucket, f'{pdf_s3_path}/{i}', f"/home/ec2-user/APGenAI/streamlit/tmp/{i}")
                download_list.append(f"{i}")
            elif add_file_list in add_full_name_file_list:
                # s3_client.download_file(source_bucket, f"{add_s3_path}/{i}", f"/home/ec2-user/APGenAI/streamlit/tmp/{i}")
                download_list.append(f"{i}")
    return download_list

st.set_page_config(layout="wide")

## 로그인 상태 확인
if "authentication_status" not in st.session_state or not st.session_state["authentication_status"]:
    st.warning("📢 Please log in first.")
    st.stop()

korea_time = get_korea_time()
current_time = korea_time.strftime("%Y-%m-%d %H:%M")
yyyymmdd = korea_time.strftime("%Y%m%d")

s3_client = boto3.client('s3')

## admin 용 사이드 바 : s3 리소스를 화면상에서 공유 -> s3, region, bedrock
if st.session_state["username"] == 'admin' or st.session_state["username"] == 'ai-admin':
    name = st.session_state["name"]
    username = st.session_state["username"]

    ## 파라미터 경로 하드코딩
    params_path = "/home/ec2-user/APGenAI/streamlit/parameter/chat_parameters.json"
    with open(params_path, "r", encoding='UTF-8-sig') as json_file:
        json_data = json.load(json_file)

    region = json_data["region"]
    bedrock_region = json_data['bedrock_region']
    bedrock_endpoint_url = json_data['bedrock_endpoint_url']
    bedrock_model_id = json_data["bedrock_model_id"]
    embedding_model_id = json_data["embedding_model_id"]  
    model_params = json_data['model_params']
    s3_bucket = json_data["s3_bucket"]

    button_nums = ['AI-agent', 'Chatbot']

    with st.sidebar:
        mode = st.sidebar.radio('Select Mode', button_nums)
        st.markdown("""---""")

        st.sidebar.write('AWS 리소스 정보')
        col1, col2 = st.sidebar.columns(2)
        with col1:
            set_region = st.text_input(label="Region", value=region)
        with col2:
            set_s3_bucket = st.text_input(label="S3 bucket", value=s3_bucket)

        st.markdown("""---""")
        st.sidebar.write('Bedrock Model 정보')
        set_bedrock_region = st.sidebar.text_input(label="Bedrock Region", value=bedrock_region)
        set_bedrock_endpoint_url = st.sidebar.text_input(label="Bedrock Endpoint URL", value=bedrock_endpoint_url)
        set_model_id = st.sidebar.text_input(label="Bedrock Model ID", value=bedrock_model_id)
        set_embedding_model_id = st.sidebar.text_input(label="Bedrock Embedding Model ID", value=embedding_model_id)

        col7, col8 = st.sidebar.columns(2)
        with col7:
            set_max_tokens = st.text_input(label="Max tokens", value=model_params['max_tokens'])
        with col8:
            set_temperature = st.text_input(label="Temperature", value=model_params['temperature'])

else:
    name = st.session_state["name"]
    username = st.session_state["username"]

    button_nums = ['AI-agent', 'Chatbot']
    with st.sidebar:
        mode = st.sidebar.radio('Select Mode', button_nums)
        st.markdown("""---""")

# 메인 화면
st.title(" AI-agent for IT Helpdesk")
st.write(f"📢 {username} 님 환영합니다! 문의를 남겨주세요.")

# 입력 양식 : 권한 상관없이 공통
download_list = []

try:
    if mode == button_nums[0]:
        if 'key' not in st.session_state:
            st.session_state['key'] = 'value'
        # form에 입력 받는 양식 작성
        with st.form("issue_form", clear_on_submit=True):
            st.markdown("### 1. 이슈 유형")
            issue_type = st.selectbox("이슈 유형은 변경할 수 없습니다", ["인프라 문의"])

            st.markdown("### 2. 업무 유형")
            working_type = st.selectbox("업무 유형 선택", ["기타", "서버", "DB", "보안", "OpenSearch"])

            st.markdown("### 3. 이슈 제목")
            issue_title = st.text_input("이슈 제목 입력")

            st.markdown("### 4. 이슈 내용")
            issue_content = st.text_area("이슈 내용 입력", height=150)

            # 제출 버튼
            submitted_btn = st.form_submit_button("이슈 제출")

            if submitted_btn:
                user_message = f"""  ‼️️‼️ 이슈 생성 완료 ‼️‼️ \n\n - 이슈 유형: {issue_type}\n - 업무 유형: {working_type}\n - 이슈 제목: {issue_title}\n - 이슈 내용: {issue_content}\n """
                # 메시지 session_state[key]에 저장
                st.session_state['key'] = f"{user_message}"

                # user 이슈 생성 완료 메시지 표시
                with st.chat_message("user"):
                    st.markdown(user_message)
                # post_data는 ai-agent에 post 요청을 보내는 형식 구성
                with st.spinner("답변 생성 중입니다...."):
                    post_data = {
                        'fields': {
                            'issuetype': {
                                'name': {issue_title}
                            },
                            'description': {issue_content},
                            'customfield_18505': {working_type},
                            'summary': {issue_title},
                            'assignee': {
                                'displayName': '서수희/메가존'
                            },
                            'reporter': {
                                'displayName': '서수희/메가존'
                            },
                            'created': '1735774655000'
                        },
                        'key': 'TECMSITSM-354'  # 정해진 키값으로 진행
                    }
                    post_list = convert_sets_to_lists(post_data)
                    print("========= User Question =========")
                    print(post_list)

                    url = 'http://localhost:9002/streamlit/answer/'
                    st.session_state['post_list'] = post_list
                    
                    response = requests.post(url, data=json.dumps(post_list), timeout=300)
                    response_data = response.json()
                    file_name = response_data.get('file_name')

                    response_result = response_data.get('rag_result')
                    print("========= RAG Result =========")
                    print(response_result)

                    if response_result == "CANNOT ANSWER":
                        response_result = 'Agent에서 답변할 수 없는 유형의 티켓입니다. 운영자들 확인이 필요합니다.'
                        with st.chat_message("assistant"):
                            st.markdown(response_result)
                    else:
                        response_result = response_data.get('rag_result')

                        if len(file_name) > 0:
                            with st.chat_message("assistant"):
                                st.markdown(f"{response_result}\n- 가이드 문서 : {response_data.get('file_name')} ",unsafe_allow_html=True)
                        else:
                            with st.chat_message("assistant"):
                                st.markdown(f"{response_result}")
                # 로그 적재
                make_log(f'{username}', f'{username}', f'{issue_title}', f'{issue_content}', f'{response_result}')
    else:
        # 세션 상태 초기화
        if "messages" not in st.session_state:
            st.session_state["messages"] = [
                {"role": "assistant", "content": f"안녕하세요! {username}님, 인프라 문의사항을 아래에 남겨주세요."}
            ]

        # 이전 메시지 표시
        for msg in st.session_state["messages"]:
            if msg["role"] == "user":
                with st.chat_message("user"):
                    st.markdown(msg["content"])
            else:
                with st.chat_message("assistant"):
                    st.markdown(msg["content"])

        # 사용자 입력 받기
        if prompt := st.chat_input("질문을 입력해 주세요"):
            # 사용자 메시지 표시 및 세션 상태에 추가
            st.session_state["messages"].append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)

            # Assistant의 응답 처리
            obj = {'user_prompt': prompt}
            headers = {'Content-Type': 'application/json'}

            print("========= User Question =========")
            print(prompt)
            
            with st.spinner('답변 생성 중입니다...'):
                # API 요청
                response = requests.post('http://localhost:9002/streamlit/chat', json=json.dumps(obj), headers=headers)
                return_value = response.json()  # JSON 응답 파싱
                
                # Assistant의 응답 표시 및 세션 상태에 추가
                if return_value['rag_result'] == 'CANNOT ANSWER':
                    assistant_response = 'Agent에서 답변할 수 없는 유형의 티켓입니다. 운영자들 확인이 필요합니다.'
                else:
                    assistant_response = return_value['rag_result']

                # Assistant 메시지 추가 및 표시
                print("========= RAG Result =========")
                print(return_value['rag_result'])

                st.session_state["messages"].append({"role": "assistant", "content": assistant_response})
                with st.chat_message("assistant"):
                    st.markdown(assistant_response)

                make_log(f'{username}', f'{username}', ' ', f'{prompt}', f'{assistant_response}')

except Exception as e:
    st.write('📢 error > Agent 프로젝트 팀에게 문의하세요')
    print("----------------------------------------")
    print(f'{e}')
    print(traceback.format_exc)