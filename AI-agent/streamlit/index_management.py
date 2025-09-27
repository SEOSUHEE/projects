import os, sys, re, time
import io
import json
import pandas as pd
import boto3
import botocore
import streamlit as st
from opensearchpy import OpenSearch, helpers, RequestsHttpConnection
from datetime import timedelta, datetime, timezone
import time
from langchain_community.embeddings import BedrockEmbeddings
import uuid

from parameter.chat_parameters import *

# 새로고침 : streamlit session 초기화 함수
def clear_streamlit_cache():
    '''
    streamlit session에 저장된 key-value 들 삭제
    '''
    keys_to_delete = ["modified_df", "sorted_df", "file_to_edit", "show_df", "editor", "edited_df",  "edited_rows", "modified_columns"]
    for key in keys_to_delete:
        if key in st.session_state:
            st.session_state.pop(key)

def get_korea_time():
    # UTC 시간.
    utc_time = datetime.now(timezone.utc)
    # 한국 시간대 (+09:00)을 생성
    korea_offset = timedelta(hours=9)
    # UTC 시간에 한국 시간 더함
    korea_time = utc_time + korea_offset
    return korea_time

# opensearch에서 인덱스 내 데이터 가져옴
@st.cache_data
def making_origin_df(selected_index):
    '''
    인덱스가 선택되어지면 선택된 인덱스의 원본데이터를 불러옴
    이때, 해당 페이지에서 필요한 컬럼만을 가져오도록 하드코딩 -> 수정할 컬럼만 최소로
    Args:
        selected_index
    Return:
        df(DataFrame)
    '''
    search_query = {
        "size": 100,
        "query": {
            "match_all": {}
        }
    }
    # 클라이언트 호출하여 데이터 추출, 스크롤 아이디(식별자)로 후속 검색 지속 진행
    # 1초로 설정하여 검색 컨텍스트 유지
    results = os_client.search(index=f'{selected_index}', body=search_query, scroll='1s')
    scroll_id = results['_scroll_id']
    hits = results['hits']['hits']  # 검색된 문서들

    data = []
    # hits에서 '_source' = 데이터이고, 데이터를 DataFrame으로 변환
    for hit in hits:
        # 필요한 컬럼 : file_name,  description, page_text, summary_text, id
        id = hit['_id']
        file_name = hit['_source']['metadata']['file_name']
        description = hit['_source']['description']
        page_text = hit['_source']['page_text']
        page_vector = hit['_source']['page_vector']
        summary_text = hit['_source']['summary_text']
        summary_vector = hit['_source']['summary_vector']
        row = {
            "id": id,
            "file_name": file_name,
            "page_text": page_text,
            "summary_text": summary_text,
            "page_vector": page_vector,
            "summary_vector": summary_vector,
            "description": description
        }
        data.append(row)
    df = pd.DataFrame(data=data, index=None)

    # 추가 데이터가 있을 경우, scroll을 통해 계속 가져오기
    while len(hits) > 0:
        results = os_client.scroll(scroll_id=scroll_id, scroll='1s')
        scroll_id = results['_scroll_id']
        hits = results['hits']['hits']
        if len(hits) > 0:
            id = hit['_id']
            file_name = hit['_source']['metadata']['file_name']
            description = hit['_source']['description']
            page_text = hit['_source']['page_text']
            page_vector = hit['_source']['page_vector']
            summary_text = hit['_source']['summary_text']
            summary_vector = hit['_source']['summary_vector']
            row = {
                "id": id,
                "file_name": file_name,
                "page_text": page_text,
                "summary_text": summary_text,
                "page_vector": page_vector,
                "summary_vector": summary_vector,
                "description": description
            }
            data.append(row)
            df = pd.concat([df, pd.DataFrame(data=data, index=None)])
    return df

# 수정된 데이터 벡터화 진행, 임베딩 모델 호출 : text to vector
def generate_text_embedding(text):
    '''
    bedrock 임베딩 모델을 호출하여 수정된 데이터에 대한 벡터값 생성
    Args:
        text
    return:
        vector[List]
    '''
    bedrock_embeddings_client = BedrockEmbeddings(
        region_name=bedrock_region,
        endpoint_url=bedrock_endpoint_url,
        model_id=embedding_model_id)
    vector = bedrock_embeddings_client.embed_query(text)
    return vector

#로컬에 로그 저장하지 않음
def make_log(id, index, file, current_time, action):
    '''
    인덱스 수정 혹은 롤백을 진행했을 때 로그 생성하는 함수
    Args:
        id
        index
        file
        current_time
        action
    Returns:
    '''
    korea_time = get_korea_time()
    current_time = korea_time.strftime("%Y-%m-%d %H:%M")
    yyyymmdd = korea_time.strftime("%Y%m%d")

    log_bucket = s3_log_bucket
    log_s3_path = "index_log"
    obj_list = s3_client.list_objects(Bucket=log_bucket, Prefix=log_s3_path)
    log_file_list = [obj['Key'] for obj in obj_list['Contents']]

    # 오늘 일자의 폴더명
    today_object_key = f"index_log/{yyyymmdd}/streamlit_index_log.csv"

    if today_object_key in log_file_list:
        new_data = {'TIME': [current_time], 'ID': [id], '인덱스명': [index], '작업명': [action], '파일명': [file]}
        new_df = pd.DataFrame(new_data)
        
        response = s3_client.get_object(Bucket=log_bucket, Key=today_object_key)
        s3_df = pd.read_csv(io.BytesIO(response['Body'].read()),sep = "|")

        s3_final_df = pd.concat([s3_df, new_df])
        
        csv_buffer = io.StringIO()
        s3_final_df.to_csv(csv_buffer, sep='|', index=False, encoding='utf-8')
        s3_client.put_object(Bucket=log_bucket, Key=today_object_key, Body=csv_buffer.getvalue())
    else:
        new_data = {'TIME': [current_time], 'ID': [id], '인덱스명': [index], '작업명': [action], '파일명': [file]}
        new_df = pd.DataFrame(new_data)
        csv_buffer = io.StringIO()
        new_df.to_csv(csv_buffer, sep='|', index=False, encoding='utf-8')
        s3_client.put_object(Bucket=log_bucket, Key=today_object_key, Body=csv_buffer.getvalue())


# data_editor key에 uuid 업데이트 하여 key값 적용하는 함수
def update():
    st.session_state["dek"] = str(uuid.uuid4())

########################################시작#####################################################3

st.set_page_config(layout="wide")

## 로그인 체크
if "authentication_status" not in st.session_state or not st.session_state["authentication_status"]:
    st.warning("📢 Please log in first.")
    st.stop()

## admin 로그인, AWS 리소스 정보 확인 가능
if st.session_state["username"] == 'admin' or st.session_state["username"] == 'ai-admin':
    name = st.session_state["name"]
    username = st.session_state["username"]
    print("---------------로그인 성공-------------------------")
    print(username)
    print(st.session_state)

    ## 파라미터 경로 하드코딩해서 정보 추출
    params_path = "/home/ec2-user/APGenAI/streamlit/parameter/chat_parameters.json"
    with open(params_path, "r", encoding='UTF-8-sig') as json_file:
        json_data = json.load(json_file)

    region = json_data["region"]
    os_url = json_data["os_url"]
    os_id = json_data['os_id']
    os_pw = json_data['os_pw']
    idx_k = json_data["idx_k"]
    bedrock_region = json_data['bedrock_region']
    bedrock_endpoint_url = json_data['bedrock_endpoint_url']
    bedrock_model_id = json_data["bedrock_model_id"]
    embedding_model_id = json_data["embedding_model_id"]  #
    model_params = json_data['model_params']
    s3_bucket = json_data["s3_bucket"]

    # S3, opensearch 클라이언트 생성
    s3_client = boto3.client('s3', region_name=region)
    os_client = OpenSearch(
        hosts=[{'host': os_url, 'port': 443}],
        http_auth=(os_id, os_pw),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
        timeout=60,
        config=botocore.config.Config(
            read_timeout=900,
            connect_timeout=900,
            retries={"max_attempts": 0}
        )
    )

    # 페이지 구성 시작
    st.title('Opensearch INDEX 데이터 확인')
    # 인덱스 선택 세션 선언
    # 제일 처음
    if "selected_index" not in st.session_state:
        st.session_state["selected_index"] = None

    indices = os_client.indices.get_alias("*apcms*")  # opensearch에서 인덱스 목록 추출
    index_list = list(indices.keys())
    # 인덱스 선택 기능 -> 선택 후 session_state에 저장
    selected_index = st.selectbox("choice one of index", options=["인덱스를 선택하세요"] + index_list, index=0)
    st.session_state["selected_index"] = selected_index

    # 세션에 selected_index있는데 selected_index값이 !=인덱스를 선택하세요가 아닌 경우 => session_state.start
    if "selected_index" in st.session_state and selected_index != "인덱스를 선택하세요":
        st.session_state.start = True
        # st.session_state["selected_index"] = selected_index

        origin_df = making_origin_df(selected_index)  # 선택한 인덱스의 원본 데이터 가져오기
        sorted_df = origin_df[["file_name", "page_text", "summary_text", "description", "id"]].copy().sort_values(
            by=['file_name'], ascending=True).reset_index(drop=True)
        st.session_state["sorted_df"] = sorted_df  # 가져온 원본 데이터 정렬

        grouped_file_names = sorted_df["file_name"].unique().tolist()  # 원본 데이터의 유일한 파일명을 가져옴
        file_to_edit = st.selectbox('choice one of file', options=["파일을 선택하세요"] + grouped_file_names, index=0)
        st.session_state["file_to_edit"] = file_to_edit  # 파일 선택 -> 사용자 필터 기능 생성

        # 제일 처음 페이지 접속 시, st.session_state["dek"] 없으므로
        if 'dek' not in st.session_state:
            st.session_state["dek"] = str(uuid.uuid4())

         # 제일 처음 수정 데이터 저장하는 빈 데이터 프레임 생성
        if "modified_df" not in st.session_state:
            st.session_state["modified_df"] = pd.DataFrame(
                columns=st.session_state["sorted_df"].columns.tolist() + ['modified_columns'], index=None)

        if file_to_edit != "파일을 선택하세요":  # 사용자가 파일명을 선택한 경우
            # 선택한 파일명에 해당하는 행만 필터링하여 화면에 보여줌
            st.session_state["edited_df"] = st.session_state["sorted_df"][st.session_state["sorted_df"]["file_name"] == file_to_edit].copy()
            edited_df = st.session_state["edited_df"]
            edited_df = edited_df.reset_index(drop=True)

            # data editor를 적용 : 데이터 수정 기능
            edited_dfa = st.data_editor(
                edited_df,
                column_order=["file_name", "page_text", "summary_text", 'description'],
                key=st.session_state["dek"],
                use_container_width=True,
                disabled=['file_name'],
                hide_index=True
            )

            # 수정된 값이 세션에 있을때 == len() > 0
            if len(st.session_state[st.session_state["dek"]]["edited_rows"]) > 0:
                for row_num, changes in st.session_state[st.session_state["dek"]]["edited_rows"].items():
                    id_value = edited_df.loc[row_num, 'id']
                    modified_columns = list(changes.keys())
                    st.session_state["modified_columns"] = modified_columns  # 변경된 컬럼명을 추출

                    for col_name, new_value in changes.items():
                        st.session_state["edited_df"].loc[
                            st.session_state["edited_df"]['id'] == id_value, col_name] = new_value
                        original_row = st.session_state["edited_df"][
                            st.session_state["edited_df"]['id'] == id_value].copy()
                        original_row['modified_columns'] = ', '.join(modified_columns)
                        st.session_state["modified_df"] = pd.concat([st.session_state["modified_df"], original_row])

            def highlight_changes(row):
                modified_cols = row['modified_columns'].split(', ')
                return ['background-color: yellow' if col in modified_cols else '' for col in row.index]

            # 수정된 데이터를 쌓는 프레임에 쌓여있을 때만 시각화 진행
            if len(st.session_state["modified_df"]) > 0:
                modified_df = st.session_state["modified_df"].drop_duplicates()
                modified_df = modified_df.drop_duplicates(subset=['id'], keep='last')
                show_df = modified_df.drop(columns=['id']).sort_values(by=['file_name'], ascending=True)
                styled_df = show_df.style.apply(highlight_changes, axis=1)
                st.dataframe(styled_df, hide_index=True, use_container_width=True)

                # 안내 메시지
                st.error("🚨 수정한 내용을 다시 확인 해주세요. 오류가 있다면 REFRESH 버튼 클릭")
                time.sleep(1)
                st.success("✅  수정된 내용을 저장하려면 SAVE 버튼 클릭")

        # 파일을 선택하지 않은 경우 -> 인덱스만 선택하고 파일명으로 필터링 하지 않은 경우
        else:
            edited_dfa = st.data_editor(
                st.session_state["sorted_df"],
                column_order=["file_name", "page_text", "summary_text", 'description'],
                key=st.session_state["dek"],
                use_container_width=True,
                disabled=['file_name'],
                hide_index=True
            )

            # 수정된 값이 세션에 있을때 == len() > 0
            if len(st.session_state[st.session_state["dek"]]["edited_rows"]) > 0:
                for row_num, changes in st.session_state[st.session_state["dek"]]["edited_rows"].items():
                    id_value = sorted_df.loc[row_num, 'id']
                    modified_columns = list(changes.keys())
                    st.session_state["modified_columns"] = modified_columns  # 변경된 컬럼명을 추출

                    for col_name, new_value in changes.items():
                        st.session_state["sorted_df"].loc[
                            st.session_state["sorted_df"]['id'] == id_value, col_name] = new_value
                        original_row = st.session_state["sorted_df"][
                            st.session_state["sorted_df"]['id'] == id_value].copy()
                        original_row['modified_columns'] = ', '.join(modified_columns)
                        st.session_state["modified_df"] = pd.concat([st.session_state["modified_df"], original_row])

            def highlight_changes(row):
                modified_cols = row['modified_columns'].split(', ')
                return ['background-color: yellow' if col in modified_cols else '' for col in row.index]

            # 수정된 데이터를 쌓는 프레임에 쌓여있을 때만 시각화 진행
            if len(st.session_state["modified_df"]) > 0:
                modified_df = st.session_state["modified_df"].drop_duplicates()
                modified_df = modified_df.drop_duplicates(subset=['id'], keep='last')
                show_df = modified_df.drop(columns=['id']).sort_values(by=['file_name'], ascending=True)
                styled_df = show_df.style.apply(highlight_changes, axis=1)
                st.dataframe(styled_df, hide_index=True, use_container_width=True)

                # 안내 메시지
                st.error("🚨 수정한 내용을 다시 확인 해주세요. 오류가 있다면 REFRESH 버튼 클릭")
                time.sleep(1)
                st.success("✅  수정된 내용을 저장하려면 SAVE 버튼 클릭")

        # 새로고침 버튼 : streamlit session 초기화
        if st.button("REFRESH", type="secondary", use_container_width=True, on_click=update):
            print("--------------- 새로 고침 버튼 클릭 시-> uuid 새로 생성하는 함수 실행됨 ------------------------")
            clear_streamlit_cache()  # 캐시 지우기 함수
            making_origin_df.clear() #원본 데이터 프레임 재호출
            st.rerun()  # 페이지 새로 고침

        # 저장 버튼 : 수정전 원본 데이터 백업 -> 수정 데이터 opensearch에 적용
        if st.button("SAVE", type="secondary", use_container_width=True, on_click=update):
            if st.session_state["modified_df"].empty:
                st.error("수정된 데이터가 없습니다...")
                time.sleep(1)
            else:
                with st.spinner("수정 전 데이터를 백업 중입니다..."):
                    # st.session_state["modified_df"] = modified_df
                    modified_df = modified_df.drop_duplicates().reset_index(drop=True)  # 중복 제거 한번 더
                    # 백업용 원본 데이터 프레임 추출
                    backup_df = origin_df[origin_df["id"].isin(modified_df["id"].tolist())].copy()

                    korea_time = get_korea_time()
                    current_time = korea_time.strftime("%Y-%m-%d %H:%M")
                    backup_file = f'{selected_index}_backup_{korea_time}.csv'  # 백업용 파일
                    st.session_state["modified_df"] = modified_df
                    modified_df["last_modified"] = current_time  # 수정할 데이터 프레임

                    # s3에 인덱스별로 백업 파일 따로 보관, 백업은 로컬에 저장하지 않고 오직 s3에만 저장됨
                    st.session_state["backup_df"] = backup_df
                    csv_buffer = io.StringIO()
                    backup_df.to_csv(csv_buffer, sep="|", index=False, encoding='utf-8')
                    s3_client.put_object(Bucket=s3_log_bucket, Key=f"streamlit_index_backup/{selected_index}/{backup_file}", Body=csv_buffer.getvalue())
                    
                    st.success(f"백업 완료 : {backup_file}")
                    print(f"--------------- 백업 완료 : {backup_file}  ------------------------")

                with st.spinner("수정 사항을 적용 중입니다..."):  # 수정 데이터 opensearch에 반영
                    for _, row in modified_df.iterrows():
                        doc_id = row["id"]  # 필터링된 데이터에서 id 컬럼 사용
                        document = {
                            'summary_text': row["summary_text"],
                            'summary_vector': generate_text_embedding(row["summary_text"]),
                            'page_text': row["page_text"],
                            'page_vector': generate_text_embedding(row["page_text"]),
                            'description': row["description"],
                            'metadata': {'last_modified': row["last_modified"]}
                        }
                        response = os_client.update(index=selected_index, id=doc_id, body={"doc": document})

                    st.success(f"수정 완료")
                    print("--------------- opensearch에 수정 완료 ------------------------")

                    # 로그 생성: 인덱스 | 수정한 사용자 | 수정한 파일 | 시간 |
                    korea_time = get_korea_time()
                    current_time = korea_time.strftime("%Y-%m-%d %H:%M")
                    revised_file_name = ', '.join(st.session_state["modified_df"]["file_name"].astype(str).tolist())
                    st.session_state["selected_index"] = selected_index
                    username = st.session_state["username"]
                    make_log(username, selected_index, revised_file_name, current_time, 'Update(수정)')
                    print(f"------ 수정 로그 : {current_time}|{username}|{selected_index}|{revised_file_name} ---------")

                    time.sleep(1) #정상 작동을 위함
                    clear_streamlit_cache()
                    making_origin_df.clear()
                    st.rerun()


        # 수정하기 직전 상태로 돌아가는 기능 : 수정 데이터 저장 전에 백업했던 파일 사용
        if st.button("ROLLBACK", type="secondary", use_container_width=True, on_click=update):
            with st.spinner(f"이전 상태로 돌아가는 중...."):
                Bucket = s3_log_bucket
                path = f'streamlit_index_backup/{selected_index}'
                obj_list = s3_client.list_objects_v2(Bucket=Bucket, Prefix=path)  # 인덱스 별로 보관된 백업 파일 리스트업

                # 백업할 파일 목록이 있는지 확인 : 없다면 else
                if 'Contents' in obj_list:
                    full_name_file_list = sorted([obj['Key'] for obj in obj_list['Contents']], reverse=True)  # 정렬
                    rollback_file = full_name_file_list[0]  # 가장 최근 파일 prifix 정보 가져옴

                    response=s3_client.get_object(Bucket=s3_log_bucket,Key=rollback_file)
                    rollback_df = pd.read_csv(io.BytesIO(response['Body'].read()), sep='|')
                    
                    korea_time = get_korea_time()
                    current_time = korea_time.strftime("%Y-%m-%d %H:%M")
                    rollback_df["last_modified"] = current_time

                    rollback_file_df = rollback_df["file_name"].reset_index(drop=True).tolist()
                    rollback_file_name = ', '.join(rollback_file_df) #롤백하는 파일명 추출

                    for _, row in rollback_df.iterrows():
                        try:
                            doc_id = row["id"]
                            document = {
                                'summary_text': row["summary_text"],
                                'summary_vector': generate_text_embedding(row["summary_text"]),
                                'page_text': row["page_text"],
                                'page_vector': generate_text_embedding(row["page_text"]),
                                'description': row["description"],
                                'metadata': {'last_modified': row["last_modified"]}
                            }
                            response = os_client.update(index=selected_index, id=doc_id, body={"doc": document})
                        except KeyError as e:
                            print(e)

                    s3_client.delete_object(Bucket=Bucket, Key=rollback_file)
                    st.success("이전 상태로 복구 완료")
                    print("--------------- 롤백 완료 ------------------------")

                    selected_index = st.session_state["selected_index"]
                    username = st.session_state["username"]

                    make_log(username, selected_index, rollback_file_name, current_time, 'Rollback(롤백)')
                    print(f"------ 롤백 로그 : {current_time}|{username}|{selected_index}|{rollback_file_name} ---------")

                else:
                    time.sleep(1)
                    st.error("이전 버전이 없습니다...")
                    time.sleep(1)

                time.sleep(1) #정상 작동을 위함
                clear_streamlit_cache()
                making_origin_df.clear()
                st.rerun()

    # session_state에 들어있는 selected_index 가 '인덱스를 선택하세요' 인 경우
    else:
        st.session_state.start = False
        if "sorted_df" in st.session_state:
            st.session_state["sorted_df"] = "tmp"
            del st.session_state["sorted_df"]
        if "file_to_edit" in st.session_state:
            st.session_state["file_to_edit"] = "tmp"
            del st.session_state["file_to_edit"]
        if "edited_df" in st.session_state:
            st.session_state["edited_df"] = "tmp"
            del st.session_state["edited_df"]
        if "modified_df" in st.session_state:
            st.session_state["modified_df"] = "tmp"
            del st.session_state["modified_df"]
        if "editor" in st.session_state:
            st.session_state["editor"] = "tmp"
            del st.session_state["editor"]
        if "modified_columns" in st.session_state:
            st.session_state["modified_columns"] = "tmp"
            del st.session_state["modified_columns"]
        if "edited_rows" in st.session_state:
            st.session_state["edited_rows"] = "tmp"
            del st.session_state["edited_rows"]

# admin이 아닌 경우 -> 인덱스 수정 불가, 단순히 인덱스와 데이터 확인 가능
elif st.session_state["username"] != 'admin' or st.session_state["username"] != 'ai-admin':
    name = st.session_state["name"]
    username = st.session_state["username"]
    print("---------------로그인 성공-------------------------")
    print(username)

    params_path = "/home/ec2-user/APGenAI/streamlit/parameter/chat_parameters.json"
    with open(params_path, "r", encoding='UTF-8-sig') as json_file:
        json_data = json.load(json_file)

    os_url = json_data["os_url"]
    os_id = json_data['os_id']
    os_pw = json_data['os_pw']

    os_client = OpenSearch(
        hosts=[{'host': os_url, 'port': 443}],
        http_auth=(os_id, os_pw),
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
        timeout=60,
        config=botocore.config.Config(
            read_timeout=900,
            connect_timeout=900,
            retries={"max_attempts": 0}
        )
    )

    st.title('Opensearch INDEX 데이터 확인')
    indices = os_client.indices.get_alias("*apcms*")  # 모든 인덱스 리스트 가져오기
    index_list = list(indices.keys())
    selected_index = st.selectbox("choice one of index", options=["인덱스를 선택하세요"] + index_list, index=0)
    st.session_state["selected_index"] = selected_index
    print("---------------인덱스 선택-------------------------")
    print(selected_index)

    if "selected_index" in st.session_state and selected_index != "인덱스를 선택하세요":
        origin_df = making_origin_df(selected_index)  # making_origin_df 함수 호출 -> 선택된 인덱스 데이터 가져옴
        sorted_df = origin_df[["file_name", "page_text", "summary_text", "description", "id"]].copy().sort_values(
            by=['file_name'], ascending=True).reset_index(drop=True)
        st.session_state["sorted_df"] = sorted_df

        st.dataframe(sorted_df)  # 선택된 인덱스 화면에 보여줌

        grouped_file_names = sorted_df["file_name"].unique().tolist()  # 유일한 파일명을 가져옴
        file_to_edit = st.selectbox('choice one of file', options=["파일을 선택하세요"] + grouped_file_names, index=0)
        st.session_state["file_to_edit"] = file_to_edit
        print("---------------파일 선택-------------------------")
        print(file_to_edit)

        if "selected_index" in st.session_state and selected_index != "인덱스를 선택하세요" and file_to_edit != "파일을 선택하세요":
            edited_df = sorted_df[sorted_df["file_name"] == file_to_edit].copy()
            st.session_state["edited_df"] = edited_df
            st.dataframe(edited_df)  # 선택된 인덱스 + 파일명 화면에 보여줌