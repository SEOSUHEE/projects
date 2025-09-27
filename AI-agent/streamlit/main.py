import streamlit as st
import yaml
import streamlit_authenticator as stauth


def logout():
    st.session_state['authentication_status'] = None
    st.rerun()

# 세션 상태 초기화
if 'authentication_status' not in st.session_state:
    st.session_state['authentication_status'] = None
    st.session_state['name'] = None
    st.session_state['username'] = None

# config.yaml 파일 로드 : 로그인 정보, 해싱된 패스워드
with open('/home/ec2-user/APGenAI/streamlit/yaml/config.yaml') as file:
    config = yaml.load(file, Loader=stauth.SafeLoader)

# 로그인 처리
authenticator = stauth.Authenticate(
    config['credentials'],
    config['cookie']['name'],
    config['cookie']['key'],
    config['cookie']['expiry_days']
)

# 로그인 시도
name, authentication_status, username = authenticator.login("Login","main")

# 로그인 상태에 따라 처리
if authentication_status == None:
    st.warning("📢  발급받은 사용자명과 암호를 입력하세요.")

elif authentication_status == False:
    st.warning("📢  발급받은 사용자명과 암호를 확인하세요.")

#로그인이 되었다면 session_state에 로그인 정보 저장
if authentication_status == True:
    st.session_state['authentication_status'] = True
    st.session_state['name'] = name
    st.session_state['username'] = username
    st.title(f"\n")
    st.title(f"\n")
    st.markdown("""---""")
    st.title(f"Hello, {username} 🤖")
    st.title(f"Welcome to Amorepacific AI-agent !!")

    # 로그아웃 버튼 , 로그아웃은 해당 페이지에서만 가능
    st.markdown("""---""")

    if st.button("LOGOUT"):
        logout()
