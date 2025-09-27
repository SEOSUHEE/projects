# 아이디, 패스워드를 기준으로 스트림릿에 로그인할 수 있는 yaml 파일 생성하기
import yaml
import streamlit_authenticator as stauth

usernames = ["admin",  "user1", "user2"]  # 아이디
passwords = ["Abcd123!", "123QWE!@#", "qaz!@#123"]  # 비밀번호
names = ["관리자1", "관리자2", "사용자1", "사용자2"] #닉네임

hashed_passwords = stauth.Hasher(passwords).generate()  # 비밀번호 해싱(암호화)

data = {
    "credentials": {
        "usernames": {
            usernames[0]: {
                "name": names[0],
                "password": hashed_passwords[0]
            },
            usernames[1]: {
                "name": names[1],
                "password": hashed_passwords[1]
            },
            usernames[2]: {
                "name": names[2],
                "password": hashed_passwords[2]
            },
            usernames[3]: {
                "name": names[3],
                "password": hashed_passwords[3]
            }
        }
    },
    "cookie": {
        "expiry_days": 0,  # 만료일, 재인증 기능 필요없으면 0으로 세팅
        "key": "some_signature_key",
        "name": "some_cookie_name"
    }
}

#yaml 파일로 생성
with open('/home/ec2-user/APGenAI/streamlit/yaml/config.yaml', 'w') as file:
    yaml.dump(data, file, default_flow_style=False)