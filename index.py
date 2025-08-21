import streamlit as st
import base64
from PIL import Image

# --- Streamlit 앱의 기본 설정 ---
st.set_page_config(layout="centered", page_title="방구 - 사회 초년생의 집 찾기")

img = Image.open('./images2/cat.jpg')



with st.container():
    # 이미지 표시 (Streamlit은 이미지 크기를 자동으로 조절합니다)
    st.image(img, width=300, caption='cat')
    
    # 제목
    st.title("방구")

    # 부제
    st.markdown("### 사회 초년생의 방구하기!")

    st.markdown("---") # 수평선 추가


    # 해당 스크립트를 새 탭에서 열도록 유도할 수 있습니다.
    st.link_button("지금 시작하기", url="http://localhost:8501/filters", help="매물 조건 입력 페이지로 이동합니다.")