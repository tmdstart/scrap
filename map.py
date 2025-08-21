import streamlit as st
import folium
import requests
from pandas import json_normalize
from streamlit_folium import st_folium
from PIL import Image
# Streamlit 앱 제목 설정

st.title("나의 원룸 조건")
img = Image.open('./images2/room.jpg')

# 사이드바 시작
with st.sidebar:
    
    
    st.header("매물 정보")

    st.subheader("방사진")
    st.image(img, width=300, caption='room')
    
    
    st.subheader("주소")
    st.write("영등포구 달빛로 145-14 삼동빌라 502호")
    

    st.subheader("면적")
    st.write("25m²")
    
    st.subheader("보증금")
    st.write("2,000만 원")
    
    st.subheader("월세")
    st.write("40만 원")
    
    st.subheader("관리비")
    st.write("5만 원")
    
st.set_page_config(layout="wide")

# 1. 데이터 가져오기 (기존 코드)
loc = [37.5662952, 126.9779451] # 서울시청 위도, 경도
targetSite = 'https://www.starbucks.co.kr/store/getStore.do?r=8K3XF06R9Q'
req = requests.post(targetSite, data={
    'ins_lat': 37.563398,
    'ins_lng': 126.9863309,
    'p_sido_cd': '01',
    'p_gugun_cd': '',
    'in_biz_cd': '',
    'iend': 600,
    'set_date': ''
})
starbucks = req.json()

st_df = json_normalize(starbucks, 'list')
st_df_map = st_df[['s_name', 'lat', 'lot']]
st_df_map['lat'] = st_df_map['lat'].astype(float)
st_df_map['lot'] = st_df_map['lot'].astype(float)

# 서울 중심부를 기준으로 지도 초기화
m = folium.Map(location=loc, zoom_start=12)

for index, row in st_df_map.iterrows():
    popup_text = f"{row['s_name']}"
    folium.Marker(
        location=[row['lat'], row['lot']],
        popup=folium.Popup(popup_text, max_width=300),
        tooltip=row['s_name'],
        icon=folium.Icon(color='green', icon='coffee', prefix='fa')
    ).add_to(m)

# 3. Streamlit에 지도 표시
# st_folium.st_folium() 함수를 사용하여 folium 지도를 Streamlit에 렌더링
st_folium(m, width=1500, height=800)