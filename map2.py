import streamlit as st
import folium
import requests
from pandas import json_normalize
from streamlit_folium import st_folium
from PIL import Image

# Streamlit 앱 제목 설정
st.title("나의 원룸 조건")
img = Image.open('./images2/room.jpg')


# 2개의 열(컬럼)을 만듭니다.
# 첫 번째 열은 1의 너비를, 두 번째 열은 4의 너비를 가집니다. (1:4 비율)
col1, col2 = st.columns([1, 2])

 
# 첫 번째 열(왼쪽)에 '사이드바' 내용 넣기
with col1:
    st.header("매물 정보")

    st.subheader("방사진")
    st.image(img, width=200, caption='room') # 이미지 너비도 여기서 조절 가능

    st.subheader("주소")
    st.write("영등포구 달빛로 145-14 삼동빌라 502호")

    st.subheader("면적")
    st.write("25m²")

    st.subheader("보증금")
    score= st.slider('내 점수 선택', 0, 2000,100)
    st.text(f'score: {score}')

    st.subheader("월세")
    st.write("40만 원")

    st.subheader("관리비")
    st.write("5만 원")


# 두 번째 열(오른쪽)에 지도 내용 넣기
with col2:
    loc = [37.5662952, 126.9779451]
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

    m = folium.Map(location=loc, zoom_start=12)

    for index, row in st_df_map.iterrows():
        popup_text = f"{row['s_name']}"
        folium.Marker(
            location=[row['lat'], row['lot']],
            popup=folium.Popup(popup_text, max_width=300),
            tooltip=row['s_name'],
            icon=folium.Icon(color='green', icon='coffee', prefix='fa')
        ).add_to(m)

    st_folium(m, width=1500, height=800)