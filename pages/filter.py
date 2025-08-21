import streamlit as st
import base64


st.set_page_config(layout="centered", page_title="매물 조건 입력")

# --- Streamlit UI 구성 ---

with st.container(border=True):
    # 페이지 제목
    st.title("매물 조건 입력")

    # --- 입력 폼 ---
    with st.form(key='property_filters_form'):
        st.header("지역")
        region = st.text_input("지역을 입력하세요", placeholder="예: 서울 강남구")

        st.header("가격 정보")
        # 보증금
        deposit_options = {
            "선택 안함": None,
            "~500만원": 500,
            "~1,000만원": 1000,
            "~2,000만원": 2000,
            "~3,000만원": 3000,
            "~5,000만원": 5000,
            "~1억원": 10000,
            "1억원 이상": 10001,
        }
        deposit_selection = st.selectbox("보증금", list(deposit_options.keys()))

        # 월세
        rent_options = {
            "선택 안함": None,
            "~30만원": 30,
            "~40만원": 40,
            "~50만원": 50,
            "~60만원": 60,
            "~70만원": 70,
            "~80만원": 80,
            "~90만원": 90,
            "~100만원": 100,
            "100만원 이상": 101,
        }
        rent_selection = st.selectbox("월세", list(rent_options.keys()))

        st.header("매물 상세 조건")
        # 평수
        area_options = {
            "선택 안함": None,
            "~5평": 5,
            "~10평": 10,
            "~15평": 15,
            "~20평": 20,
            "~25평": 25,
            "~30평": 30,
            "30평 이상": 31,
        }
        area_selection = st.selectbox("평수", list(area_options.keys()))

        # 방 개수
        rooms = st.number_input("방 개수", min_value=0, step=1, placeholder="예: 1")

        # --- 버튼 그룹 ---
        col1, col2 = st.columns(2)
        with col1:
            st.link_button("상세 필터 (기타 옵션)", url="http://example.com", help="더 자세한 조건을 설정합니다.") # 실제 url을 넣어주세요
        with col2:
            submit_button = st.form_submit_button("매물 찾기")

    # 폼 제출 로직
    if submit_button:
        # 여기에 사용자가 입력한 값들을 처리하는 로직을 추가합니다.
        # 예를 들어, 이 값들을 백엔드 API로 전송하거나,
        # 필터링된 결과를 다른 페이지에 표시할 수 있습니다.
        
        # 보증금, 월세, 평수 값을 숫자형으로 변환
        deposit_value = deposit_options[deposit_selection]
        rent_value = rent_options[rent_selection]
        area_value = area_options[area_selection]
        
        st.success("매물 찾기 조건이 제출되었습니다.")
        st.write("---")
        st.subheader("입력하신 조건:")
        st.write(f"**지역**: {region}")
        st.write(f"**보증금**: {deposit_selection} ({deposit_value}만원)")
        st.write(f"**월세**: {rent_selection} ({rent_value}만원)")
        st.write(f"**평수**: {area_selection} ({area_value}평)")
        st.write(f"**방 개수**: {rooms}개")
        
        


st.set_page_config(layout="wide", page_title="매물 상세 필터")

main_col, filter_col = st.columns([4, 1])


# 이렇게 하면 필터가 오른쪽에 위치하게 됩니다.
main_col, filter_col = st.columns([2, 1])
        
with st.sidebar:
    st.header("매물 상세 필터 🔍")
    
    # 세부 옵션만 포함된 체크박스 필터
    with st.expander("세부 옵션"):
        st.subheader("필수 옵션")
        options = ["에어컨", "세탁기", "건조기", "냉장고", "인덕션", "전자레인지", "침대", "옷장", "책상"]
        selected_options = st.multiselect("필수 옵션을 선택하세요", options)

        st.subheader("보안/안전")
        security_options = ["CCTV", "방범창", "도어락"]
        selected_security = st.multiselect("보안/안전 옵션을 선택하세요", security_options)

        st.subheader("친환경/웰빙")
        environment_options = ["공기청정기", "층간소음방지"]
        selected_environment = st.multiselect("친환경/웰빙 옵션을 선택하세요", environment_options)

        st.subheader("주차/교통")
        parking_options = ["주차 가능", "역세권"]
        selected_parking = st.multiselect("주차/교통 옵션을 선택하세요", parking_options)

    st.write("---")
    
# 매물 찾기 버튼