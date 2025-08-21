import streamlit as st
from PIL import Image
# 사이드바에 위젯 배치하기 sidebar, columns, tabs, expander
st.title("스트림 릿 앱페이지 구성하기")



img = Image.open('./images2/cat.jpg')
img2 = Image.open('./images2/water.jpg')



st.sidebar.header('웰컴메뉴')
selected_menu =st.sidebar.selectbox(
    '메뉴선택', ['메인', '분식', '설정']
)

if selected_menu == '메인':
    st.subheader('*메인페이지')
    st.image(img, width=300, caption='cat')
    st.image(img2, width=300, caption='water') 
    


    # 2. 2개의 열을 생성
    col1, col2 = st.columns(2)

    # 3. 각 열에 이미지 배치  
    with col1:
        st.header("첫 번째 이미지")
        st.image(img, caption="왼쪽 이미지")

    with col2:
        st.header("두 번째 이미지")
        st.image(img2, caption="오른쪽 이미지")

    
    
elif selected_menu =='분석':
    st.subheader('*분석보고서')
else:
    st.subheader('설정변경')
    st.write('앱설정에 성공 ')
    
if st.sidebar.button('선택'):
    st.sidebar.write("선택을 클릭하셨습니다.")

# 슬라이드바 추가 0~100 ,50 


# st.slider('슬라이더 라벨', 최소값, 최대값, 기본값)
slider_value = st.slider('값 선택:', 0, 100, 50)

# 선택된 값을 화면에 표시
st.sidebar.write(f'선택된 값: {slider_value}')




st.divider()

#tab추가
st.header('탭추가')
tab1, tab2, tab3 = st.tabs(['차트', '데이터', '설정'])
with tab1:
    st.subheader('차트 탭')
    st.bar_chart({'데이터':[1,2,3,4,5]})
    
with tab2:
    st.subheader('데이터 탭')
    st.dataframe({'기준': ['a', 'b','c','d','e'], '값':[1,2,3,4,5]})

with tab3:
    st.subheader("체크박스활성화")
    st.checkbox("체크박스활성화여부")
    st.slider('값 선택:', 0, 5, 10)



#분석페이지의 분석탭 구성함수
def make_anal_tab():
    st.header('탭추가')
    tab1, tab2, tab3 = st.tabs(['차트', '데이터', '설정'])
    with tab1:
        st.subheader('차트 탭')
        st.bar_chart({'데이터':[1,2,3,4,5]})
    
    with tab2:
        st.subheader('데이터 탭')
        st.dataframe({'기준': ['a', 'b','c','d','e'], '값':[1,2,3,4,5]})

    with tab3:
        st.subheader("체크박스활성화")
        st.checkbox("체크박스활성화여부")
        st.slider('값 선택:', 0, 5, 10)

st.divider()

#확장영역 추가
st.header('인스펜더 추가')
with st.expander('숨긴영역'):
    st.write('여기는 보이지 않습니다. 클릭해야 보입니다.')
 
