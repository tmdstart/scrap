import streamlit as st
import pandas as pd

#checkbox
active = st.checkbox('I agree')
active2 = st.checkbox('not agree')

if active:
    st.text('Welcome....') 
if active2: 
    st.text('helloWelcome....') 

#함수 on_change

def write():
    if active: 
         st.text('Welcome....') 
    if active2: 
         st.text('helloWelcome....') 

st.checkbox('butten', on_change=write)



if 'checkbox_state' not in st.session_state:
    st.session_state.checkbox_state = True
    
if st.session_state.checkbox_state :
    st.write('응..')
    
st.checkbox('진짜누를래??', on_change='checkbox_write1')



selected = st.toggle('Turn in thr switch!!')
if selected:
    st.text('trun on!') 
else:
    st.text('trun off!')  

# selectbox 선택지
option=st.selectbox(
    'your selection is', 
     options=['김밥','떡볶이','우동','쫄면'],
     index=None,
     placeholder='네개중 하나만 골라야돼'
)
st.text(f'오늘의 점심메뉴는:{option}')



#radio
genre=st.radio(
    '무슨 영화를 좋아하세요', ['멜로', '스럴러', '판타지'],
     captions=['봄남은 간다', '트리거', '웬즈데이 ']

)

st.text(f'당신이 좋아하는 장르는 {genre}')

#multiselect
menus =st.multiselect(
    '먹고싶은거 다 골라', ['김밥', '떡볶이', '우동', '쫄면']
)

st.text(f'내가 선택한 메뉴는 {menus}')


#slider
score= st.slider('내 점수 선택', 0, 100,10)
st.text(f'score: {score}')
        
from datetime import time
st_time, end_time =st.slider(
    '공부시간선택',
    min_value = time(0), max_value=time(11),
    value = (time(8), time(18))
)

st.text(f'공부시간: {st_time} ~ {end_time}')


#text_input 
txt1 = st.text_input('영화제목', placeholder='제목을입력하세요')
txt2 = st.text_input('비밀번호', placeholder='비밀번호를 입력하세요', type ='password')
st.text(f'텍스트 입력 결과: {txt1}, {txt2}')


#파일업로더 
file =st.file_uploader(
    '파일선택',  type='csv', accept_multiple_files = True
)

if file is not None:
   df=pd.read_csv(file)
   st.write(df)

   with open(file.name, 'wb') as out:
       out.write(file.getbuffer())

       