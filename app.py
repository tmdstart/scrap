import streamlit as st
import pandas as pd



st.write("Hello, Streamlit!")
st.title('스트림릿 안녕하세요.')
st.write('hello, streamlit !!!!')

st.divider()


name=st.text_input('이름:')
if name : 
  st.write(f'안녕하세요...{name}님')

df=pd.read_csv("./data/ABNB_stock.csv")
print(df)
df


