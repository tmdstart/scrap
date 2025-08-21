#datafile.csv > load > table 출력 > px.box > st.plotly_chart()
import streamlit as st
import seaborn as sns
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt


df_abnb = pd.read_csv(
    './data/ABNB_stock.csv'
)


st.write("Plotly chart")

import plotly.express as px

plotly_fig = px.line(
    data_frame=df_abnb, 
    x='Date', 
    y='Open', 
    width=500, 
    height=400,
    title='Plotly Line Plot'
)
st.plotly_chart(plotly_fig) # <-- fig 객체를 st.plotly_chart()에 전달



st.title('주식 데이터 분석')
st.markdown('드롭다운 메뉴에서 원하는 지표를 선택하세요.')

# 3. 드롭다운 박스 만들기
# df_abnb.columns에서 'Date'를 제외한 나머지 열 이름을 리스트로 만듭니다.
# 'Date' 열은 x축으로 고정하기 때문입니다.
metric_list = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']

# st.selectbox를 사용하여 드롭다운 메뉴를 만들고, 사용자가 선택한 값을 selected_metric 변수에 저장합니다.
selected_metric = st.selectbox('📈 지표 선택:', options=metric_list)

# 4. Plotly 선 그래프 생성 및 표시
# 사용자가 드롭다운에서 선택한 값(selected_metric)을 y축 값으로 사용합니다.
plotly_fig = px.line(
    data_frame=df_abnb, 
    x='Date', 
    y=selected_metric,  # ⭐ 이 부분이 드롭다운에서 선택된 변수로 바뀝니다.
    width=700, 
    height=500,
    title=f'{selected_metric}의 시간별 변화'
)

# Streamlit에 그래프를 표시합니다.
st.plotly_chart(plotly_fig)


"""
RangeIndex: 454 entries, 0 to 453
Data columns (total 7 columns):
 #   Column     Non-Null Count  Dtype  
---  ------     --------------  -----  
 0   Date       454 non-null    object 
 1   Open       454 non-null    float64
 2   High       454 non-null    float64
 3   Low        454 non-null    float64
 4   Close      454 non-null    float64
 5   Adj Close  454 non-null    float64
 6   Volume     454 non-null    int64  
dtypes: float64(5), int64(1), object(1)
memory usage: 25.0+ KB

"""


























#fig, ax = plt.subplots()
#sns.lineplot(x='Date', y = 'Close', data=df_abnb, ax=ax)
#px.line(data_frame=df_abnb, x='Date', y='Close', width=500, height=400)

# 3. Plotly 그래프 그리기 (Plotly Express)