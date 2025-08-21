import streamlit as st
import seaborn as sns
import pandas as pd
import plotly.express as px
import matplotlib.pyplot as plt
#layout 요소
#cilumns는 요소를 왼쪽- > 오른쪽으로 배치가능


col1, col2, col3= st.columns(3)

with col1:
    st.metric(
        '오늘의 날씨',
        value = '35도',
        delta='+3' 
    )


with col2:
    st.metric(
        '미세먼지',
        value = '좋음',
        delta_color='inverse' 
    )



with col3:
    st.metric(
        '습도',
        value = '51.6',
        delta='-3' 
    )
    
    
## 
st.markdown('---')

data = {'이름' : ['홍길동', '김길동','박길동'],
        '나이' : [10,20,30]
        }

import pandas as pd
df = pd.DataFrame(data)
st.dataframe(df)


st.divider()

st.table(df)

st.json(data)

#datafile.csv > load > table 출력 > px.box() -> st.plotly.chaart()

#주식데이터 분석


df_abnb = pd.read_csv(
    './data/ABNB_stock.csv'
)

#fig, ax = plt.subplots()
#sns.lineplot(x='Date', y = 'Close', data=df_abnb, ax=ax)
#px.line(data_frame=df_abnb, x='Date', y='Close', width=500, height=400)

st.title("Matplotlib, Seaborn, Plotly 그래프")

# 2. Seaborn 그래프 그리기 (Matplotlib Figure 객체 사용)
st.header("Seaborn 그래프 (with st.pyplot)")
st.write("matplotlib.pyplot을 사용하여 seaborn 그래프를 그립니다.")

fig, ax = plt.subplots(figsize=(8, 4))
sns.lineplot(x='Date', y='Close', data=df_abnb, ax=ax)
ax.set_title('Seaborn Line Plot')
st.pyplot(fig) # <-- fig 객체를 st.pyplot()에 전달

# 3. Plotly 그래프 그리기 (Plotly Express)
st.header("Plotly 그래프 (with st.plotly_chart)")
st.write("Plotly의 interactive한 그래프를 그립니다.")

import plotly.express as px

plotly_fig = px.line(
    data_frame=df_abnb, 
    x='Date', 
    y='Close', 
    width=500, 
    height=400,
    title='Plotly Line Plot'
)
st.plotly_chart(plotly_fig) # <-- fig 객체를 st.plotly_chart()에 전달
