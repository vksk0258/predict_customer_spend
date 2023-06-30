# %%
import streamlit as st
import pandas as pd
import altair as alt
from snowflake.snowpark.session import Session
from snowflake.snowpark.functions import *
import json



# %%
# Create a session to Snowflake with credentials
with open("connection.json") as f:
    connection_parameters = json.load(f)
session = Session.builder.configs(connection_parameters).create()

st.set_page_config(layout="wide")

st.markdown(
    """
<style>
[data-testid="stMetricValue"] {
    font-size: 37px;S
    color: #000080;
    border : 2px solid;
    font-weight : bold;
}
""",
    unsafe_allow_html=True,
)


# %%
# Header
empty1,head1, head2 ,empty2= st.columns([2, 6, 3, 6])

with head1:
    st.markdown('#')
    st.header("쇼핑몰 고객 연간 소비액 예측 모델")
with head2:
    st.image("dk.png",width=250)

st.markdown('##')
st.markdown('##')

# %%
# Customer Spend Slider Column
empty1, col1, col2, col3, empty2 = st.columns([2, 5, 1, 6, 3])

customer_df = session.table('PREDICTED_CUSTOMER_SPEND')

# Read Data
minasl, maxasl, mintoa, maxtoa, mintow, maxtow, minlom, maxlom = customer_df.select(
    floor(min(col("SESSION_LENGTH"))),
    ceil(max(col("SESSION_LENGTH"))),
    floor(min(col("TIME_ON_APP"))),
    ceil(max(col("TIME_ON_APP"))),
    floor(min(col("TIME_ON_WEBSITE"))),
    ceil(max(col("TIME_ON_WEBSITE"))),
    floor(min(col("LENGTH_OF_MEMBERSHIP"))),
    ceil(max(col("LENGTH_OF_MEMBERSHIP")))
).toPandas().iloc[0, ]

minasl = int(minasl)
maxasl = int(maxasl)
mintoa = int(mintoa)
maxtoa = int(maxtoa)
mintow = int(mintow)
maxtow = int(maxtow)
minlom = int(minlom)
maxlom = int(maxlom)


# Column 1
with col1:
    st.markdown("## Search")
    st.markdown("####")
    st.write("#### 오프라인 매장에 머무는 평균 시간(분)")
    asl = st.slider("Session Length", minasl, maxasl, (minasl, minasl+5), 1,label_visibility="collapsed")
    st.write("#### 애플리케이션 평균 사용 시간(분)")
    toa = st.slider("Time on App", mintoa, maxtoa, (mintoa, mintoa+5), 1,label_visibility="collapsed")
    st.write("#### 웹 사이트 평균 사용 시간(분)")
    tow = st.slider("Time on Website", mintow, maxtow, (mintow, mintow+5), 1,label_visibility="collapsed")
    st.write("#### 맴버쉽 가입 년 수")
    lom = st.slider("Length of Membership", minlom,
                    maxlom, (minlom, minlom+4), 1,label_visibility="collapsed")
    
# Column 2 (3)
with col3:
    st.markdown("## Data Analysis")
    st.markdown('###')

    minspend, maxspend = customer_df.filter(
        (col("SESSION_LENGTH") <= asl[1]) & (
            col("SESSION_LENGTH") > asl[0])
        & (col("TIME_ON_APP") <= toa[1]) & (col("TIME_ON_APP") > toa[0])
        & (col("TIME_ON_WEBSITE") <= tow[1]) & (col("TIME_ON_WEBSITE") > tow[0])
        & (col("LENGTH_OF_MEMBERSHIP") <= lom[1]) & (col("LENGTH_OF_MEMBERSHIP") > lom[0])
    ).select(trunc(min(col('PREDICTED_SPEND'))), trunc(max(col('PREDICTED_SPEND')))).toPandas().iloc[0, ]

    st.write('#### 쇼핑몰 고객 연간 소비액 예측')
    met1,ans1,met2,emp1 = st.columns([4,1,4,10])
    with met1:
        st.metric(label="최소", value=f"${int(minspend)}", label_visibility="collapsed")
        st.write("<h5 style='text-align: center;'>최소</h5>", unsafe_allow_html=True)
    with ans1:
        st.write("<h5 style='text-align: center; font-size: 50px;'><strong>~</strong></h5>", unsafe_allow_html=True)
    with met2:
        st.metric(label="최대", value=f"${int(maxspend)}", label_visibility="collapsed")
        st.write("<h5 style='text-align: center;'>최대</h5>", unsafe_allow_html=True)

    st.markdown("---")
    st.write("\n#### 특성 중요도 그래프")
    # 주어진 값들
    SESSION_LENGTH = 0.1
    TIME_ON_APP = 0.21
    TIME_ON_WEBSITE = 0.01
    LENGTH_OF_MEMBERSHIP = 0.68

    # 데이터 프레임 생성
    data = pd.DataFrame({
        'Variable': ['매장에 있는 시간', '앱 사용 시간', '웹 사용 시간', '맴버쉽 가입 년 수'],
        'Value': [SESSION_LENGTH, TIME_ON_APP, TIME_ON_WEBSITE, LENGTH_OF_MEMBERSHIP]
    })

    # 막대 그래프 생성
    bar_chart = alt.Chart(data).mark_bar().encode(
        x='Value',
        y=alt.Y('Variable', sort=None),
        color=alt.Color('Variable', legend=None),
    ).properties(
        width=500,
        height=200
    )

    # 텍스트 폰트 크기를 조정하는 새로운 막대 그래프 생성
    text_chart = alt.Chart(data).mark_text(
        align='left',
        baseline='middle',
        dx=3,  # 텍스트와 막대 사이의 거리
        fontSize=14  # 폰트 크기 설정
    ).encode(
        x='Value',
        y=alt.Y('Variable', sort=None),
        text=alt.Text('Value', format='.2f')
    )

    # 그래프 결합
    combined_chart = (bar_chart + text_chart)

    # Streamlit에서 그래프 표시
    st.altair_chart(combined_chart, use_container_width=True)
    
    quote = '''애플리케이션이 웹 사이트보다 소비액에 미치는 영향이 더 크기 때문에 
    \n**애플리케이션에 집중하기를 권합니다.** '''
    st.info(quote, icon="🤖")

    # st.write("\n#### 고객의 활동 중요도 순위")
    # st.markdown("<p style='color: #808080; font-size: 20px;'><strong>1. 맴버쉽 가입 년 수</strong></p>"
    #         "<p style='color: rgb(220, 0, 0); font-size: 20px;'><strong>2. 애플리케이션 평균 사용 시간</strong></p>"
    #         "<p style='color: #808080; font-size: 20px;'><strong>3. 오프라인 매장에 머무는 평균 시간</strong></p>"
    #         "<p style='color: rgb(220, 0, 0); font-size: 20px;'><strong>4. 웹 사이트 평균 사용 시간</strong></p>", unsafe_allow_html=True)