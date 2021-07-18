import numpy as np
import pandas as pd
import streamlit as st
import altair as alt
import plotly.express as px
from add_data import db_execute_fetch

st.set_page_config(page_title="Dashboard | Telecom User Data Analysis ", layout="wide")

def loadData():
    query = "select * from TelecomUserData"
    df = db_execute_fetch(query, dbName="TelecomUserData", rdf=True)
    return df

def selectHandset():
    df = loadData()
    handset = st.multiselect("choose Handset Type(s)", list(df['handset_type'].unique()))
    if handset:
        df = df[np.isin(df, handset).any(axis=1)]
        st.write(df)

def selectHandsetManufac():
    df = loadData()
    manufacturer = st.multiselect("choose Handset Manufacturer", list(df['handset_manufacturer'].unique()))
    duration = st.multiselect("choose Time spent on session", list(df['duration_ms'].unique()))

    if manufacturer and not duration:
        df = df[np.isin(df, manufacturer).any(axis=1)]
        st.write(df)
    elif duration and not manufacturer:
        df = df[np.isin(df, duration).any(axis=1)]
        st.write(df)
    elif duration and manufacturer:
        manufacturer.extend(duration)
        df = df[np.isin(df, manufacturer).any(axis=1)]
        st.write(df)
    else:
        st.write(df)

def barChart(data, title, X, Y):
    title = title.title()
    st.title(f'{title} Chart')
    msgChart = (alt.Chart(data).mark_bar().encode(alt.X(f"{X}:N", sort=alt.EncodingSortField(field=f"{Y}", op="values",
                order='ascending')), y=f"{Y}:Q"))
    st.altair_chart(msgChart, use_container_width=True)


def stBarChart():
    df = loadData()
    dfCount = pd.DataFrame({'Session_count': df.groupby(['msisdn_number'])['bearer_id'].count()}).reset_index()
    dfCount["msisdn_number"] = dfCount["msisdn_number"].astype(str)
    dfCount = dfCount.sort_values("Session_count", ascending=False)

    num = st.slider("Select number of Rankings", 0, 50, 5)
    title = f"Top {num} Sessions Ranking By User"
    barChart(dfCount.head(num), title, "msisdn_number", "Session_count")

def DataUsageBarChart():
    df = loadData()
    dfCount = pd.DataFrame({'Session_count': df.groupby(['handset_type'])['bearer_id'].count()}).reset_index()
    dfCount["handset_type"] = dfCount["handset_type"].astype(str)
    dfCount = dfCount.sort_values("Session_count", ascending=False)

    # num = st.slider("Select number of Rankings", 0, 50, 5)
    num = 5
    title = f"Top {num} Sessions Ranking By Type of Handset"
    barChart(dfCount.head(num), title, "handset_type", "Session_count")

def DataUsagePerAppChart():
    df = loadData()
    dfLangCount = pd.DataFrame({'Data_Usage': df.groupby(['handset_type'])['social_media_data', 'google_data',
                                                                           'email_data', 'youtube_data', 'netflix_data',
                                                                           'gaming_data', 'total_dl_ul'].sum()}).reset_index()
    dfLangCount["handset_type"] = dfLangCount["handset_type"].astype(str)
    dfLangCount = dfLangCount.sort_values("Data_Usage", ascending=False)
    dfLangCount.loc[dfLangCount['Data_Usage'] < 10, 'handset_type'] = 'Other Handsets'
    st.title(" User data per Handsets pie chart")
    fig = px.pie(dfLangCount, values='Data_Usage', names='handset_type', width=500, height=350)
    fig.update_traces(textposition='inside', textinfo='percent+label')

    colB1, colB2 = st.beta_columns([2.5, 1])

    with colB1:
        st.plotly_chart(fig)
    with colB2:
        st.write(dfLangCount)

st.markdown("<h1 style='color:#0b4eab;font-size:36px;border-radius:10px;'>Dashboard | Telecommunication Users Data Analysis </h1>", unsafe_allow_html=True)
selectHandset()
# st.markdown("<p style='padding:10px; background-color:#000000;color:#00ECB9;font-size:16px;border-radius:10px;'>Section Break</p>", unsafe_allow_html=True)
selectHandsetManufac()
st.title("Data Visualizations")
with st.beta_expander("Show More Graphs"):
    stBarChart()
    DataUsageBarChart()
    DataUsagePerAppChart()
    # langPie()