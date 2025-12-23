import streamlit as st
from google.cloud import bigquery
import pandas as pd

st.set_page_config(page_title="OpenAir Taunus", layout="wide")

st.title("🌲 OpenAir: Taunus Mountains Air Quality")
st.markdown("Real-time data processed by Airflow and DBT.")

client = bigquery.Client.from_service_account_json("gcp-key.json")

@st.cache_data
def load_data():
    query = """
        SELECT * FROM `openair-nature-pipeline.openair_nature_data.stg_taunus_air`
        ORDER BY measurement_time DESC
        LIMIT 100
    """
    return client.query(query).to_dataframe()

df = load_data()

threshold = st.sidebar.slider("PM2.5 Select Warning Threshold", 0, 100, 15)

if df['pm2_5'].iloc[0] > threshold:
    st.sidebar.warning(f"Attention! PM2.5 has exceeded your selected {threshold} threshold.")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Air Quality Index (AQI)", df['air_quality_index'].iloc[0])
with col2:
    st.metric("PM2.5 Value", f"{df['pm2_5'].iloc[0]} µg/m³")
with col3:
    st.metric("Last Update", df['measurement_time'].iloc[0].strftime("%H:%M:%S"))

st.subheader("Time Series Analysis")
st.line_chart(df.set_index('measurement_time')[['pm2_5', 'pm10']])

st.subheader("Measurement Point")

map_df = df[['latitude', 'longitude']].rename(columns={'latitude': 'lat', 'longitude': 'lon'})
st.map(map_df)

if st.checkbox("Show Raw Data"):
    st.write(df)