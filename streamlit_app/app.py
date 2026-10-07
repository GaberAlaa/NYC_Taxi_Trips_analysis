import streamlit as st

st.set_page_config(page_title="Traffic Prediction", layout="wide")

st.title("Traffic Prediction")

st.write(
    "This application will load the trained traffic model from SeaweedFS "
    "and provide prediction inputs."
)
