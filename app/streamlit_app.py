import streamlit as st

st.set_page_config(page_title="Stroke Insights", layout="wide")

st.title("Stroke Insights — Starter App")
st.write("✅ Your project structure is set. We'll plug in data and automation next.")

tab1, tab2, tab3 = st.tabs(["Hospital Explorer", "County Map", "State Deep Dives"])

with tab1:
    st.subheader("Hospital Explorer")
    st.info("Coming soon: CMS stroke outcome metrics by hospital (mortality & readmission)")

with tab2:
    st.subheader("County Map")
    st.info("Coming soon: CDC county stroke burden + ACS demographics + HRSA overlays")

with tab3:
    st.subheader("State Deep Dives")
    st.info("Coming soon: e.g., California HCAI with ischemic stroke counts")
