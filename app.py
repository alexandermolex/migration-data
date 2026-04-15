import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
import os

# --- Page config ---
st.set_page_config(page_title="US County Migration", layout="wide")

# --- Database connection ---
# st.cache_resource means the connection is created once and reused
# instead of reconnecting on every interaction
@st.cache_resource
def get_engine():
    return create_engine(os.environ["DATABASE_URL"])

# --- Data loading ---
# st.cache_data means this query result is cached
# so clicking the same state twice doesn't hit the database twice
@st.cache_data
def load_inflow(state_name):
    engine = get_engine()
    query = """
        SELECT origin_state_name, origin_county_name,
               dest_state_name, dest_county_name,
               movers_in_county_to_county_flow_estimate
        FROM inflow
        WHERE dest_state_name = %(state)s
        AND movers_in_county_to_county_flow_estimate > 0
        ORDER BY movers_in_county_to_county_flow_estimate DESC
    """
    return pd.read_sql(query, engine, params={"state": state_name})

# --- Get list of all destination states for the dropdown ---
@st.cache_data
def load_states():
    engine = get_engine()
    df = pd.read_sql("SELECT DISTINCT dest_state_name FROM inflow ORDER BY dest_state_name", engine)
    return df["dest_state_name"].tolist()

# --- UI ---
st.title("US County-to-County Migration")
st.caption("Data: IRS/Census 5-year estimates, 2016-2020")

states = load_states()
selected_state = st.selectbox("Select a destination state/country", states)

df = load_inflow(selected_state)

st.subheader(f"Who moved into {selected_state}?")
st.write(f"{len(df):,} county-to-county flows found")

# Bar chart — top 20 origin counties
top20 = df.head(20).copy()
top20["label"] = top20["origin_county_name"] + ", " + top20["origin_state_name"]

st.bar_chart(top20.set_index("label")["movers_in_county_to_county_flow_estimate"])

# Data table
st.subheader("All flows")
st.dataframe(df, use_container_width=True)