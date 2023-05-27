import streamlit as st
from util.snowflake_loader import SnowflakeLoader
from util.snowflake_util import Snowflake
from util.ag_grid_util import agGrid

st.set_page_config(layout="wide")
page_title = "Warehouse Analysis"
st.title(page_title)

snowflake = Snowflake()
snowflake_loader = SnowflakeLoader(snowflake.conn())

@st.cache_data(show_spinner=False, ttl = "60m", max_entries= 50)
def load_data(query):
    with st.spinner(text="Cooking up data..."):
        data = snowflake_loader.load_data(query)
    return data

add_selectbox = st.sidebar.selectbox(
    "What would you like to check?",
    (
        "Spilling",
        "Queueing",
        "Query Acceleration Service"
    ),
)

days = st.slider('Filter for last X days?', 1, 365, 7)

if add_selectbox == "Spilling":
    query = f"""
                select warehouse_name
                , warehouse_size
                , round(avg(total_elapsed_time)/1000,1) as elapsed_time_seconds
                , round(avg(execution_time)/1000,1) as execution_time_seconds
                , count(iff(bytes_spilled_to_local_storage/1024/1024/1024 > 1,1,null)) as count_spilled_queries
                , round(sum(bytes_spilled_to_local_storage/1024/1024/1024),1) as local_spillage_gb
                , round(sum(bytes_spilled_to_remote_storage/1024/1024/1024),1) as remote_spillage_gb
                from Snowflake.account_usage.query_history
                where warehouse_size is not null
                and start_time > dateadd('day', -{days}, current_timestamp())
                group by 1, 2
                having local_spillage_gb > 1
                order by remote_spillage_gb desc, local_spillage_gb desc;
            """
    with st.expander("Query"):
        st.code(query)
    data = load_data(query)
    st.subheader("Warehouses with data spilling")
    agGrid(data)

if add_selectbox == "Queueing":
    query = f"""
                select warehouse_name as warehouse_name
                ,warehouse_size as warehouse_size
                ,round(avg(execution_time)/1000,1) as avg_execution_seconds
                ,round(avg(total_elapsed_time)/1000,1) as avg_elapsed_seconds
                ,round(avg(queued_overload_time)/1000,1) as avg_overload_seconds
                ,round(((avg_overload_seconds/avg_elapsed_seconds)* 100),1) as percent_overload
                from Snowflake.account_usage.query_history
                where warehouse_size is not null
                and start_time >= DATEADD('day',-{days},CURRENT_TIMESTAMP())
                group by 1,2
                having percent_overload > 0
                order by percent_overload desc; 
            """
    with st.expander("Query"):
        st.code(query)
    data = load_data(query)
    st.subheader("Warehouses with data queueing")
    agGrid(data)
    
if add_selectbox == "Query Acceleration Service":
    query = f"""
                SELECT warehouse_name, 
                    COUNT(query_id) AS num_eligible_queries, 
                    SUM(eligible_query_acceleration_time) AS total_eligible_time
                  FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
                  WHERE start_time >= dateadd(day, -{days}, current_date)
                  GROUP BY warehouse_name
                  ORDER BY num_eligible_queries DESC, total_eligible_time DESC;
            """
    with st.expander("Query"):
        st.code(query)
    data = load_data(query)
    st.subheader("Warehouses that can benefit from Query Acceleration Service")
    agGrid(data)