import streamlit as st

from util.snowflake_loader import SnowflakeLoader
from util.snowflake_util import Snowflake
from util.ag_grid_util import agGrid

st.set_page_config(layout="wide")
page_title = "Query Analysis"
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
        "Top Queries for Improvement",
    ),
)

if add_selectbox == "Top Queries for Improvement":
    c1,c2,c3,c4,c5 = st.columns((1,1,1,1,1))
    c11, c12, c13, c14, c15 = st.columns((1, 1, 1, 1, 1))
    c21, c22, c23 = st.columns((1,1,1))

    container = c1.container()
    query = """
                SELECT DISTINCT WAREHOUSE_NAME 
                FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
                order by 1
            """
    wh_df = load_data(query)
    wh_list = wh_df['WAREHOUSE_NAME'].tolist()
    all_wh = c11.checkbox("Select All", key="wh", value= True)
    if all_wh:
        warehouse_list = container.multiselect("Choose Warehouse:", wh_list, disabled=True)
        warehouse_filter = ""
    else:
        warehouse_list = container.multiselect("Choose Warehouse:", wh_list, 'ADHOC_WH')
        if warehouse_list:
            warehouse_list_in_query = ','.join('\'' + i + '\'' for i in warehouse_list)
            warehouse_filter = f"and warehouse_name in ({warehouse_list_in_query})"
        else:
            warehouse_filter = ""

    container2 = c2.container()
    query = """
                    select distinct schema_name, database_name 
                    FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    order by 2,1 
                """
    schema_df = load_data(query)
    schema_df = schema_df[schema_df.DATABASE_NAME != 'None']
    db_list = schema_df['DATABASE_NAME'].unique().tolist()
    all_db = c12.checkbox("Select All", key="database", value=True)
    if all_db:
        database_list = container2.multiselect("Choose database:", db_list, disabled=True)
        db_filter = ""
    else:
        database_list = container2.multiselect("Choose database:", db_list, 'MERAKIDW')
        if database_list:
            database_list_in_query = ','.join('\'' + i + '\'' for i in database_list)
            db_filter = f"and database_name in ({database_list_in_query})"
        else:
            db_filter = ""

    container3 = c3.container()
    db_df = schema_df[schema_df['DATABASE_NAME'].isin(database_list)]
    schema_list = db_df['SCHEMA_NAME'].tolist()
    all_schema = c13.checkbox("Select All", key="schema", value=True)
    if all_schema:
        schemas_list = container3.multiselect("Choose schema:", schema_list, disabled=True)
        schema_filter = ""
    else:
        schemas_list = container3.multiselect("Choose schema:", schema_list, 'FACT')
        if schemas_list:
            schemas_list_in_query = ','.join('\'' + i + '\'' for i in schemas_list)
            schema_filter = f"and schema_name in ({schemas_list_in_query})"
        else:
            schema_filter = ""

    container4 = c4.container()
    query = """
                    select distinct user_name
                    from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                    order by 1;
            """
    user_df = load_data(query)
    user_list = user_df['USER_NAME'].tolist()
    all_user = c14.checkbox("Select All", key="user", value= True)
    if all_user:
        users_list = container4.multiselect("Choose User:", user_list, disabled=True)
        user_filter = ""
    else:
        users_list = container4.multiselect("Choose User:", user_list, 'ANALYST_USER')
        if user_list:
            user_list_in_query = ','.join('\'' + i + '\'' for i in users_list)
            user_filter = f"and user_name in ({user_list_in_query})"
        else:
            user_filter = ""

    container5 = c5.container()
    query = """
                        select distinct role_name
                        from SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
                        order by 1;
                """
    role_df = load_data(query)
    role_list = role_df['ROLE_NAME'].tolist()
    all_role = c15.checkbox("Select All", key="role", value=True)
    if all_role:
        roles_list = container5.multiselect("Choose Role:", role_list, disabled=True)
        role_filter = ""
    else:
        roles_list = container5.multiselect("Choose Role:", role_list, 'ANALYST_ROLE')
        if roles_list:
            role_list_in_query = ','.join('\'' + i + '\'' for i in roles_list)
            role_filter = f"and role_name in ({role_list_in_query})"
        else:
            role_filter = ""

    option = c21.radio(
        "Select an Option:",
        (
            'Top Expensive Queries',
            'Top long running queries',
            'Top queries with high partition scans',
            'Top queries with high disk spillage',
            'Top queries eligible for Query Acceleration Service'
        ))

    days = c22.slider('Filter for last X days?', 1, 365, 7)
    limit = c23.slider('Limit to Y rows?', 1, 1000, 50)

    if option == 'Top Expensive Queries':
        query = f"""
                    WITH
                    filtered_queries AS (
                        SELECT
                            query_id,
                            query_text AS original_query_text,
                            -- First, we remove comments enclosed by /* <comment text> */
                            REGEXP_REPLACE(query_text, '(/\*.*\*/)') AS _cleaned_query_text,
                            -- Next, removes single line comments starting with --
                            -- and either ending with a new line or end of string
                            REGEXP_REPLACE(_cleaned_query_text, '(--.*$)|(--.*\n)') AS cleaned_query_text,
                            warehouse_id,
                            warehouse_name,
                            user_name, 
                            role_name, 
                            schema_name, 
                            database_name,
                            execution_status,
                            round(total_elapsed_time/1000,1) as elapsed_time_seconds,
                            TIMEADD(
                                'millisecond',
                                queued_overload_time + compilation_time +
                                queued_provisioning_time + queued_repair_time +
                                list_external_files_time,
                                start_time
                            ) AS execution_start_time,
                            end_time
                        FROM snowflake.account_usage.query_history AS q
                        WHERE TRUE
                            AND warehouse_size IS NOT NULL
                            {warehouse_filter} {db_filter} {schema_filter} {user_filter} {role_filter}
                            AND QUERY_TYPE = 'SELECT'  -- only select queries
                            AND start_time >= DATEADD('day', -{days}, current_timestamp)
                    ),
                    -- 1 row per hour from 30 days ago until the end of today
                    hours_list AS (
                        SELECT
                            DATEADD(
                                'hour',
                                '-' || row_number() over (order by null),
                                DATEADD('day', '+1', CURRENT_DATE)
                            ) as hour_start,
                            DATEADD('hour', '+1', hour_start) AS hour_end
                        FROM TABLE(generator(rowcount => (24*31))) t
                    ),
                    -- 1 row per hour a query ran
                    query_hours AS (
                        SELECT
                            hl.hour_start,
                            hl.hour_end,
                            queries.*
                        FROM hours_list AS hl
                        INNER JOIN filtered_queries AS queries
                            ON hl.hour_start >= DATE_TRUNC('hour', queries.execution_start_time)
                            AND hl.hour_start < queries.end_time
                    ),
                    query_seconds_per_hour AS (
                        SELECT
                            *,
                            DATEDIFF('millisecond', GREATEST(execution_start_time, hour_start), LEAST(end_time, hour_end)) AS num_milliseconds_query_ran,
                            SUM(num_milliseconds_query_ran) OVER (PARTITION BY warehouse_id, hour_start) AS total_query_milliseconds_in_hour,
                            num_milliseconds_query_ran/total_query_milliseconds_in_hour AS fraction_of_total_query_time_in_hour,
                            hour_start AS hour
                        FROM query_hours
                    ),
                    credits_billed_per_hour AS (
                        SELECT
                            start_time AS hour,
                            warehouse_id,
                            credits_used_compute
                        FROM snowflake.account_usage.warehouse_metering_history
                    ),
                    query_cost AS (
                        SELECT
                            query.*,
                            credits.credits_used_compute*3 AS actual_warehouse_cost,
                            credits.credits_used_compute*fraction_of_total_query_time_in_hour*3 AS query_allocated_cost_in_hour
                        FROM query_seconds_per_hour AS query
                        INNER JOIN credits_billed_per_hour AS credits
                            ON query.warehouse_id=credits.warehouse_id
                            AND query.hour=credits.hour
                    ),
                    cost_per_query AS (
                        SELECT
                            query_id,
                            ANY_VALUE(original_query_text) AS original_query_text,
                            ANY_VALUE(warehouse_name) AS warehouse_name,
                            ANY_VALUE(MD5(cleaned_query_text)) AS query_signature,
                            SUM(query_allocated_cost_in_hour) AS query_cost,
                            SUM(num_milliseconds_query_ran) / 1000 AS execution_time_s,
                            ANY_VALUE(user_name) as user_name,
                            ANY_VALUE(role_name) as role_name,
                            ANY_VALUE(schema_name) as schema_name,
                            ANY_VALUE(database_name) as database_name,
                            ANY_VALUE(execution_status) as execution_status
                        FROM query_cost
                        GROUP BY 1
                    )
                    SELECT
                        query_signature,
                        ANY_VALUE(query_id) AS sample_query_id,
                        ANY_VALUE(original_query_text) AS sample_query_text,
                        ANY_VALUE(warehouse_name) AS warehouse_name,
                        COUNT(*) AS num_executions,
                        AVG(query_cost) AS avg_cost_per_execution,
                        SUM(query_cost) AS total_cost,
                        ANY_VALUE(user_name) as user_name,
                        ANY_VALUE(role_name) as role_name,
                        ANY_VALUE(schema_name) as schema_name,
                        ANY_VALUE(database_name) as database_name,
                        ANY_VALUE(execution_status) as execution_status
                    FROM cost_per_query
                    GROUP BY 1
                    order by avg_cost_per_execution desc
                    limit {limit};  
                """
        with st.expander("Query"):
            st.code(query)
        data = load_data(query)
        st.subheader("Top Expensive Queries")
        agGrid(data)
    elif option == 'Top long running queries':
        query = f"""
            select query_id,
                    query_text,
                    warehouse_name,
                    round(total_elapsed_time/1000,1) as elapsed_time_seconds,
                    round(execution_time/1000,1) as execution_time_seconds, 
                    round(compilation_time/1000,1) as compilation_time_seconds,
                    partitions_scanned, 
                    partitions_total, 
                    partitions_scanned / nullifzero(partitions_total) * 100 as pct_scanned,
                    round(bytes_spilled_to_local_storage/1024/1024/1024,1) as local_spillage_gb,
                    round(bytes_spilled_to_remote_storage/1024/1024/1024,1) as remote_spillage_gb, 
                    user_name, 
                    role_name, 
                    schema_name, 
                    database_name,
                    execution_status
            from snowflake.account_usage.query_history
            where warehouse_size is not null
            and start_time > dateadd('day', -{days}, current_timestamp())
            {warehouse_filter} {db_filter} {schema_filter} {user_filter} {role_filter}
            AND total_elapsed_time > 0 --only get queries that actually used compute
            AND partitions_scanned IS NOT NULL
            ORDER BY total_elapsed_time desc
            limit {limit};
        """
        with st.expander("Query"):
            st.code(query)
        data = load_data(query)
        st.subheader("Top long running queries")
        agGrid(data)
    elif option == 'Top queries with high partition scans':
        query = f"""
            select query_id,
                    query_text, 
                    warehouse_name, 
                    partitions_scanned, 
                    partitions_total, 
                    partitions_scanned / nullifzero(partitions_total) * 100 as pct_scanned,
                    round(total_elapsed_time/1000,1) as elapsed_time_seconds,
                    round(execution_time/1000,1) as execution_time_seconds, 
                    round(compilation_time/1000,1) as compilation_time_seconds,
                    round(bytes_spilled_to_local_storage/1024/1024/1024,1) as local_spillage_gb,
                    round(bytes_spilled_to_remote_storage/1024/1024/1024,1) as remote_spillage_gb, 
                    user_name, 
                    role_name, 
                    schema_name, 
                    database_name, 
                    execution_status
            from snowflake.account_usage.query_history
            where warehouse_size is not null
            and start_time > dateadd('day', -{days}, current_timestamp())
            {warehouse_filter} {db_filter} {schema_filter} {user_filter} {role_filter}
            and   partitions_scanned > 1000
            and   pct_scanned > 0.8
            order by partitions_scanned desc, pct_scanned desc
            limit {limit};
        """
        with st.expander("Query"):
            st.code(query)
        data = load_data(query)
        st.subheader("Top Queries with high partition scans")
        agGrid(data)
    elif option == 'Top queries with high disk spillage':
        query = f"""
                select query_id,
                  query_text,
                  warehouse_name,
                  round(bytes_spilled_to_local_storage/1024/1024/1024,1) as local_spillage_gb,
                  round(bytes_spilled_to_remote_storage/1024/1024/1024,1) as remote_spillage_gb,
                  round(total_elapsed_time/1000,1) as elapsed_time_seconds,
                  round(execution_time/1000,1) as execution_time_seconds,
                  round(compilation_time/1000,1) as compilation_time_seconds,
                  partitions_scanned, 
                  partitions_total, 
                  partitions_scanned / nullifzero(partitions_total) * 100 as pct_scanned, 
                  user_name, 
                  role_name, 
                  schema_name, 
                  database_name,
                  execution_status
                from Snowflake.account_usage.query_history
                where warehouse_size is not null
                and start_time > dateadd('day', -{days}, current_timestamp())
                {warehouse_filter} {db_filter} {schema_filter} {user_filter} {role_filter}
                and local_spillage_gb > 1
                order by remote_spillage_gb desc, local_spillage_gb desc
                limit {limit};
                """
        with st.expander("Query"):
            st.code(query)
        data = load_data(query)
        st.subheader("Top queries with high disk spillage")
        agGrid(data)
    elif option == 'Top queries eligible for Query Acceleration Service':
        query = f"""
                SELECT query_id, 
                    query_text,
                    warehouse_name,
                    eligible_query_acceleration_time, 
                    upper_limit_scale_factor
                FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_ACCELERATION_ELIGIBLE
                where start_time >= dateadd(day, -{days}, current_date) {warehouse_filter}
                ORDER BY eligible_query_acceleration_time DESC
                limit {limit};
            """
        with st.expander("Query"):
            st.code(query)
        data = load_data(query)
        st.subheader("Top queries eligible for Query Acceleration Service")
        agGrid(data)

