import os
import streamlit as st
import requests
from dotenv import load_dotenv, find_dotenv
import pandas as pd
import json
from sqlalchemy import create_engine, text, inspect, MetaData
import pymongo
from urllib.parse import urlparse

# Load environment variables from root .env
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

# Backend API URLs
API_URL = os.getenv("NL_SQL_API_URL", "http://localhost:8001/query")
HISTORY_URL = os.getenv("NL_SQL_HISTORY_URL", "http://localhost:8001/history")

st.set_page_config(page_title="NL SQL Assistant", layout="wide")
st.title("Natural Language SQL Assistant")

# Initialize session state variables if they don't exist
if 'query_results_df' not in st.session_state:
    st.session_state.query_results_df = None
if 'sql_query_text' not in st.session_state:
    st.session_state.sql_query_text = ""
if 'data_profiling_df' not in st.session_state: # For persisting profiling results
    st.session_state.data_profiling_df = None
if 'performance_insights_df' not in st.session_state: # For persisting performance insights
    st.session_state.performance_insights_df = None

tab1, tab2, tab3 = st.tabs(["Query", "History", "Explorer"])

with tab1:
    # Connection configuration for Query
    st.subheader("Connection Configuration")
    st.info("Supports DDL commands (CREATE, ALTER, DROP). Non-SELECT statements execute but return no rows.")
    db_type = st.selectbox("Database Type", ["PostgreSQL", "MySQL", "SQLite", "MongoDB"])
    use_custom = st.checkbox("Use custom connection URL", value=False)
    custom_url = st.text_input("Custom Connection URL", os.getenv("DB_URL", ""), key="custom_url")
    if db_type in ["PostgreSQL", "MySQL"]:
        host = st.text_input("Host", os.getenv("DB_HOST", "localhost"))
        default_port = "5432" if db_type == "PostgreSQL" else "3306"
        port = st.text_input("Port", os.getenv("DB_PORT", default_port))
        user = st.text_input("User", os.getenv("DB_USER", ""))
        password = st.text_input("Password", "", type="password")
        driver = "postgresql" if db_type == "PostgreSQL" else "mysql+pymysql"
        # List accessible databases
        default_db = "postgres" if db_type == "PostgreSQL" else "information_schema"
        try:
            engine_list = create_engine(f"{driver}://{user}:{password}@{host}:{port}/{default_db}")
            with engine_list.connect() as conn:
                if db_type == "PostgreSQL":
                    dbs = [r[0] for r in conn.execute(text(
                        "SELECT datname FROM pg_database WHERE datistemplate = false;"
                    )).fetchall()]
                else:
                    dbs = [r[0] for r in conn.execute(text("SHOW DATABASES;"))]
            selected_db = st.selectbox("Select Database", dbs, key="db_select")
            connection_url = f"{driver}://{user}:{password}@{host}:{port}/{selected_db}"
        except Exception as e:
            st.error(f"Could not fetch databases: {e}")
            dbname = st.text_input("Database Name", os.getenv("DB_NAME", ""), key="dbname")
            connection_url = f"{driver}://{user}:{password}@{host}:{port}/{dbname}"
    elif db_type == "SQLite":
        sqlite_file = st.text_input("SQLite file path", os.getenv("DB_SQLITE_PATH", ""))
        connection_url = f"sqlite:///{sqlite_file}"
    else:
        connection_url = st.text_input("MongoDB URI", os.getenv("DB_MONGO_URI", ""))
    if use_custom and custom_url:
        connection_url = custom_url
    st.markdown(f"**Connection URL:** `{connection_url}`")
    # Preview schema and data
    st.subheader("Database Preview")
    try:
        engine_preview = create_engine(connection_url)
        md = MetaData()
        md.reflect(bind=engine_preview)
        tables = list(md.tables.keys())
        st.write("Tables:", tables)
        if tables:
            tbl = st.selectbox("Choose table to preview", tables, key="preview_tbl")
            if st.button("Preview Data", key="preview_btn"):
                df_prev = pd.read_sql(f"SELECT * FROM {tbl} LIMIT 5", engine_preview)
                st.dataframe(df_prev)
    except Exception as e:
        st.info(f"Preview not available: {e}")

    # ER Diagram
    st.subheader("ER Diagram")
    try:
        engine_er = create_engine(connection_url)
        md_er = MetaData()
        md_er.reflect(bind=engine_er)
        dot = "digraph ER { rankdir=LR; node [shape=record];"
        for tname, table in md_er.tables.items():
            cols = [c.name for c in table.columns]
            label = "{" + tname + "|" + "\\l".join(cols) + "\\l}" 
            dot += f'"{tname}" [label="{label}"];'
        for tname, table in md_er.tables.items():
            for col in table.columns:
                for fk in col.foreign_keys:
                    referred = fk.column.table.name
                    dot += f'"{tname}" -> "{referred}" [label="{col.name}"];'
        dot += "}"
        st.graphviz_chart(dot)
    except Exception as e:
        st.error(f"ER Diagram not available: {e}")

    # Quick Query Presets
    quick_query = st.selectbox("Quick Queries", ["None", "List tables", "Show first 10 rows", "Count rows", "Describe table schema", "Distinct values", "Top 5 by column"], key="quick_query")
    preset_question = None
    table_for_preset = None
    column_for_preset = None
    if quick_query != "None":
        if quick_query in ["Show first 10 rows", "Count rows", "Describe table schema", "Distinct values", "Top 5 by column"]:
            table_for_preset = st.text_input("Table name (for quick query)", key="preset_table")
        if quick_query in ["Distinct values", "Top 5 by column"]:
            column_for_preset = st.text_input("Column name (for quick query)", key="preset_column")
        if quick_query == "List tables":
            preset_question = "List all tables in the database."
        elif quick_query == "Show first 10 rows" and table_for_preset:
            preset_question = f"Show me the first 10 rows from the {table_for_preset} table."
        elif quick_query == "Count rows" and table_for_preset:
            preset_question = f"Count the number of rows in the {table_for_preset} table."
        elif quick_query == "Describe table schema" and table_for_preset:
            preset_question = f"Describe the schema of the {table_for_preset} table."
        elif quick_query == "Distinct values" and table_for_preset and column_for_preset:
            preset_question = f"List distinct values of column {column_for_preset} in table {table_for_preset}."
        elif quick_query == "Top 5 by column" and table_for_preset and column_for_preset:
            preset_question = f"Show the top 5 values in column {column_for_preset} of table {table_for_preset}."
        if preset_question:
            st.session_state.question = preset_question
    question = st.text_area("Enter your question in plain English:", value=st.session_state.get("question", ""), height=100, key="question")

    if st.button("Run Query"):
        if not question.strip():
            st.warning("Please enter a question.")
        elif not connection_url.strip():
            st.warning("Please configure the database connection.")
        else:
            # Clear previous results when a new query is run
            st.session_state.query_results_df = None
            st.session_state.sql_query_text = ""
            st.session_state.data_profiling_df = None
            st.session_state.performance_insights_df = None

            with st.spinner("Generating and executing SQL..."):
                data_api_response = None # Use a different variable name to avoid confusion with session_state
                try:
                    resp = requests.post(API_URL, json={"question": question, "connection_url": connection_url})
                    if resp.status_code != 200:
                        error_message = resp.text
                        try:
                            error_message = resp.json().get("detail", resp.text)
                        except ValueError:
                            pass
                        st.error(f"An error occurred with the API request. Server responded with status {resp.status_code}: {error_message}")
                        st.stop()
                    data_api_response = resp.json()
                except requests.exceptions.RequestException as e:
                    st.error(f"Failed to connect to the backend API at {API_URL}. Please ensure the backend is running and accessible. Error: {e}")
                    st.stop()
                except json.JSONDecodeError as e:
                    st.error(f"Error decoding API response (expected JSON): {e}. Response text: {resp.text[:200]}...")
                    st.stop()
                except Exception as e:
                    st.error(f"An unexpected error occurred while communicating with the API: {e}")
                    st.stop()

                if data_api_response:
                    st.session_state.sql_query_text = data_api_response.get("sql", "")
                    rows = data_api_response.get("rows", [])
                    if rows:
                        st.session_state.query_results_df = pd.DataFrame(rows)
                    else:
                        st.session_state.query_results_df = pd.DataFrame() # Use empty DataFrame for "no rows"

                    # Automatically run Performance Insights if SQL was generated
                    if st.session_state.sql_query_text and not connection_url.startswith("mongodb"):
                        try:
                            from sqlalchemy import text as sql_text_fn # Keep import local
                            engine_insights = create_engine(connection_url)
                            sql_text_perf = st.session_state.sql_query_text
                            plan_sql = None
                            if connection_url.startswith("sqlite"):
                                plan_sql = f"EXPLAIN QUERY PLAN {sql_text_perf}"
                            elif connection_url.startswith("postgresql"):
                                plan_sql = f"EXPLAIN ANALYZE {sql_text_perf}"
                            elif connection_url.startswith("mysql"):
                                plan_sql = f"EXPLAIN {sql_text_perf}"
                            
                            if plan_sql:
                                with engine_insights.connect() as conn_plan:
                                    res_plan = conn_plan.execute(sql_text_fn(plan_sql))
                                    st.session_state.performance_insights_df = pd.DataFrame(res_plan.fetchall(), columns=res_plan.keys())
                        except Exception as e:
                            # Silently fail for performance insights for now, or use st.info/st.warning
                            print(f"Performance insights error: {e}") # Log for debugging
                            st.session_state.performance_insights_df = None


    # --- Display sections based on session state (outside the button logic) ---
    if st.session_state.sql_query_text:
        st.subheader("Generated SQL")
        st.code(st.session_state.sql_query_text, language="sql")

    if st.session_state.query_results_df is not None:
        st.subheader("Results")
        if not st.session_state.query_results_df.empty:
            st.dataframe(st.session_state.query_results_df)
            # Visualization options
            chart_type = st.selectbox("Chart Type", ["None", "Line", "Bar", "Area"], key="chart_type")
            if chart_type != "None":
                try:
                    if chart_type == "Line":
                        st.line_chart(st.session_state.query_results_df)
                    elif chart_type == "Bar":
                        st.bar_chart(st.session_state.query_results_df)
                    else: # Area
                        st.area_chart(st.session_state.query_results_df)
                except Exception as e:
                    st.error(f"Error rendering {chart_type.lower()} chart: {e}")
            
            # Download results
            try:
                csv_data = st.session_state.query_results_df.to_csv(index=False)
                st.download_button("Download CSV", csv_data, "results.csv", "text/csv", key="download")
            except Exception as e:
                st.error(f"Error preparing CSV for download: {e}")
        else: # Empty DataFrame means query ran but returned no rows
            st.info("No rows returned or the query was a DDL/DML statement.")
            
    if st.session_state.performance_insights_df is not None and not st.session_state.performance_insights_df.empty:
        st.subheader("Performance Insights")
        st.table(st.session_state.performance_insights_df)
    elif st.session_state.sql_query_text and not connection_url.startswith("mongodb"): # Query was run but no insights
        st.subheader("Performance Insights")
        st.info("Performance insights could not be generated for this query or database type.")


    # Data Profiling - this can remain more independent but also use session state if we want to persist its result
    if connection_url and not connection_url.startswith("mongodb"): # Only for SQL DBs
        st.subheader("Data Profiling")
        try:
            engine_profile = create_engine(connection_url)
                    md_profile = MetaData()
                    md_profile.reflect(bind=engine_profile)
                    tables_profile = list(md_profile.tables.keys())
                    
                    if 'profile_tbl_selection' not in st.session_state:
                        st.session_state.profile_tbl_selection = tables_profile[0] if tables_profile else None

                    if tables_profile:
                        # Use a callback to update selection and clear old profiling data
                        def on_profile_table_change():
                            st.session_state.data_profiling_df = None
                            st.session_state.profile_tbl_selection = st.session_state.profile_tbl_widget # new selection

                        prof_tbl = st.selectbox(
                            "Choose table for profiling", 
                            tables_profile, 
                            key="profile_tbl_widget", # Use a different key for the widget itself
                            on_change=on_profile_table_change,
                            index=tables_profile.index(st.session_state.profile_tbl_selection) if st.session_state.profile_tbl_selection in tables_profile else 0
                        )

                        if st.button("Run Profiling", key="profiling_btn"):
                            if prof_tbl: # Ensure a table is selected
                                stats_list = []
                                with engine_profile.connect() as conn_prof:
                                    selected_table_obj = md_profile.tables[prof_tbl]
                                    for col in selected_table_obj.columns:
                                        col_name = col.name
                                        total = conn_prof.execute(text(f"SELECT COUNT(*) as cnt FROM {prof_tbl}")).scalar_one_or_none()
                                        non_null = conn_prof.execute(text(f"SELECT COUNT({col_name}) as cnt FROM {prof_tbl}")).scalar_one_or_none()
                                        distinct = conn_prof.execute(text(f"SELECT COUNT(DISTINCT {col_name}) as cnt FROM {prof_tbl}")).scalar_one_or_none()
                                        null_count = total - non_null if total is not None and non_null is not None else 0
                                        stat = {"column": col_name, "null_count": null_count, "distinct_count": distinct}
                                        
                                        # Check if column type supports MIN, MAX, AVG
                                        if col.type.python_type in (int, float) or isinstance(col.type, (pd.IntegerDtype, pd.Float64Dtype)): # Basic check
                                            try:
                                                res_stats_query = text(f"SELECT MIN({col_name}) as min_val, MAX({col_name}) as max_val, AVG({col_name}) as avg_val FROM {prof_tbl}")
                                                res_stats = conn_prof.execute(res_stats_query).mappings().first()
                                                if res_stats:
                                                    stat.update({"min": res_stats["min_val"], "max": res_stats["max_val"], "avg": res_stats["avg_val"]})
                                            except Exception: # Catch errors for non-numeric types if check above is not sufficient
                                                pass # Skip min/max/avg for non-numeric or problematic columns
                                        stats_list.append(stat)
                                st.session_state.data_profiling_df = pd.DataFrame(stats_list)
                        
                        if st.session_state.data_profiling_df is not None and not st.session_state.data_profiling_df.empty:
                            st.dataframe(st.session_state.data_profiling_df)
                        elif st.session_state.data_profiling_df is not None and st.session_state.data_profiling_df.empty:
                             st.info("Profiling run, but no statistics generated.")

        except Exception as e:
            st.error(f"Data Profiling setup error: {e}")
            st.session_state.data_profiling_df = None # Clear on error

with tab2:
    with st.spinner("Fetching history..."):
        try:
            resp = requests.get(HISTORY_URL)
            resp.raise_for_status()
            history = resp.json()
            if history:
                # Export history
                df_hist = pd.DataFrame(history)
                csv_hist = df_hist.to_csv(index=False)
                st.download_button("Download history CSV", csv_hist, "history.csv", "text/csv", key="download_history_csv")
                st.download_button("Download history JSON", json.dumps(history, default=str), "history.json", "application/json", key="download_history_json")
                for item in history:
                    with st.expander(f"{item['created_at']} - {item['question']}"):
                        st.code(item['sql'], language="sql")
                        rows = item['rows']
                        if rows:
                            df_h = pd.DataFrame(rows)
                            st.dataframe(df_h)
                            csv_h = df_h.to_csv(index=False)
                            st.download_button(
                                "Download CSV", csv_h,
                                f"history_{item['id']}.csv",
                                key=f"download_{item['id']}"
                            )
                        else:
                            st.info("No rows returned.")
            else:
                st.info("No history available.")
        except Exception as e:
            st.error(f"Error fetching history: {e}")

with tab3:
    st.subheader("MongoDB Explorer")
    mongo_uri_explorer = st.text_input("MongoDB URI for Explorer", os.getenv("DB_MONGO_URI", ""), key="mongo_explorer_uri")
    try:
        client = pymongo.MongoClient(mongo_uri_explorer)
        dbs = client.list_database_names()
        st.write("Databases:", dbs)
        if dbs:
            db_select = st.selectbox("Select Database", dbs, key="explorer_db")
            collections = client[db_select].list_collection_names()
            st.write("Collections:", collections)
    except Exception as e:
        st.error(f"MongoDB Explorer error: {e}")
