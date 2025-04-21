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
            with st.spinner("Generating and executing SQL..."):
                try:
                    resp = requests.post(API_URL, json={"question": question, "connection_url": connection_url})
                    try:
                        resp_json = resp.json()
                    except ValueError:
                        resp_json = {}
                    if resp.status_code != 200:
                        detail = resp_json.get("detail", resp.text)
                        st.error(f"API Error ({resp.status_code}): {detail}")
                        st.stop()
                    data = resp_json
                except requests.exceptions.RequestException as e:
                    st.error(f"Network error: {e}")
                except Exception as e:
                    st.error(f"Unexpected error: {e}")
                st.subheader("Generated SQL")
                st.code(data.get("sql", ""), language="sql")

                st.subheader("Results")
                rows = data.get("rows", [])
                if rows:
                    df = pd.DataFrame(rows)
                    st.dataframe(df)
                    # Visualization options
                    chart_type = st.selectbox("Chart Type", ["None", "Line", "Bar", "Area"], key="chart_type")
                    if chart_type != "None":
                        if chart_type == "Line":
                            st.line_chart(df)
                        elif chart_type == "Bar":
                            st.bar_chart(df)
                        else:
                            st.area_chart(df)
                    # Download results
                    csv = df.to_csv(index=False)
                    st.download_button("Download CSV", csv, "results.csv", "text/csv", key="download")
                else:
                    st.info("No rows returned.")

                # Performance Insights
                st.subheader("Performance Insights")
                try:
                    from sqlalchemy import text as sql_text_fn
                    engine_insights = create_engine(connection_url)
                    sql_text = data.get("sql", "")
                    if connection_url.startswith("sqlite"):
                        plan_sql = f"EXPLAIN QUERY PLAN {sql_text}"
                    elif connection_url.startswith("postgresql"):
                        plan_sql = f"EXPLAIN ANALYZE {sql_text}"
                    elif connection_url.startswith("mysql"):
                        plan_sql = f"EXPLAIN {sql_text}"
                    else:
                        plan_sql = None
                    if plan_sql:
                        with engine_insights.connect() as conn_plan:
                            res_plan = conn_plan.execute(text(plan_sql))
                            df_plan = pd.DataFrame(res_plan.fetchall(), columns=res_plan.keys())
                            st.table(df_plan)
                except Exception as e:
                    st.error(f"Performance insights error: {e}")

                # Data Profiling
                st.subheader("Data Profiling")
                try:
                    engine_profile = create_engine(connection_url)
                    md_profile = MetaData()
                    md_profile.reflect(bind=engine_profile)
                    tables_profile = list(md_profile.tables.keys())
                    if tables_profile:
                        prof_tbl = st.selectbox("Choose table for profiling", tables_profile, key="profile_tbl")
                        if st.button("Run Profiling", key="profiling_btn"):
                            stats_list = []
                            with engine_profile.connect() as conn_prof:
                                for col in md_profile.tables[prof_tbl].columns:
                                    col_name = col.name
                                    total = conn_prof.execute(text(f"SELECT COUNT(*) as cnt FROM {prof_tbl}")).scalar()
                                    non_null = conn_prof.execute(text(f"SELECT COUNT({col_name}) as cnt FROM {prof_tbl}")).scalar()
                                    distinct = conn_prof.execute(text(f"SELECT COUNT(DISTINCT {col_name}) as cnt FROM {prof_tbl}")).scalar()
                                    null_count = total - non_null
                                    stat = {"column": col_name, "null_count": null_count, "distinct_count": distinct}
                                    try:
                                        res_stats = conn_prof.execute(text(f"SELECT MIN({col_name}) as min, MAX({col_name}) as max, AVG({col_name}) as avg FROM {prof_tbl}")).mappings().first()
                                        stat.update({"min": res_stats["min"], "max": res_stats["max"], "avg": res_stats["avg"]})
                                    except:
                                        pass
                                    stats_list.append(stat)
                            st.dataframe(pd.DataFrame(stats_list))
                except Exception as e:
                    st.error(f"Data Profiling error: {e}")

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
