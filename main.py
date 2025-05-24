import os
from typing import List, Dict, Any
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv, find_dotenv
import openai
from sqlalchemy import create_engine, text, MetaData
from sqlalchemy.exc import SQLAlchemyError
import json
from datetime import datetime
import pymongo
from urllib.parse import urlparse
from fastapi.responses import JSONResponse
import re


# Helper function to get list tables query based on dialect
def get_list_tables_query(dialect: str) -> str:
    """Returns the SQL query for listing tables based on the database dialect."""
    if dialect.startswith("postgres"):
        return "SELECT table_name FROM information_schema.tables WHERE table_schema='public';"
    elif dialect.startswith("mysql"):
        return "SHOW TABLES;"
    elif dialect.startswith("sqlite"):
        return "SELECT name FROM sqlite_master WHERE type='table';"
    else:
        return ""  # Return empty string for unsupported dialects or if no specific query is needed


# Load environment variables for API keys
dotenv_path = find_dotenv()
if dotenv_path:
    load_dotenv(dotenv_path)

# History database (SQLite)
script_dir = os.path.dirname(os.path.abspath(__file__))
history_db_path = os.path.join(script_dir, 'history.db')
history_engine = create_engine(
    f"sqlite:///{history_db_path}", connect_args={"check_same_thread": False}
)
with history_engine.begin() as conn:
    conn.execute(text(
        """
        CREATE TABLE IF NOT EXISTS query_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            question TEXT NOT NULL,
            sql TEXT NOT NULL,
            result TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """
    ))

# OpenAI / Azure OpenAI setup
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
AZURE_OPENAI_API_BASE = os.getenv("AZURE_OPENAI_API_BASE")
AZURE_OPENAI_API_KEY = os.getenv("AZURE_OPENAI_API_KEY")
if OPENAI_API_KEY:
    openai.api_key = OPENAI_API_KEY
    MODEL_NAME = os.getenv("OPENAI_MODEL", "gpt-4o")
elif AZURE_OPENAI_API_BASE and AZURE_OPENAI_API_KEY:
    openai.api_type = "azure"
    openai.api_base = AZURE_OPENAI_API_BASE
    openai.api_version = os.getenv("AZURE_OPENAI_API_VERSION")
    openai.api_key = AZURE_OPENAI_API_KEY
    MODEL_NAME = os.getenv("AZURE_OPENAI_DEPLOYMENT_NAME")
else:
    raise RuntimeError("No OpenAI API key found. Set OPENAI_API_KEY or Azure keys.")

# FastAPI app
app = FastAPI(
    title="Natural Language SQL Assistant",
    description="Translate natural language questions into SQL queries and execute them against your database.",
    version="0.1.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class QueryRequest(BaseModel):
    question: str
    connection_url: str

class QueryResponse(BaseModel):
    sql: str
    columns: List[str]
    rows: List[Dict[str, Any]]

class HistoryItem(BaseModel):
    id: int
    question: str
    sql: str
    rows: List[Dict[str, Any]]
    created_at: str

@app.post("/query", response_model=QueryResponse)
def translate_and_query(req: QueryRequest):
    # Determine database type from connection URL
    parsed = urlparse(req.connection_url)
    scheme = parsed.scheme
    if scheme.startswith("mongodb"):
        # Convert NL to MongoDB query
        try:
            system_prompt = (
                "You are an assistant that converts natural language questions into MongoDB find queries. "
                "Respond with a JSON object with keys 'collection' and 'filter' only, without explanation."
            )
            messages = [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": req.question}
            ]
            response = openai.ChatCompletion.create(
                model=MODEL_NAME, messages=messages, temperature=0, max_tokens=512
            )
            content = response.choices[0].message.content.strip()
            if content.startswith("```"):
                content = "\n".join(content.split("\n")[1:-1]).strip()
            query_def = json.loads(content)
            coll_name = query_def["collection"]
            filter_def = query_def.get("filter", {})
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"Error generating MongoDB query: {e}")

        # Execute MongoDB query
        try:
            client = pymongo.MongoClient(req.connection_url)
            dbname = parsed.path.lstrip("/")
            db = client[dbname]
            docs = list(db[coll_name].find(filter_def))
            rows = docs
            columns = list({k for doc in docs for k in doc.keys()})
        except Exception as e:
            raise HTTPException(status_code=400, detail=f"MongoDB execution error: {e}")
    else:
        # Check if the question is about listing tables
        is_list_tables_query = re.search(r"\b(?:list|show)\b.*\btables?\b", req.question, re.IGNORECASE)
        
        if is_list_tables_query:
            sql_query = get_list_tables_query(scheme)
            if sql_query:  # If a specific query exists for the dialect
                try:
                    engine = create_engine(req.connection_url)
                    with engine.connect() as conn:
                        result = conn.execute(text(sql_query))
                        rows = [dict(r._mapping) for r in result.fetchall()]
                        columns = list(result.keys())
                except Exception as e:
                    raise HTTPException(status_code=400, detail=f"Error executing list tables query: {e}")
            else:
                # Fallback to general SQL generation if dialect not supported by helper or not a list tables query
                is_list_tables_query = False # Ensure we proceed to general SQL generation
        
        if not is_list_tables_query: # If not a list tables query or fallback needed
            # Schema introspection for SQL prompt
            metadata = None
            try:
                engine_inspect = create_engine(req.connection_url)
                metadata = MetaData()
                metadata.reflect(bind=engine_inspect)
                tables_desc = []
                for tbl_name, tbl in metadata.tables.items():
                    cols = [col.name for col in tbl.columns]
                    tables_desc.append(f"{tbl_name}({', '.join(cols)})")
                schema_info = "; ".join(tables_desc)
            except Exception:
                schema_info = ""

            # Convert NL to SQL query
            try:
                # Dialect-specific prompt: SQLite vs general SQL
                if scheme.startswith("sqlite"):
                    system_prompt = (
                        "You are an assistant that converts natural language questions into SQL queries for SQLite databases. "
                        "Use SQLite-specific syntax (e.g., pragma, sqlite_master) for metadata. "
                        "Respond with only the SQL query without explanation or formatting."
                        + (f" This database has the following tables: {schema_info}" if schema_info else "")
                    )
                else:
                    system_prompt = (
                        "You are an assistant that converts natural language questions into SQL queries for SQL databases. "
                        "Respond with only the SQL query without explanation or formatting."
                        + (f" This database has the following tables: {schema_info}" if schema_info else "")
                    )
                messages = [
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": req.question}
                ]
                response = openai.ChatCompletion.create(
                    model=MODEL_NAME, messages=messages, temperature=0, max_tokens=512
                )
                sql_query = response.choices[0].message.content.strip()
                if sql_query.startswith("```"):
                    sql_query = "\n".join(sql_query.split("\n")[1:-1]).strip()
                # Adapt SQLite info_schema queries
                if scheme.startswith("sqlite") and "information_schema.tables" in sql_query.lower():
                    sql_query = "SELECT name FROM sqlite_master WHERE type='table';"
                if not sql_query.endswith(";"):
                    sql_query += ";"
            except Exception as e:
                raise HTTPException(status_code=500, detail=f"Error generating SQL: {e}")

            # Execute SQL query (supports DDL operations)
            try:
                engine = create_engine(req.connection_url)
                # transactional block to handle DDL/DML
                with engine.begin() as conn:
                    result = conn.execute(text(sql_query))
                    if result.returns_rows:
                        rows = [dict(row._mapping) for row in result.fetchall()]
                        columns = list(result.keys())
                    else:
                        # DDL or non-select statement
                        rows = []
                        columns = []
            except Exception as e:
                err_msg = str(e)
                # Handle undefined-table errors gracefully
                if 'does not exist' in err_msg or 'UndefinedTable' in err_msg:
                    available = ', '.join(metadata.tables.keys()) if metadata else ''
                    raise HTTPException(status_code=400, detail=f"{err_msg}. Available tables: {available}")
                raise HTTPException(status_code=400, detail=f"SQL execution error: {err_msg}")

    # Persist to history
    try:
        timestamp = datetime.utcnow().isoformat()
        # Determine what to save as "sql" for history based on query type
        history_sql_entry = content if scheme.startswith("mongodb") else sql_query
        with history_engine.begin() as conn:
            conn.execute(text(
                "INSERT INTO query_history (question, sql, result, created_at) "
                "VALUES (:q, :s, :r, :c)"
            ), {"q": req.question, "s": history_sql_entry, "r": json.dumps(rows), "c": timestamp})
    except Exception:
        pass

    # Return response
    display_query = content if scheme.startswith("mongodb") else sql_query
    return QueryResponse(sql=display_query, columns=columns, rows=rows)

@app.get("/history", response_model=List[HistoryItem])
def get_history(limit: int = 100):
    try:
        with history_engine.connect() as conn:
            res = conn.execute(text(
                "SELECT id, question, sql, result, created_at FROM query_history ORDER BY created_at DESC LIMIT :limit"
            ), {"limit": limit})
            items = []
            # Use .mappings() to get dict-like rows by column name
            for row in res.mappings():
                items.append(HistoryItem(
                    id=row["id"],
                    question=row["question"],
                    sql=row["sql"],
                    rows=json.loads(row["result"]),
                    created_at=row["created_at"]
                ))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error fetching history: {e}")
    return items

@app.exception_handler(Exception)
async def generic_exception_handler(request, exc):
    # Log traceback
    import traceback
    traceback.print_exc()
    # Return error detail in response
    return JSONResponse(status_code=500, content={"detail": str(exc)})
