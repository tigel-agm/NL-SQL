# Natural Language SQL Assistant

Use natural language to query or modify your databases (PostgreSQL, MySQL, SQLite, MongoDB), including DDL operations.
Run both backend and frontend together via `python run.py` for a unified start.

## Features

- Convert user questions into SQL using OpenAI/Azure OpenAI.
- Execute SQL queries and return results as JSON.

## Getting Started

### Prerequisites

- Python 3.10+
- PostgreSQL database
- A `.env` file with the following:

```dotenv
OPENAI_API_KEY=sk-...
# or for Azure OpenAI
AZURE_OPENAI_API_BASE=https://<your-endpoint>
AZURE_OPENAI_API_KEY=<key>
AZURE_OPENAI_API_VERSION=2025-01-01-preview
AZURE_OPENAI_DEPLOYMENT_NAME=<deployment_name>
```

### Installation

```bash
pip install -r requirements.txt
```

### Usage

```bash
# Run both backend and frontend concurrently
python run.py
```

Alternatively, to launch only the backend:
```bash
uvicorn main:app --reload --port 8001
```

Send a POST request to `/query`:

```bash
curl -X POST "http://localhost:8001/query" -H "Content-Type: application/json" \
  -d '{"question": "List the top 5 customers by total order value"}'
```

### Streamlit Frontend Usage

In another terminal, run the Streamlit frontend (port 8502):

```bash
streamlit run streamlit_app.py --server.port 8502
```

### Environment Variables for Frontend

You can set `NL_SQL_API_URL` if your backend runs on a different URL:

```bash
# Example for Windows PowerShell
$env:NL_SQL_API_URL = "http://localhost:8001/query"
```

### Response

```json
{
  "sql": "SELECT customer_id, SUM(order_total) as total_value FROM orders GROUP BY customer_id ORDER BY total_value DESC LIMIT 5;",
  "columns": ["customer_id", "total_value"],
  "rows": [
    {"customer_id": 123, "total_value": 9876.54},
  ]
}
```

## Known Issues
- Exporting CSV or rendering charts may refresh the app unexpectedly without any error messages.

## Features (What It Can Do)
- Convert natural language to SQL for PostgreSQL, MySQL, SQLite, MongoDB.
- Quick query presets: list tables, preview rows, count, describe schema, distinct values, top 5 by column.
- ER diagram generation with Graphviz.
- Performance insights via EXPLAIN/EXPLAIN ANALYZE/EXPLAIN QUERY PLAN.
- Data profiling statistics: null counts, distinct counts, min/max/avg.
- Query history logging and retrieval.
- Perform DDL operations (CREATE, ALTER, DROP).

## Limitations (What It Cannot Do)
- No support for multi-statement or complex transactions.
- Limited chart types (line, bar, area) and known CSV/chart refresh bug.
- No advanced authentication or role-based access control.
- Basic error logging; no centralized monitoring.

## Environment Variables & API Requirements
- `DATABASE_URL`: PostgreSQL default connection string (for history DB).
- For Azure OpenAI: `AZURE_OPENAI_API_BASE`, `AZURE_OPENAI_API_KEY`, `AZURE_OPENAI_API_VERSION`, `AZURE_OPENAI_DEPLOYMENT_NAME`.
- For structured DB inputs: `DB_HOST`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `DB_NAME`.
- For SQLite: `DB_SQLITE_PATH`.
- For MongoDB: `DB_MONGO_URI`.
- Frontend URLs: `NL_SQL_API_URL` (default `http://localhost:8001/query`), `NL_SQL_HISTORY_URL` (default `http://localhost:8001/history`).
- `DB_URL`: override full custom connection URL.


## Contributing

Feel free to open issues or submit pull requests!

## License

MIT License

## Authors

- **[tigel-agm](https://github.com/tigel-agm)**

# Acknowledgements

- The developers and maintainers of PostgreSQL, MySQL, SQLite, and MongoDB for creating reliable and widely-used database systems.
- Streamlit
- FastAPI
- OpenAI
- Azure OpenAI
