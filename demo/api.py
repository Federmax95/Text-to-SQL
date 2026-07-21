import sys
import os
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates
from fastapi.responses import StreamingResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel
from contextlib import asynccontextmanager
import uvicorn
import sqlglot
import asyncio
import uuid
import time
import requests as http_requests
from sqlglot import exp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.services.retriever import Retriever
from app.services.schema_adapter import SchemaAdapter
from app.services.ask import process_question
from app.core.config import OLLAMA_URL

class QueryRequest(BaseModel):
    question: str
    session_id: str | None = None
    previous_sql: str | None = None
    user_feedback: str | None = None
    use_baseline: bool = False
    llm_model: str | None = None
    additional_context: str | None = None

class SaveRequest(BaseModel):
    question: str
    sql: str
    correct: bool

class ExecuteSqlRequest(BaseModel):
    sql: str
    db_id: str | None = None

class DbPathRequest(BaseModel):
    db_path: str

app_state = {}
progress_state = {}
UPLOAD_DIR = os.path.join(PROJECT_DIR, "data", "uploaded_dbs")

def get_progress_callback(session_id: str):
    def callback(step: str, message: str = ""):
        progress_state[session_id] = {
            "step": step,
            "message": message,
            "timestamp": time.time()
        }
    return callback

def _json_safe_value(value):
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    return str(value)

def _resolve_db_path(raw_path: str) -> str:
    path = os.path.expandvars(os.path.expanduser(raw_path.strip()))
    if not os.path.isabs(path):
        path = os.path.join(PROJECT_DIR, path)
    return os.path.abspath(path)

def _normalize_db_id(raw_path: str, fallback_name: str | None = None) -> str:
    base_name = fallback_name or os.path.basename(raw_path)
    return os.path.splitext(base_name)[0].lower()

def _load_database(db_path: str, db_name: str | None = None):
    resolved_path = _resolve_db_path(db_path)
    if not os.path.exists(resolved_path):
        raise FileNotFoundError(f"File SQLite non trovato: {resolved_path}")

    adapter = SchemaAdapter(sqlite_path=resolved_path)
    schema_data = adapter.extract_schema()
    schema_text = adapter.schema_to_text(schema_data)
    valid_tables = schema_data["valid_tables"]
    valid_columns = schema_data["valid_columns"]

    if not valid_tables:
        raise RuntimeError("DB vuoto o non valido.")

    app_state["adapter"] = adapter
    app_state["schema_text"] = schema_text
    app_state["valid_tables"] = valid_tables
    app_state["valid_columns"] = valid_columns
    app_state["db_path"] = resolved_path
    app_state["db_name"] = db_name or os.path.basename(resolved_path)
    app_state["db_id"] = _normalize_db_id(resolved_path, app_state["db_name"])
    app_state["db_ready"] = True

@asynccontextmanager
async def lifespan(app: FastAPI):
    print("\n[INIT] Inizializzazione moduli SPS-SQL (Retriever) [DEMO MODE]...")
    try:
        os.makedirs(UPLOAD_DIR, exist_ok=True)
        retriever = Retriever()
        app_state["retriever"] = retriever
        app_state["ready"] = True

        default_db_path = os.environ.get(
            "SQLITE_PATH", os.path.join(
                BASE_DIR, "baseball_1.sqlite")
        )
        if os.path.exists(_resolve_db_path(default_db_path)):
            _load_database(default_db_path)
            print(f"[OK] Startup completato. DB iniziale caricato con {len(app_state['valid_tables'])} tabelle.")
        else:
            app_state["db_ready"] = False
            app_state["db_path"] = None
            print("[WARN] Nessun DB iniziale disponibile.")

    except Exception as e:
        print(f"[ERROR] Errore critico nell'avvio: {e}")
        app_state["ready"] = False
        app_state["db_ready"] = False

    yield
    print("\n[SHUTDOWN] Spegnimento server Text-to-SQL...")

app = FastAPI(
    title="Text-to-SQL API & Web App (DEMO)",
    description="Versione demo con dataset precaricato e senza upload.",
    version="2.0.0",
    lifespan=lifespan
)

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(request=request, name="index.html")

@app.get("/pool")
async def pool_page(request: Request):
    return templates.TemplateResponse(request=request, name="pool.html")

@app.get("/api/health")
async def health_check():
    if not app_state.get("ready"):
        return {"status": "error", "message": "Errore in fase di avvio del server."}

    if not app_state.get("db_ready"):
        return {
            "status": "warning",
            "message": "Retriever attivo, ma DB non configurato.",
            "db_ready": False,
            "db_name": None,
        }

    return {
        "status": "ok",
        "message": "I motori LLM e DB sono operativi.",
        "db_ready": True,
        "db_name": app_state.get("db_name"),
        "db_id": app_state.get("db_id"),
    }

@app.get("/api/models")
async def list_models():
    ollama_base = OLLAMA_URL.rsplit("/api/", 1)[0] if "/api/" in OLLAMA_URL else OLLAMA_URL.rstrip("/")
    tags_url = f"{ollama_base}/api/tags"
    try:
        resp = http_requests.get(tags_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        models = [m["name"] for m in data.get("models", [])]
        return {"success": True, "models": models}
    except Exception as e:
        return {"success": False, "models": [], "error": str(e)}

@app.post("/api/set-db")
async def set_db_path(request: DbPathRequest):
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non inizializzato.")
    raw_path = request.db_path.strip()
    if not raw_path:
        raise HTTPException(status_code=400, detail="Path vuoto.")
    try:
        await run_in_threadpool(_load_database, raw_path)
        return {
            "success": True,
            "message": "Database configurato.",
            "db_name": app_state.get("db_name"),
            "tables": len(app_state.get("valid_tables", [])),
            "db_id": app_state.get("db_id"),
        }
    except Exception as e:
        app_state["db_ready"] = False
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/api/upload-db")
async def upload_db_file(request: Request, filename: str | None = None):
    raise HTTPException(status_code=403, detail="Upload non disponibile nella versione Demo.")

@app.get("/api/progress")
async def get_progress(session_id: str):
    async def event_generator():
        last_state = None
        max_idle_time = 30
        start_time = time.time()

        while True:
            current_time = time.time()
            state = progress_state.get(session_id)
            if state is not None and last_state != state:
                payload = json.dumps(state, ensure_ascii=False)
                yield f"data: {payload}\n\n"
                last_state = dict(state)
                start_time = current_time

            if state is None and (current_time - start_time) > max_idle_time:
                break
            await asyncio.sleep(0.1)

    return StreamingResponse(event_generator(), media_type="text/event-stream", headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"})

@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non pronto.")
    if not app_state.get("db_ready"):
        raise HTTPException(status_code=400, detail="DB non configurato.")

    q = request.question.strip()
    if not q:
        raise HTTPException(status_code=400, detail="Domanda vuota.")

    session_id = request.session_id or str(uuid.uuid4())
    progress_state[session_id] = {"step": "start", "message": "Inizio elaborazione", "timestamp": 0}

    res = await run_in_threadpool(
        process_question,
        q=q,
        retriever=app_state["retriever"],
        adapter=app_state["adapter"],
        schema_text=app_state["schema_text"],
        valid_tables=app_state["valid_tables"],
        valid_columns=app_state["valid_columns"],
        progress_callback=get_progress_callback(session_id),
        previous_sql=request.previous_sql,
        user_feedback=request.user_feedback,
        current_db_id=app_state.get("db_id"),
        use_baseline=request.use_baseline,
        llm_model=request.llm_model,
        additional_context=request.additional_context,
    )

    if session_id in progress_state:
        del progress_state[session_id]

    if res.get("success"):
        return {
            "success": True,
            "sql": res.get("sql", ""),
            "columns": res.get("columns", []),
            "data": res.get("data", []),
            "retrieved": res.get("retrieved", False)
        }
    else:
        return {"success": False, "error": res.get("error", "Errore sconosciuto.")}

@app.post("/api/save")
async def save_query(request: SaveRequest):
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non pronto.")
    # In demo mode, we pretend to save but we don't actually modify the pool.
    return {"success": True, "saved": False, "message": "Query non salvata realmente (versione demo)"}

@app.get("/api/pool")
async def get_pool_examples():
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non pronto.")

    pool_data = app_state["retriever"].pool_data
    rows = []
    for idx, item in enumerate(pool_data):
        rows.append({
            "id": idx,
            "question": item.get("question", ""),
            "query": item.get("query", ""),
            "db_id": item.get("db_id", "northwind"),
            "is_correct": bool(item.get("is_correct", True)),
            "error": item.get("error"),
        })
    return {"success": True, "items": rows}

@app.post("/api/pool/execute")
async def execute_pool_query(request: ExecuteSqlRequest):
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non pronto.")
    if not app_state.get("db_ready"):
        raise HTTPException(status_code=400, detail="DB non configurato.")

    sql = request.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="Query vuota.")

    active_db_id = app_state.get("db_id")
    requested_db_id = (request.db_id or "").strip().lower() or None
    if requested_db_id and active_db_id and requested_db_id != active_db_id:
        raise HTTPException(status_code=400, detail=f"Query per DB '{requested_db_id}', ma attivo è '{active_db_id}'.")
    if requested_db_id and not active_db_id:
        raise HTTPException(status_code=400, detail="Carica il database prima.")

    try:
        ast = sqlglot.parse_one(sql)
        if not isinstance(ast, exp.Select):
            raise HTTPException(status_code=400, detail="Solo query SELECT permesse.")
    except sqlglot.errors.ParseError as e:
        raise HTTPException(status_code=400, detail=f"Errore sintassi SQL: {e}")

    try:
        def _execute():
            with app_state["adapter"]._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(sql)
                cols = [d[0] for d in cursor.description] if cursor.description else []
                rows = cursor.fetchall()
            return cols, rows

        columns, data = await run_in_threadpool(_execute)
        safe_data = [[_json_safe_value(cell) for cell in row] for row in data]

        return {
            "success": True,
            "columns": columns,
            "data": safe_data,
            "row_count": len(safe_data),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    uvicorn.run("demo.api:app", host="0.0.0.0", port=8000, reload=False)
