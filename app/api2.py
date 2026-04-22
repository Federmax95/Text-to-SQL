
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
from sqlglot import exp

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.services.retriever2 import Retriever
from app.services.schema_adapter2 import SchemaAdapter
from app.services.ask2 import process_question

class QueryRequest(BaseModel):
    question: str
    session_id: str | None = None
    previous_sql: str | None = None
    user_feedback: str | None = None


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
    """Ritorna una callback che aggiorna lo stato di progresso."""
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
    """Normalizza il path DB supportando input relativi/assoluti."""
    path = os.path.expandvars(os.path.expanduser(raw_path.strip()))
    if not os.path.isabs(path):
        path = os.path.join(PROJECT_DIR, path)
    return os.path.abspath(path)


def _normalize_db_id(raw_path: str, fallback_name: str | None = None) -> str:
    base_name = fallback_name or os.path.basename(raw_path)
    return os.path.splitext(base_name)[0].lower()


def _store_uploaded_db(filename: str, content: bytes) -> tuple[str, str]:
    """Salva il file caricato in una cartella locale e ritorna path assoluto e nome originale."""
    if not content:
        raise ValueError("Il file caricato è vuoto.")

    safe_name = os.path.basename(filename or "database.sqlite")
    ext = os.path.splitext(safe_name)[1].lower()
    allowed_ext = {".sqlite", ".db", ".sqlite3"}
    if ext not in allowed_ext:
        raise ValueError(
            "Formato non supportato. Usa un file .sqlite, .sqlite3 o .db")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    target_name = f"{uuid.uuid4().hex}_{safe_name}"
    target_path = os.path.abspath(os.path.join(UPLOAD_DIR, target_name))

    with open(target_path, "wb") as f:
        f.write(content)

    return target_path, safe_name


def _load_database(db_path: str, db_name: str | None = None):
    """Inizializza adapter e schema per il DB selezionato."""
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
    print("\n⏳ Inizializzazione moduli SPS-SQL (Retriever)...")
    try:
        retriever = Retriever()
        app_state["retriever"] = retriever
        app_state["ready"] = True

        default_db_path = os.environ.get(
            "SQLITE_PATH", os.path.join(
                PROJECT_DIR, "academic", "academic.sqlite")
        )
        if os.path.exists(_resolve_db_path(default_db_path)):
            _load_database(default_db_path)
            print(
                f"✅ Startup completato. DB iniziale caricato con {len(app_state['valid_tables'])} tabelle."
            )
        else:
            app_state["db_ready"] = False
            app_state["db_path"] = None
            print("⚠️ Nessun DB iniziale disponibile. Impostalo da interfaccia grafica.")

    except Exception as e:
        print(f"❌ Errore critico nell'avvio: {e}")
        app_state["ready"] = False
        app_state["db_ready"] = False

    yield
    print("\n👋 Spegnimento server Text-to-SQL...")


app = FastAPI(
    title="Text-to-SQL API & Web App",
    description="Web App + API con selezione DB SQLite da interfaccia grafica.",
    version="2.0.0",
    lifespan=lifespan
)

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/")
async def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index2.html"
    )


@app.get("/pool")
async def pool_page(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="pool.html"
    )


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


@app.post("/api/set-db")
async def set_db_path(request: DbPathRequest):
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503,
            detail="Server non inizializzato correttamente.",
        )

    raw_path = request.db_path.strip()
    if not raw_path:
        raise HTTPException(
            status_code=400, detail="Il path del DB non può essere vuoto.")

    try:
        await run_in_threadpool(_load_database, raw_path)
        return {
            "success": True,
            "message": "Database configurato correttamente.",
            "db_name": app_state.get("db_name"),
            "tables": len(app_state.get("valid_tables", [])),
            "db_id": app_state.get("db_id"),
        }
    except Exception as e:
        app_state["db_ready"] = False
        raise HTTPException(status_code=400, detail=str(e))


@app.post("/api/upload-db")
async def upload_db_file(request: Request, filename: str | None = None):
    """Carica un file SQLite dal browser, lo salva lato server e lo imposta come DB attivo."""
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503,
            detail="Server non inizializzato correttamente.",
        )

    try:
        content = await request.body()
        saved_path, original_name = await run_in_threadpool(
            _store_uploaded_db, filename or "database.sqlite", content
        )
        await run_in_threadpool(_load_database, saved_path, original_name)
        return {
            "success": True,
            "message": "File caricato e database configurato correttamente.",
            "db_name": app_state.get("db_name"),
            "tables": len(app_state.get("valid_tables", [])),
            "db_id": app_state.get("db_id"),
        }
    except Exception as e:
        app_state["db_ready"] = False
        raise HTTPException(status_code=400, detail=str(e))


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

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no"
        }
    )


@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    if not app_state.get("db_ready"):
        raise HTTPException(
            status_code=400, detail="Configura prima un database SQLite dalla UI.")

    q = request.question.strip()
    if not q:
        raise HTTPException(
            status_code=400, detail="La domanda non può essere vuota.")

    session_id = request.session_id or str(uuid.uuid4())
    progress_state[session_id] = {
        "step": "start", "message": "Inizio elaborazione", "timestamp": 0}

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
        return {
            "success": False,
            "error": res.get("error", "Errore sconosciuto.")
        }


@app.post("/api/save")
async def save_query(request: SaveRequest):
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    try:
        saved = await run_in_threadpool(
            app_state["retriever"].add_example,
            request.question,
            request.sql,
            db_id=app_state.get("db_id", "northwind"),
            is_correct=bool(request.correct),
            error=None if request.correct else "Segnata come non corretta dall'utente",
        )
        if not saved:
            return {"success": True, "saved": False, "message": "Query già presente nel pool, non è stata duplicata."}
        return {"success": True, "saved": True, "message": "Query salvata nel pool."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Errore salvataggio query: {e}")


@app.get("/api/pool")
async def get_pool_examples():
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

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
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    if not app_state.get("db_ready"):
        raise HTTPException(
            status_code=400, detail="Configura prima un database SQLite dalla UI.")

    sql = request.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="La query SQL è vuota.")

    active_db_id = app_state.get("db_id")
    requested_db_id = (request.db_id or "").strip().lower() or None
    if requested_db_id and active_db_id and requested_db_id != active_db_id:
        raise HTTPException(
            status_code=400,
            detail=(
                f"La query appartiene al database '{requested_db_id}', ma il database attivo è '{active_db_id}'. "
                "Carica il database corretto prima di rieseguirla."
            ),
        )
    if requested_db_id and not active_db_id:
        raise HTTPException(
            status_code=400,
            detail="Carica il database corretto prima di rieseguire questa query.",
        )

    try:
        ast = sqlglot.parse_one(sql)
        if not isinstance(ast, exp.Select):
            raise HTTPException(
                status_code=400,
                detail="È consentita solo l'esecuzione di query SELECT.",
            )
    except sqlglot.errors.ParseError as e:
        raise HTTPException(
            status_code=400, detail=f"Errore sintassi SQL: {e}")

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
    uvicorn.run("app.api2:app", host="0.0.0.0", port=8000, reload=False)
