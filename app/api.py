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

from app.services.retriever import SPSRetriever
from app.services.schema_adapter import NorthwindSchemaAdapter
from app.services.ask import process_question


class QueryRequest(BaseModel):
    question: str
    session_id: str | None = None


class SaveRequest(BaseModel):
    question: str
    sql: str
    correct: bool


class ExecuteSqlRequest(BaseModel):
    sql: str


app_state = {}
progress_state = {}  # Traccia lo stato di progresso per ogni sessione


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


@asynccontextmanager
async def lifespan(app: FastAPI):
    # --- Startup ---
    print("\n⏳ Inizializzazione moduli SPS-SQL (Retriever e DB)...")
    try:
        retriever = SPSRetriever()
        adapter = NorthwindSchemaAdapter()
        schema_data = adapter.extract_schema()
        schema_text = adapter.schema_to_text(schema_data)

        valid_tables = schema_data["valid_tables"]
        valid_columns = schema_data["valid_columns"]

        if not valid_tables:
            raise RuntimeError("DB vuoto o credenziali non valide.")

        print(
            f"✅ Startup completato. {len(valid_tables)} tabelle pronte all'uso.")
        app_state["retriever"] = retriever
        app_state["adapter"] = adapter
        app_state["schema_text"] = schema_text
        app_state["valid_tables"] = valid_tables
        app_state["valid_columns"] = valid_columns
        app_state["ready"] = True
    except Exception as e:
        print(f"❌ Errore critico nell'avvio: {e}")
        app_state["ready"] = False

    yield
    # --- Shutdown ---
    print("\n👋 Spegnimento server Text-to-SQL...")

# Inizializzazione App FastAPI
app = FastAPI(
    title="Northwind Text-to-SQL API & Web App",
    description="Web App + API per interrogare il DB Northwind in linguaggio naturale.",
    version="1.0.0",
    lifespan=lifespan
)

templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))


@app.get("/")
async def index(request: Request):
    """Restituisce l'interfaccia web (il file HTML)."""
    return templates.TemplateResponse(
        request=request,
        name="index.html"
    )


@app.get("/pool")
async def pool_page(request: Request):
    """Restituisce la pagina web con gli esempi del pool."""
    return templates.TemplateResponse(
        request=request,
        name="pool.html"
    )


@app.get("/api/health")
async def health_check():
    """Restituisce lo stato dell'API."""
    if app_state.get("ready"):
        return {"status": "ok", "message": "I motori LLM e DB sono operativi."}
    return {"status": "error", "message": "Errore in fase di avvio del server."}


@app.get("/api/progress")
async def get_progress(session_id: str):
    """Endpoint SSE che manda gli aggiornamenti di stato di progresso."""
    async def event_generator():
        last_state = None
        max_idle_time = 30  # secondi
        start_time = time.time()

        while True:
            current_time = time.time()

            state = progress_state.get(session_id)
            if state is not None and last_state != state:
                payload = json.dumps(state, ensure_ascii=False)
                yield f"data: {payload}\n\n"
                last_state = dict(state)
                start_time = current_time

            # Se la sessione non ha più stato e siamo rimasti idle, chiudi
            if state is None and (current_time - start_time) > max_idle_time:
                break

            await asyncio.sleep(0.1)  # Controlla ogni 100ms

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
    """
    Riceve la domanda e restituisce la query SQL generata e i dati.
    """
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    q = request.question.strip()
    if not q:
        raise HTTPException(
            status_code=400, detail="La domanda non può essere vuota.")

    # Usa la sessione client (se presente) per allineare stream e richiesta
    session_id = request.session_id or str(uuid.uuid4())
    progress_state[session_id] = {
        "step": "start", "message": "Inizio elaborazione", "timestamp": 0}

    # Passa la callback di stato a process_question
    res = await run_in_threadpool(
        process_question,
        q=q,
        retriever=app_state["retriever"],
        adapter=app_state["adapter"],
        schema_text=app_state["schema_text"],
        valid_tables=app_state["valid_tables"],
        valid_columns=app_state["valid_columns"],
        progress_callback=get_progress_callback(session_id)
    )

    # Pulisci la sessione dal progress_state
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
    """Salva un esempio corretto nel pool RAG quando l'utente conferma."""
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    try:
        saved = await run_in_threadpool(
            app_state["retriever"].add_example,
            request.question,
            request.sql,
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
    """Restituisce tutti gli esempi presenti nel pool con stato correttezza."""
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
    """Esegue una query del pool solo se è una SELECT valida."""
    if not app_state.get("ready"):
        raise HTTPException(
            status_code=503, detail="Server non inizializzato correttamente.")

    sql = request.sql.strip()
    if not sql:
        raise HTTPException(status_code=400, detail="La query SQL è vuota.")

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
                with conn.cursor() as cursor:
                    cursor.execute(sql)
                    cols = [d[0] for d in cursor.description] if cursor.description else []
                    rows = cursor.fetchall()
            return cols, rows

        columns, data = await run_in_threadpool(_execute)

        safe_data = [
            [_json_safe_value(cell) for cell in row]
            for row in data
        ]

        return {
            "success": True,
            "columns": columns,
            "data": safe_data,
            "row_count": len(safe_data),
        }
    except Exception as e:
        return {"success": False, "error": str(e)}

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=False)
