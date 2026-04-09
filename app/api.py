
import sys
import os
from fastapi import FastAPI, HTTPException, Request
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from contextlib import asynccontextmanager
import uvicorn

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.services.retriever import SPSRetriever
from app.services.schema_adapter import NorthwindSchemaAdapter
from app.services.ask import process_question


class QueryRequest(BaseModel):
    question: str


class SaveRequest(BaseModel):
    question: str
    sql: str
    correct: bool


app_state = {}


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

        print(f"✅ Startup completato. {len(valid_tables)} tabelle pronte all'uso.")
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


@app.get("/api/health")
async def health_check():
    """Restituisce lo stato dell'API."""
    if app_state.get("ready"):
        return {"status": "ok", "message": "I motori LLM e DB sono operativi."}
    return {"status": "error", "message": "Errore in fase di avvio del server."}


@app.post("/api/ask")
async def ask_question(request: QueryRequest):
    """
    Riceve la domanda e restituisce la query SQL generata e i dati.
    """
    if not app_state.get("ready"):
        raise HTTPException(status_code=503, detail="Server non inizializzato correttamente.")

    q = request.question.strip()
    if not q:
        raise HTTPException(status_code=400, detail="La domanda non può essere vuota.")

    res = process_question(
        q=q,
        retriever=app_state["retriever"],
        adapter=app_state["adapter"],
        schema_text=app_state["schema_text"],
        valid_tables=app_state["valid_tables"],
        valid_columns=app_state["valid_columns"]
    )

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

    if not request.correct:
        return {"success": True, "saved": False, "message": "Feedback ricevuto: query non corretta."}

    try:
        saved = app_state["retriever"].add_example(
            request.question, request.sql)
        if not saved:
            return {"success": True, "saved": False, "message": "Query già presente nel pool, non è stata duplicata."}
        return {"success": True, "saved": True, "message": "Query salvata nel pool."}
    except Exception as e:
        raise HTTPException(
            status_code=500, detail=f"Errore salvataggio query: {e}")

if __name__ == "__main__":
    uvicorn.run("app.api:app", host="0.0.0.0", port=8000, reload=False)
