import os
from getpass import getpass

# Percorsi base
CORE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(CORE_DIR)
PROJECT_DIR = os.path.dirname(APP_DIR)

# Modello e Ollama
LLM_MODEL = "qwen2.5-coder"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")

# Database MySQL Config
db_pass = os.environ.get("DB_PASSWORD")
if db_pass is None:
    db_pass = getpass("Inserisci la password del database MySQL: ")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "localhost"),
    "user": os.environ.get("DB_USER", "root"),
    "password": db_pass,
    "database": os.environ.get("DB_NAME", "northwind")
}

# Cross-Domain RAG: Puntiamo al pool locale costruito tramite spider_data
POOL_DIR = os.path.join(PROJECT_DIR, "data", "pool")
POOL_EMBEDDINGS_PATH = os.path.join(POOL_DIR, "pool_embeddings.npy")
POOL_DATA_PATH = os.path.join(POOL_DIR, "pool_data.json")

# Modello di embedding usato per RAG (deve combaciare con quello usato per generare pool_embeddings.npy)
EMBEDDING_MODEL = "all-mpnet-base-v2"

# Retrieval (Quanti esempi analoghi caricare)
TOP_K = 5

# Risultati
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")