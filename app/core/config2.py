import os

# Percorsi base
CORE_DIR = os.path.dirname(os.path.abspath(__file__))
APP_DIR = os.path.dirname(CORE_DIR)
PROJECT_DIR = os.path.dirname(APP_DIR)

# Modello e Ollama
LLM_MODEL = "cogito:8B"
OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434/api/generate")

# Cross-Domain RAG: Puntiamo al pool locale
POOL_DIR = os.path.join(PROJECT_DIR, "data", "pool")
POOL_EMBEDDINGS_PATH = os.path.join(POOL_DIR, "pool_embeddings.npy")
POOL_DATA_PATH = os.path.join(POOL_DIR, "pool_data.json")

# Modello di embedding usato per RAG
EMBEDDING_MODEL = "all-mpnet-base-v2"

# Retrieval (Quanti esempi analoghi caricare)
TOP_K = 5

# Risultati
RESULTS_DIR = os.path.join(PROJECT_DIR, "results")