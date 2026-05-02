import os
import sys
import json
import numpy as np
from datasets import load_dataset
from sentence_transformers import SentenceTransformer

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.core.config2 import POOL_EMBEDDINGS_PATH, POOL_DATA_PATH, EMBEDDING_MODEL

def build_pool():
    print("📥 Caricamento dataset gretelai/synthetic_text_to_sql...")
    ds = load_dataset("gretelai/synthetic_text_to_sql")
    train_split = ds["train"]
    
    total = len(train_split)
    print(f"📊 Trovati {total} esempi nel training set. Li preparo per il RAG Vector DB...")
    
    # Caricamento Modello
    print(f"🧠 Caricamento modello di embedding ({EMBEDDING_MODEL})...")
    model = SentenceTransformer(EMBEDDING_MODEL)
    
    # Estrazione domande
    questions = [sample['sql_prompt'] for sample in train_split]
    
    print(f"⏳ Estrazione embeddings per {total} domande in corso...")
    print("   (Questa operazione può richiedere svariati minuti a seconda dell'hardware)")
    
    # Processa in batch usando il SentenceTransformer per velocizzare enormemente
    embeddings = model.encode(questions, batch_size=128, show_progress_bar=True, normalize_embeddings=True)
    
    print("💾 Costruzione struttura dati...")
    pool_data = []
    for sample in train_split:
        pool_data.append({
            "question": sample['sql_prompt'],
            "query": sample['sql'],
            "db_id": "gretelai",
            "is_correct": True,
            "error": None
        })
        
    print(f"📁 Salvataggio dei dati in {POOL_DATA_PATH} ...")
    os.makedirs(os.path.dirname(POOL_DATA_PATH), exist_ok=True)
    
    with open(POOL_DATA_PATH, "w", encoding="utf-8") as f:
        json.dump(pool_data, f, ensure_ascii=False, indent=2)
        
    print(f"📁 Salvataggio degli embeddings in {POOL_EMBEDDINGS_PATH} ...")
    np.save(POOL_EMBEDDINGS_PATH, embeddings)
    print("✅ Completato! Pool RAG aggiornato con i 100.000 esempi di training.")

if __name__ == "__main__":
    build_pool()
