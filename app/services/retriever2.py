"""
🔍 SPS-SQL — Retriever
========================
Dato un input in linguaggio naturale, recupera i K esempi
più simili dal pool pre-costruito usando la similarità coseno.
"""

import json
import os
import numpy as np
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from app.core.config2 import (
    POOL_EMBEDDINGS_PATH, POOL_DATA_PATH, EMBEDDING_MODEL, TOP_K)


class SPSRetriever:
    """Retriever per trovare le domande SQL più simili nel pool."""

    def __init__(self):
        """Carica il modello di embedding, gli embeddings e i metadati del pool."""
        print(f"  🔍 Caricamento retriever Text-to-SQL...")

        # Carica modello di embedding
        self.model = SentenceTransformer(EMBEDDING_MODEL)

        # Carica embeddings pre-calcolati, oppure inizializza un pool vuoto
        if os.path.exists(POOL_EMBEDDINGS_PATH) and os.path.exists(POOL_DATA_PATH):
            self.embeddings = np.load(POOL_EMBEDDINGS_PATH)
            with open(POOL_DATA_PATH, "r", encoding="utf-8") as f:
                self.pool_data = json.load(f)
            print(
                f"  ✅ Retriever pronto: {len(self.pool_data)} esempi nel pool")
        else:
            print("  ⚠️  Pool RAG non trovato. Il retriever userà un pool vuoto.")
            embedding_dim = self.model.get_sentence_embedding_dimension()
            self.embeddings = np.zeros((0, embedding_dim), dtype=np.float32)
            self.pool_data = []

    def retrieve(self, question: str, top_k: int = None, db_id: str | None = None) -> list[dict]:
        """
        Trova i K esempi più simili alla domanda data.

        Args:
            question: Domanda in linguaggio naturale
            top_k: Numero di risultati (default: config.TOP_K)

        Returns:
            Lista di dict con: question, query, db_id, similarity
        """
        if top_k is None:
            top_k = TOP_K

        if len(self.pool_data) == 0 or self.embeddings.size == 0:
            return []

        # Usa solo esempi validi/corretti per evitare di contaminare il retrieval.
        valid_indices = [
            i for i, item in enumerate(self.pool_data)
            if item.get("is_correct", True)
            and (db_id is None or item.get("db_id", "northwind") == db_id)
        ]
        if not valid_indices:
            return []

        # Encode la domanda
        query_emb = self.model.encode([question], normalize_embeddings=True)

        # Calcola similarità coseno sul sottoinsieme di esempi validi
        filtered_embeddings = self.embeddings[valid_indices]
        similarities = cosine_similarity(query_emb, filtered_embeddings)[0]

        # Ordina per similarità decrescente e prendi i top-K
        top_indices = np.argsort(similarities)[::-1][:top_k]

        results = []
        for idx in top_indices:
            pool_idx = valid_indices[idx]
            results.append({
                "question": self.pool_data[pool_idx]["question"],
                "query": self.pool_data[pool_idx]["query"],
                "db_id": self.pool_data[pool_idx].get("db_id", "northwind"),
                "similarity": float(similarities[idx]),
            })

        return results

    def format_examples(self, examples: list[dict]) -> str:
        """
        Formatta gli esempi recuperati come testo per il prompt.

        Args:
            examples: Lista di risultati da retrieve()

        Returns:
            Stringa formattata con gli esempi
        """
        if not examples:
            return ""

        parts = []
        for i, ex in enumerate(examples, 1):
            parts.append(
                f"/* Esempio {i} (similarità: {ex['similarity']:.2f}) */\n"
                f"-- Domanda: {ex['question']}\n"
                f"{ex['query']}"
            )

        return "\n\n".join(parts)

    def _normalize_text(self, text: str) -> str:
        return " ".join(text.strip().lower().split())

    def example_exists(self, question: str, query: str, db_id: str = "northwind", is_correct: bool = True) -> bool:
        normalized_question = self._normalize_text(question)
        normalized_query = self._normalize_text(query)
        for item in self.pool_data:
            same_question = self._normalize_text(
                item.get("question", "")) == normalized_question
            same_query = self._normalize_text(
                item.get("query", "")) == normalized_query
            same_db = item.get("db_id", "northwind") == db_id
            same_quality = bool(item.get("is_correct", True)
                                ) == bool(is_correct)
            if same_question and same_query and same_db and same_quality:
                return True
        return False

    def add_example(
        self,
        question: str,
        query: str,
        db_id: str = "northwind",
        is_correct: bool = True,
        error: str | None = None,
    ) -> bool:
        """Aggiunge un esempio al pool (corretto o errato) e aggiorna embedding + file sul disco."""
        if self.example_exists(question, query, db_id=db_id, is_correct=is_correct):
            print(f"  ⚠️  Esempio già presente nel pool: '{question}'")
            return False

        new_emb = self.model.encode([question], normalize_embeddings=True)

        # Aggiorna pool in memoria
        self.pool_data.append({
            "question": question,
            "query": query,
            "db_id": db_id,
            "is_correct": bool(is_correct),
            "error": error if error else None,
        })
        self.embeddings = np.vstack([self.embeddings, new_emb])

        # Assicura che la cartella del pool esista
        pool_dir = os.path.dirname(POOL_DATA_PATH)
        os.makedirs(pool_dir, exist_ok=True)

        # Salva su disco per persistenza
        with open(POOL_DATA_PATH, "w", encoding="utf-8") as f:
            json.dump(self.pool_data, f, ensure_ascii=False, indent=2)

        np.save(POOL_EMBEDDINGS_PATH, self.embeddings)

        quality_label = "corretto" if is_correct else "errato"
        print(
            f"  ✅ Esempio aggiunto al pool ({quality_label}): '{question}' (db_id={db_id})"
        )
        return True
