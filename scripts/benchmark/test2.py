import os
import sys
import re
import json
import time
from datetime import datetime


from datasets import load_dataset


BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.core.config2 import LLM_MODEL
from app.services.ask2 import format_results, process_question, call_ollama
from app.services.retriever2 import Retriever
from app.services.schema_adapter2 import SchemaAdapter

RESULTS_DIR = os.path.join(PROJECT_DIR, "results")
CHECKPOINT_EVERY = 10
METHOD = "No-RAG (stjarvie/question_to_sql_with_ddl)"


def compare_sql(generated: str, expected: str) -> bool:
    gen = generated.strip().lower().rstrip(";")
    exp = expected.strip().lower().rstrip(";")
    if gen == exp:
        return True

    prompt = f"""You are an expert SQL evaluator.
Analyze if the following two SQLite queries are semantically equivalent (meaning they will produce the exact same results on the same database).
Consider them EQUIVALENT if they achieve the same logical result, even if they use different approaches.
Specifically, IGNORE minor differences like:
- Casing of identifiers or string values
- Harmless table aliases (e.g. `table AS t`)
- INNER vs CROSS joins (if they yield the same data)
- Extra quotes around identifiers
- `COUNT(*)` vs `COUNT(column)` or `COUNT(id)`
- `MAX(col)` vs `ORDER BY col DESC LIMIT 1`
- `MIN(col)` vs `ORDER BY col ASC LIMIT 1`
- `SELECT *` vs explicitly listing all columns
- Formatting of dashes, hyphens, or spaces in literal values

Query 1 (Generated):
{generated}

Query 2 (Expected Gold Standard):
{expected}

Are they semantically equivalent? Answer ONLY with "YES" or "NO", nothing else.
"""
    try:
        response = call_ollama(prompt, retries=2).strip().upper()
        return response.startswith("YES")
    except Exception as e:
        print(f"Error during LLM comparison: {e}")
        return False


def build_checkpoint_json(matched, total, elapsed, results):
    """Costruisce il JSON di checkpoint nel formato richiesto."""
    accuracy = round(matched / total * 100, 2) if total > 0 else 0.0

    return {
        "timestamp": datetime.now().isoformat(),
        "model": LLM_MODEL,
        "method": METHOD,
        "total_tests": total,
        "execution_accuracy": accuracy,
        "matched": matched,
        "total_time_seconds": round(elapsed, 2),
        "results": results,
    }


def save_checkpoint(data, path):
    """Salva il checkpoint JSON su disco."""
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def slugify_name(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_")


def main() -> None:
    dataset_name = "stjarvie/question_to_sql_with_ddl"
    ds = load_dataset(dataset_name)
    train_split = ds["train"]

    retriever = Retriever()

    correct = 0
    total = len(train_split)
    results = []

    os.makedirs(RESULTS_DIR, exist_ok=True)
    run_stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dataset_slug = slugify_name(dataset_name)
    model_slug = slugify_name(LLM_MODEL)
    checkpoint_path = os.path.join(
        RESULTS_DIR,
        f"{dataset_slug}_{model_slug}_data_{run_stamp}.json",
    )

    start_time = time.time()

    for i in range(total):
        sample = train_split[i]
        print(f"\n{'='*60}")
        print(f"Q{i+1}: {sample['question']}")
        schema_text = str(sample.get("schema", "")).strip()

        q_start = time.time()
        res = process_question(q=sample['question'], retriever=retriever, adapter=None,
                               schema_text=schema_text, valid_columns=None, valid_tables=None, Benchmark=True)
        q_time = round(time.time() - q_start, 2)

        generated = res['sql'].replace("\n", " ")
        expected = sample['sql']
        exact_match = (generated == expected)
        semantic_match = compare_sql(generated, expected)

        is_correct = exact_match or semantic_match

        print(f"  Generated:  {generated}")
        print(f"  Expected:   {expected}")
        print(f"  Exact:      {exact_match}")
        print(f"  Semantic:   {semantic_match}")
        print(f"  ✅ PASS" if is_correct else f"  ❌ FAIL")

        if is_correct:
            correct += 1

        # Aggiungi il risultato alla lista
        result_item = {
            "question": sample['question'],
            "gold_sql": expected,
            "generated_sql": generated,
            "exact_match": exact_match,
            "semantic_match": semantic_match,
            "is_correct": is_correct,
            "error": res.get("error"),
            "time_seconds": q_time,
        }
        results.append(result_item)

        # Checkpoint ogni N query
        processed = i + 1
        if processed % CHECKPOINT_EVERY == 0:
            elapsed = time.time() - start_time
            checkpoint = build_checkpoint_json(
                matched=correct,
                total=processed,
                elapsed=elapsed,
                results=results,
            )
            save_checkpoint(checkpoint, checkpoint_path)
            acc = round(correct / processed * 100, 2)
            print(
                f"\n  💾 Checkpoint salvato ({processed} query) → {os.path.basename(checkpoint_path)}")
            print(f"     Accuracy: {acc}%  |  Tempo: {round(elapsed, 1)}s")

    # Report finale (salva sempre alla fine)
    elapsed = time.time() - start_time
    final = build_checkpoint_json(
        matched=correct,
        total=total,
        elapsed=elapsed,
        results=results,
    )
    save_checkpoint(final, checkpoint_path)

    print(f"\n{'='*60}")
    print(f"Risultato: {correct}/{total} ({correct/total*100:.0f}%)")
    print(f"📁 Report salvato: {checkpoint_path}")


if __name__ == "__main__":
    main()
