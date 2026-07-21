import sqlite3
import os
import glob
import argparse
import sys
import uuid
import gc
import json
import time
from datetime import datetime
from enum import Enum
import pandas as pd
from datasets import load_dataset
import re

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(BASE_DIR)
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.core.config import LLM_MODEL
from app.services.ask import process_question
from app.services.schema_adapter import SchemaAdapter
from app.services.retriever import Retriever

if sys.stdout.encoding != 'utf-8':
    sys.stdout.reconfigure(encoding='utf-8')


class ResultStatus(Enum):
    OK = "✅ OK"
    SKIPPED_NO_DATA = "⏭️  SKIPPED (no INSERT in schema)"
    SKIPPED_EMPTY_RESULT = "⏭️  SKIPPED (empty result)"
    SCHEMA_ERROR = "❌ SCHEMA_ERROR"
    SQL_ERROR = "❌ SQL_ERROR"


def check_df_equal(df1: pd.DataFrame, df2: pd.DataFrame, query: str = "") -> bool:
    if df1 is None or df2 is None:
        return False
    if df1.shape != df2.shape:
        return False
    try:
        def normalize(df):
            # 1. Converti a float se possibile, altrimenti string
            df = df.copy()
            for col in df.columns:
                try:
                    df[col] = pd.to_numeric(df[col]).round(4)
                except (ValueError, TypeError):
                    df[col] = df[col].astype(str).str.strip().str.lower()
            
            # 2. Rinomina colonne a indice numerico (ignora alias)
            df.columns = range(df.shape[1])
            
            # 3. Ordina le colonne (ignora column order)
            df = df.reindex(sorted(df.columns), axis=1)
            
            # 4. Ordina le righe se non è esplicitamente richiesto un ORDER BY
            has_order_by = bool(re.search(r'\border\s+by\b', query, re.IGNORECASE))
            if not has_order_by:
                df = df.sort_values(
                    by=list(df.columns), 
                    key=lambda x: x.astype(str)
                ).reset_index(drop=True)
            
            return df

        return normalize(df1).equals(normalize(df2))

    except Exception:
        return False


def get_database_string(conn: sqlite3.Connection) -> str:
    tables = pd.read_sql_query(
        """
        SELECT name
        FROM sqlite_master
        WHERE type = 'table' AND name NOT LIKE 'sqlite_%'
        ORDER BY name
        """,
        conn,
    )

    output = []
    for table_name in tables["name"]:
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', conn)
        if df.empty:
            continue
        output.append(f"[{table_name}]")
        output.append(df.to_string(index=False) + "\n")
    return "\n".join(output)


def run_sample(sample: dict, retriever: Retriever) -> dict:
    schema_text = str(sample.get("sql_context", "")).strip()
    gold_sql = sample["sql"]
    prompt = sample["sql_prompt"]
    query_complexity = sample.get("sql_complexity", "")

    temp_dir = os.path.join(BASE_DIR, "temp_dbs")
    os.makedirs(temp_dir, exist_ok=True)
    db_path = os.path.join(
        temp_dir, f"temp_ablation_{uuid.uuid4().hex}.sqlite")

    result = {
        "prompt": prompt,
        "gold_sql": gold_sql,
        "query_complexity": query_complexity,
        "status": None,
        "df": None,
        "error": None,
        "schema_text": schema_text,
        "dataset_str": "",
        
        "baseline_sql": None, "baseline_correct": False, "baseline_time": 0.0, "baseline_df": None,
        "columns_sql": None, "columns_correct": False, "columns_time": 0.0, "columns_df": None,
        "few_shot_sql": None, "few_shot_correct": False, "few_shot_time": 0.0, "few_shot_df": None,
        "base_loop_sql": None, "base_loop_correct": False, "base_loop_time": 0.0, "base_loop_df": None, "base_loop_judge": 0,
        "improved_sql": None, "improved_correct": False, "improved_time": 0.0, "improved_df": None,
        "complete_sql": None, "complete_correct": False, "complete_time": 0.0, "complete_df": None, "complete_judge": 0,
    }

    if "INSERT" not in schema_text.upper():
        result["status"] = ResultStatus.SKIPPED_NO_DATA
        return result

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.executescript(schema_text)
    except Exception as e:
        result["status"] = ResultStatus.SCHEMA_ERROR
        result["error"] = str(e)
        conn.close()
        return result

    try:
        df_gold = pd.read_sql_query(gold_sql, conn)
        if df_gold is None or df_gold.empty:
            result["status"] = ResultStatus.SKIPPED_EMPTY_RESULT
            conn.close()
            return result
        else:
            result["status"] = ResultStatus.OK
            result["df"] = df_gold
            result["dataset_str"] = get_database_string(conn)
    except Exception as e:
        result["status"] = ResultStatus.SQL_ERROR
        result["error"] = str(e)
        conn.close()
        return result

    conn.close()

    adapter = SchemaAdapter(sqlite_path=db_path)
    try:
        schema_data = adapter.extract_schema()
        valid_tables = schema_data["valid_tables"]
        valid_columns = schema_data["valid_columns"]
    except Exception as e:
        result["status"] = ResultStatus.SCHEMA_ERROR
        result["error"] = f"SchemaAdapter error: {str(e)}"
        return result

    def evaluate_pass(use_baseline, use_rag, use_cols, use_improved_prompt, max_attempts) -> tuple[str, bool, float, int, pd.DataFrame]:
        start_t = time.time()
        res = process_question(
            q=prompt,
            retriever=retriever,
            adapter=adapter,
            schema_text=schema_text,
            valid_tables=valid_tables,
            valid_columns=valid_columns,
            use_baseline=use_baseline,
            use_rag=use_rag,
            use_cols=use_cols,
            use_improved_prompt=use_improved_prompt,
            max_attempts_override=max_attempts
        )
        elapsed = round(time.time() - start_t, 2)
        judge_calls = res.get("judge_calls", 0)

        sql = res.get("sql", "")
        if not sql:
            return sql, False, elapsed, judge_calls, None
        
        is_correct = False
        df_pipe = None
        try:
            conn = sqlite3.connect(db_path)
            df_pipe = pd.read_sql_query(sql, conn)
            is_correct = check_df_equal(df_gold, df_pipe, gold_sql)
        except Exception:
            pass
        finally:
            conn.close()
            
        return sql, is_correct, elapsed, judge_calls, df_pipe

    # 1. Baseline
    result["baseline_sql"], result["baseline_correct"], result["baseline_time"], _, result["baseline_df"] = evaluate_pass(
        use_baseline=True, use_rag=False, use_cols=False, use_improved_prompt=False, max_attempts=1)
    
    # 2. Columns (+Cols)
    result["columns_sql"], result["columns_correct"], result["columns_time"], _, result["columns_df"] = evaluate_pass(
        use_baseline=False, use_rag=False, use_cols=True, use_improved_prompt=False, max_attempts=1)

    # 3. Few Shot (+RAG)
    result["few_shot_sql"], result["few_shot_correct"], result["few_shot_time"], _, result["few_shot_df"] = evaluate_pass(
        use_baseline=False, use_rag=True, use_cols=False, use_improved_prompt=False, max_attempts=1)

    # 4. Baseline + Loop
    result["base_loop_sql"], result["base_loop_correct"], result["base_loop_time"], result["base_loop_judge"], result["base_loop_df"] = evaluate_pass(
        use_baseline=True, use_rag=False, use_cols=False, use_improved_prompt=False, max_attempts=6)

    # 5. Improved Prompt
    result["improved_sql"], result["improved_correct"], result["improved_time"], _, result["improved_df"] = evaluate_pass(
        use_baseline=False, use_rag=True, use_cols=True, use_improved_prompt=True, max_attempts=1)

    # 6. Complete (+Autocorrection)
    result["complete_sql"], result["complete_correct"], result["complete_time"], result["complete_judge"], result["complete_df"] = evaluate_pass(
        use_baseline=False, use_rag=True, use_cols=True, use_improved_prompt=True, max_attempts=6)

    try:
        gc.collect()
        if os.path.exists(db_path):
            os.remove(db_path)
    except:
        pass

    return result


def print_result(i: int, r: dict) -> None:
    print(f"Q{i + 1}: {r['prompt']}")
    if r.get("query_complexity"):
        print(f"Complexity: {r['query_complexity']}")
    print(f"Status : {r['status'].value}")
    if r["error"]:
        print(f"Error  : {r['error']}")

    print("\n── Dataset creato dallo schema ──\n")
    print(r.get("dataset_str", ""))

    print("\n--- GOLD QUERY ---")
    print(r["gold_sql"])
    if r["df"] is not None:
        print("Data:")
        print(r["df"].to_string(index=False) if not r["df"].empty else "(empty result set)")

    for phase in ["baseline", "columns", "few_shot", "base_loop", "improved", "complete"]:
        print(f"\n--- {phase.upper()} QUERY ---")
        print(r[f"{phase}_sql"] if r[f"{phase}_sql"] else "(none)")
        df = r[f"{phase}_df"]
        if df is not None:
            print("Data:")
            print(df.to_string(index=False) if not df.empty else "(empty result set)")
        else:
            print("Data: (error or none)")
        print("Time   : ", f"{r[f'{phase}_time']}s")
        if phase in ["base_loop", "complete"]:
            print(f"Judge calls: {r.get(f'{phase}_judge', 0)}")
        print("Correct: ", "✅ YES" if r[f"{phase}_correct"] else "❌ NO")

    print("=" * 60 + "\n")


def save_checkpoint(out_file: str, printed_count: int, counts: dict, results_list: list, dataset_name: str) -> None:
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "model": LLM_MODEL,
        "method": f"Ablation ({dataset_name})",
        "total_tests": printed_count,
        "counts": counts,
        "accuracies": {k: round((v / printed_count) * 100, 2) if printed_count > 0 else 0 for k, v in counts.items()},
        "results": results_list
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)


def find_latest_checkpoint(results_dir: str, dataset_slug: str, model_slug: str) -> str | None:
    pattern = os.path.join(results_dir, f"ablation_{dataset_slug}_{model_slug}_*.json")
    files = glob.glob(pattern)
    if not files:
        return None
    files.sort(key=os.path.getmtime, reverse=True)
    return files[0]


def load_checkpoint(file_path: str) -> dict | None:
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        results = data.get("results", [])
        printed = data.get("total_tests", len(results))
        counts = data.get("counts", {"baseline": 0, "columns": 0, "few_shot": 0, "base_loop": 0, "improved": 0, "complete": 0})
        processed_prompts = set()
        for r in results:
            q = r.get("question") or r.get("prompt")
            if q:
                processed_prompts.add(q)
        return {
            "results_list": results,
            "printed": printed,
            "counts": counts,
            "processed_prompts": processed_prompts,
        }
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Run ablation benchmark and optionally resume from checkpoint")
    parser.add_argument("--new", action="store_true", help="Force creating a new results file even if a checkpoint exists")
    parser.add_argument("--out", type=str, help="Specify output JSON file path to write results to")
    args = parser.parse_args()
    
    dataset_name = "gretelai/synthetic_text_to_sql"
    ds = load_dataset(dataset_name, split="test")
    stats = {s: 0 for s in ResultStatus}
    printed = 0

    print("Inizializzazione Retriever per Ablation...")
    retriever = Retriever()

    counts = {"baseline": 0, "columns": 0, "few_shot": 0, "base_loop": 0, "improved": 0, "complete": 0}
    results_list = []

    dataset_slug = dataset_name.replace("/", "_").replace("-", "_")
    model_slug = str(LLM_MODEL).replace(":", "-").replace("/", "-")
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = os.path.join(BASE_DIR, "results")
    os.makedirs(results_dir, exist_ok=True)

    if args.out:
        out_file = args.out
        os.makedirs(os.path.dirname(out_file) or results_dir, exist_ok=True)
        print(f"I risultati verranno salvati in: {out_file}\n")
        processed_prompts = set()
    else:
        last_file = None if args.new else find_latest_checkpoint(results_dir, dataset_slug, model_slug)
        if last_file:
            print(f"Trovato checkpoint esistente: {last_file}")
            ck = load_checkpoint(last_file)
            if ck:
                out_file = last_file
                results_list = ck["results_list"]
                printed = ck["printed"]
                counts = ck["counts"]
                processed_prompts = ck["processed_prompts"]
            else:
                out_filename = f"ablation_{dataset_slug}_{model_slug}_{date_str}.json"
                out_file = os.path.join(results_dir, out_filename)
                print(f"Impossibile caricare checkpoint, creerò nuovo file: {out_file}")
                processed_prompts = set()
        else:
            out_filename = f"ablation_{dataset_slug}_{model_slug}_{date_str}.json"
            out_file = os.path.join(results_dir, out_filename)
            print(f"I risultati verranno salvati in: {out_file}\n")
            processed_prompts = set()

    for i in range(len(ds)):
        if printed >= 1000:
            break

        sample = ds[i]
        sample_prompt = None
        if isinstance(sample, dict):
            sample_prompt = sample.get("sql_prompt") or sample.get("prompt")
        if sample_prompt and sample_prompt in processed_prompts:
            continue

        r = run_sample(sample, retriever)
        stats[r["status"]] += 1

        if r["status"] != ResultStatus.OK:
            continue

        if r["baseline_correct"]: counts["baseline"] += 1
        if r["columns_correct"]: counts["columns"] += 1
        if r["few_shot_correct"]: counts["few_shot"] += 1
        if r["base_loop_correct"]: counts["base_loop"] += 1
        if r["improved_correct"]: counts["improved"] += 1
        if r["complete_correct"]: counts["complete"] += 1

        results_list.append({
            "question": r["prompt"],
            "query_complexity": r.get("query_complexity", ""),
            "gold_sql": r["gold_sql"],
            
            "baseline_sql": r["baseline_sql"], "baseline_correct": r["baseline_correct"], "baseline_time_seconds": r.get("baseline_time", 0.0),
            "columns_sql": r["columns_sql"], "columns_correct": r["columns_correct"], "columns_time_seconds": r.get("columns_time", 0.0),
            "few_shot_sql": r["few_shot_sql"], "few_shot_correct": r["few_shot_correct"], "few_shot_time_seconds": r.get("few_shot_time", 0.0),
            "base_loop_sql": r["base_loop_sql"], "base_loop_correct": r["base_loop_correct"], "base_loop_time_seconds": r.get("base_loop_time", 0.0), "base_loop_judge_calls": r.get("base_loop_judge", 0),
            "improved_sql": r["improved_sql"], "improved_correct": r["improved_correct"], "improved_time_seconds": r.get("improved_time", 0.0),
            "complete_sql": r["complete_sql"], "complete_correct": r["complete_correct"], "complete_time_seconds": r.get("complete_time", 0.0), "complete_judge_calls": r.get("complete_judge", 0)
        })

        try:
            if r.get("prompt"):
                processed_prompts.add(r.get("prompt"))
        except Exception:
            pass

        print_result(printed, r)
        printed += 1

        if printed % 10 == 0:
            save_checkpoint(out_file, printed, counts, results_list, dataset_name)
            print(f"\n💾 Checkpoint salvato: {printed} query completate.\n")

    print("── Summary ──")
    for status, count_val in stats.items():
        if count_val:
            print(f"  {status.value}: {count_val}")

    print("\n── Accuracies ──")
    if printed > 0:
        for phase, count_val in counts.items():
            print(f"  {phase.upper()}: {count_val} / {printed} ({(count_val/printed)*100:.2f}%)")
    else:
        print("  Nessuna query valida elaborata.")

    if printed > 0:
        save_checkpoint(out_file, printed, counts, results_list, dataset_name)
    print(f"\nRisultati finali salvati in: {out_file}")


if __name__ == "__main__":
    main()
