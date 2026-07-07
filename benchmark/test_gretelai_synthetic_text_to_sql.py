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
        temp_dir, f"temp_benchmark_{uuid.uuid4().hex}.sqlite")

    result = {
        "prompt": prompt,
        "gold_sql": gold_sql,
        "query_complexity": query_complexity,
        "status": None,
        "df": None,
        "error": None,
        "schema_text": schema_text,
        "baseline_sql": None,
        "baseline_correct": False,
        "baseline_time": 0.0,
        "baseline_df": None,
        "pipeline_sql": None,
        "pipeline_correct": False,
        "pipeline_time": 0.0,
        "pipeline_df": None,
        "dataset_str": "",
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

    # Baseline Generation
    start_time = time.time()
    res_baseline = process_question(
        q=prompt,
        retriever=retriever,
        adapter=adapter,
        schema_text=schema_text,
        valid_tables=valid_tables,
        valid_columns=valid_columns,
        use_baseline=True
    )
    result["baseline_time"] = round(time.time() - start_time, 2)
    baseline_sql = res_baseline.get("sql", "")
    result["baseline_sql"] = baseline_sql
    if baseline_sql:
        try:
            conn = sqlite3.connect(db_path)
            df_base = pd.read_sql_query(baseline_sql, conn)
            result["baseline_df"] = df_base
            result["baseline_correct"] = check_df_equal(df_gold, df_base, gold_sql)
        except:
            pass
        finally:
            conn.close()

    # Pipeline Generation
    start_time = time.time()
    res_pipeline = process_question(
        q=prompt,
        retriever=retriever,
        adapter=adapter,
        schema_text=schema_text,
        valid_tables=valid_tables,
        valid_columns=valid_columns,
        use_baseline=False
    )
    result["pipeline_time"] = round(time.time() - start_time, 2)
    pipeline_sql = res_pipeline.get("sql", "")
    result["pipeline_sql"] = pipeline_sql
    if pipeline_sql:
        try:
            conn = sqlite3.connect(db_path)
            df_pipe = pd.read_sql_query(pipeline_sql, conn)
            result["pipeline_df"] = df_pipe
            result["pipeline_correct"] = check_df_equal(df_gold, df_pipe, gold_sql)
        except:
            pass
        finally:
            conn.close()

    try:
        gc.collect()  # Ensure loose connections are closed
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
        print(r["df"].to_string(index=False)
              if not r["df"].empty else "(empty result set)")

    print("\n--- BASELINE QUERY ---")
    print(r["baseline_sql"] if r["baseline_sql"] else "(none)")
    if r["baseline_df"] is not None:
        print("Data:")
        print(r["baseline_df"].to_string(index=False)
              if not r["baseline_df"].empty else "(empty result set)")
    else:
        print("Data: (error or none)")
    print("Correct: ", "✅ YES" if r["baseline_correct"] else "❌ NO")

    print("\n--- PIPELINE QUERY ---")
    print(r["pipeline_sql"] if r["pipeline_sql"] else "(none)")
    if r["pipeline_df"] is not None:
        print("Data:")
        print(r["pipeline_df"].to_string(index=False)
              if not r["pipeline_df"].empty else "(empty result set)")
    else:
        print("Data: (error or none)")
    print("Correct: ", "✅ YES" if r["pipeline_correct"] else "❌ NO")

    print("=" * 60 + "\n")


def save_checkpoint(out_file: str, printed_count: int, baseline_count: int, pipeline_count: int, results_list: list, dataset_name: str) -> None:
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "model": LLM_MODEL,
        "method": f"RAG ({dataset_name})",
        "total_tests": printed_count,
        "baseline_accuracy": round((baseline_count / printed_count) * 100, 2) if printed_count > 0 else 0,
        "pipeline_accuracy": round((pipeline_count / printed_count) * 100, 2) if printed_count > 0 else 0,
        "baseline_matched": baseline_count,
        "pipeline_matched": pipeline_count,
        "results": results_list
    }
    with open(out_file, "w", encoding="utf-8") as f:
        json.dump(output_data, f, indent=2, ensure_ascii=False)


def find_latest_checkpoint(results_dir: str, dataset_slug: str, model_slug: str) -> str | None:
    pattern = os.path.join(results_dir, f"{dataset_slug}_{model_slug}_*.json")
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
        baseline = data.get("baseline_matched", 0)
        pipeline = data.get("pipeline_matched", 0)
        processed_prompts = set()
        for r in results:
            q = r.get("question") or r.get("prompt")
            if q:
                processed_prompts.add(q)
        return {
            "results_list": results,
            "printed": printed,
            "baseline_correct_count": baseline,
            "pipeline_correct_count": pipeline,
            "processed_prompts": processed_prompts,
        }
    except Exception:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run benchmark and optionally resume from checkpoint")
    parser.add_argument("--new", action="store_true",
                        help="Force creating a new results file even if a checkpoint exists")
    parser.add_argument(
        "--out", type=str, help="Specify output JSON file path to write results to")
    args = parser.parse_args()
    dataset_name = "gretelai/synthetic_text_to_sql"
    ds = load_dataset(dataset_name, split="test")
    stats = {s: 0 for s in ResultStatus}
    printed = 0

    print("Inizializzazione Retriever...")
    retriever = Retriever()

    baseline_correct_count = 0
    pipeline_correct_count = 0
    results_list = []

    # Naming the file dynamically
    dataset_slug = dataset_name.replace("/", "_").replace("-", "_")
    model_slug = str(LLM_MODEL).replace(":", "-").replace("/", "-")
    date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    results_dir = os.path.join(BASE_DIR, "results")
    os.makedirs(results_dir, exist_ok=True)

    # Choose output file handling: explicit path > resume unless --new > create new
    if args.out:
        out_file = args.out
        # ensure directory exists
        os.makedirs(os.path.dirname(out_file) or results_dir, exist_ok=True)
        print(f"I risultati verranno salvati in: {out_file}\n")
        processed_prompts = set()
    else:
        last_file = None if args.new else find_latest_checkpoint(
            results_dir, dataset_slug, model_slug)
        if last_file:
            print(f"Trovato checkpoint esistente: {last_file}")
            ck = load_checkpoint(last_file)
            if ck:
                out_file = last_file
                results_list = ck["results_list"]
                printed = ck["printed"]
                baseline_correct_count = ck["baseline_correct_count"]
                pipeline_correct_count = ck["pipeline_correct_count"]
                processed_prompts = ck["processed_prompts"]
            else:
                out_filename = f"{dataset_slug}_{model_slug}_{date_str}.json"
                out_file = os.path.join(results_dir, out_filename)
                print(
                    f"Impossibile caricare checkpoint, creerò nuovo file: {out_file}")
                processed_prompts = set()
        else:
            out_filename = f"{dataset_slug}_{model_slug}_{date_str}.json"
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

        if r["baseline_correct"]:
            baseline_correct_count += 1
        if r["pipeline_correct"]:
            pipeline_correct_count += 1

        results_list.append({
            "question": r["prompt"],
            "query_complexity": r.get("query_complexity", ""),
            "gold_sql": r["gold_sql"],
            "baseline_sql": r["baseline_sql"],
            "baseline_correct": r["baseline_correct"],
            "baseline_time_seconds": r.get("baseline_time", 0.0),
            "pipeline_sql": r["pipeline_sql"],
            "pipeline_correct": r["pipeline_correct"],
            "pipeline_time_seconds": r.get("pipeline_time", 0.0)
        })
        # Mark prompt as processed to avoid duplicates on resume
        try:
            if r.get("prompt"):
                processed_prompts.add(r.get("prompt"))
        except Exception:
            pass

        print_result(printed, r)
        printed += 1

        if printed % 10 == 0:
            save_checkpoint(out_file, printed, baseline_correct_count,
                            pipeline_correct_count, results_list, dataset_name)
            print(f"\n💾 Checkpoint salvato: {printed} query completate.\n")

    print("── Summary ──")
    for status, count in stats.items():
        if count:
            print(f"  {status.value}: {count}")

    print(f"\n  Baseline Correct: {baseline_correct_count} / {printed}")
    print(f"  Pipeline Correct: {pipeline_correct_count} / {printed}")

    if printed > 0:
        save_checkpoint(out_file, printed, baseline_correct_count,
                        pipeline_correct_count, results_list, dataset_name)
    print(f"\nRisultati finali salvati in: {out_file}")


if __name__ == "__main__":
    main()
