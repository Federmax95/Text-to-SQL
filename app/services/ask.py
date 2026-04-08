"""
🔎 Northwind RAG-SQL (Interactive Mode) - SOLO SELECT
=======================================
Ti permette di fare domande in linguaggio naturale sul DB Northwind MySQL.
Usa l'intelligenza di RAG per estrarre logiche simili dal vecchio Vector Pool 
Spider e applicarle magicamente al nuovo Schema Northwind con sample data.
Genera ESCLUSIVAMENTE query di lettura (SELECT).
"""


import re
import requests
import json
import sqlglot
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.dirname(os.path.dirname(BASE_DIR))
if PROJECT_DIR not in sys.path:
    sys.path.insert(0, PROJECT_DIR)

from app.core.config import LLM_MODEL, OLLAMA_URL, TOP_K
from app.services.retriever import SPSRetriever
from app.services.schema_adapter import NorthwindSchemaAdapter
from sqlglot import exp

DEBUG = False  # True per stampare dettagli di debug, False per produzione

SIMILARITY_THRESHOLD = 0.50  # Similarità con la quale recupra gli esempi dal database


# =========================
# OLLAMA & UTILS
# =========================

import time

def call_ollama(prompt: str) -> str:
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
            timeout=120
        )
        response.raise_for_status()
        return response.json()["response"].strip()
    except:
        print("⏳ Attendo Ollama...")
        time.sleep(3)

    raise ConnectionError("Ollama non raggiungibile.")


def clean_sql(response: str) -> str:
    """Estrae codice SQL puro dai blocchi markdown."""
    response = response.strip()
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```",
                      response, re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return response.strip()


def has_placeholder(sql: str) -> bool:
    """Controlla se la query contiene placeholder (<value>, <id>, ecc.)."""
    return bool(re.search(r"<[^>]+>", sql))


# =========================
# PROMPT BUILDERS
# =========================

def build_cot_prompt(question: str, schema_text: str) -> str:
    """Chain-of-Thought: ragionamento step-by-step prima di generare SQL."""
    return f"""You are a SQL expert. Before writing SQL, reason through the problem step-by-step.

            {schema_text}

            ### Question:
            {question}

            ### Step-by-step reasoning (think out loud):
            1. What are the main entities (tables) involved in the question?
            2. What are the relationships between them (which tables to JOIN)?
            3. What filters (WHERE conditions) apply to the question?
            4. Do we need aggregation (GROUP BY, COUNT, SUM)? If yes, what?
            5. What columns should we return? (exactly the ones asked, no extras)
            6. Do we need HAVING clause, DISTINCT, or LIMIT?
            7. What is the final output format (rows, single number, list)?

            ### Your reasoning:"""


def build_columns_prompt(question: str, schema_text: str, similar: str) -> str:
    return f"""You are a MySQL schema analyzer.
            {schema_text}

            ### Similar conceptual SQL pattern (for inspiration only):
            {similar}

            ### Rules:
            - Output ONLY the columns from the schema above needed to answer the question.
            - Format: table.column (one per line)
            - Include all columns needed for SELECT, JOIN, WHERE, ORDER BY.
            - Do NOT output extra text.

            ### Question:
            {question}

            ### Columns:
            """


def build_sql_prompt(columns: str, question: str, schema_text: str, similar: str) -> str:
    return f"""You are an elite MySQL query generator.
            {schema_text}

            ### Similar Concept Reference:
            {similar}

            ### ━━ CRITICAL RULES ━━
            - Generate ONLY valid MySQL syntax. No explanations. No markdown.
            - ONLY use tables and columns listed in "Allowed Columns" below.
            - Do NOT use `SELECT *` — list columns explicitly.
            - NEVER use placeholders like <value>, <id>. Derive all values from the schema.
            - VERIFY: Query must return the exact columns requested, no more, no less.

            ### ━━ ORDER BY RULES ━━
            - NEVER reference a SELECT alias in ORDER BY.
            - Always REPEAT the full expression:
                CORRECT: ORDER BY COUNT(*) DESC
                WRONG:   ORDER BY enrollment_count DESC   ← alias not allowed in ORDER BY
            - NEVER put aggregate functions (MIN, MAX, COUNT) inside GROUP BY.
                CORRECT: GROUP BY t.transcript_id
                WRONG:   GROUP BY MIN(t.transcript_id)

            ### ━━ AGGREGATION RULES ━━
            - For "most enrolled / most registered / most times": use COUNT(*) on join rows.
            Do NOT use COUNT(DISTINCT col) unless the question explicitly says "distinct students/users".
            - "how many different/distinct" → COUNT(DISTINCT ...)
            - "at least N" → HAVING COUNT(*) >= N
            - "exactly N"  → HAVING COUNT(*) = N
            - "most/top/highest" → ORDER BY COUNT(*) DESC LIMIT 1

            ### ━━ TEXT FILTER RULES ━━
            - Use Sample DB Values section above to pick the EXACT string stored in the DB.
            - When the question says "has the substring X" or "the word X":
            → Use LIKE '%X%' where X is ONLY the key noun. Strip articles: "the computer" → '%computer%'
            - Use LIKE '%value%' as a fallback when you are unsure of exact casing/spelling.
            - For date columns: use BETWEEN or DATE() functions, NOT string comparison.

            ### ━━ SELECT COLUMN RULES ━━
            - Return EXACTLY the columns the question asks for, nothing more.
            If asked for "name and id" → return only those 2 columns, no helper columns.
            - NEVER add extra aggregate aliases (COUNT(*) AS cnt, score, rank) unless explicitly asked.
            - If the question asks for a name, ensure the column is a name (e.g., customer_name, not ID).

            ### ━━ JOIN RULES ━━
            - Use INNER JOIN by default unless the question mentions "all", "even if not", "including those without".
            - LEFT JOIN when you need to keep rows from the left table even if no match.
            - NEVER create Cartesian products — verify foreign keys match the question context.
            - When joining multiple tables, ALWAYS use table aliases (AS t1, AS t2) and prefix ALL columns with table names.
            - Example: SELECT t1.name, t2.value FROM table1 AS t1 JOIN table2 AS t2 ON t1.id = t2.id

            ### ━━ ALIAS RULES ━━
            - When using JOIN, always use aliases for table names.
            - Prefix ALL columns with table aliases to avoid ambiguous column errors.

            ### ━━ SET OPERATION RULES ━━
            - For "X but not Y" or "X that have not" patterns → use EXCEPT
            - For "both X and Y" patterns → use INTERSECT
            - NEVER use UNION without understanding if duplicates should be removed.

            ### ━━ VALIDATION CHECKLIST ━━
            Before outputting the query:
            1. ✓ All table names are in the Allowed Columns
            2. ✓ All column names are in the Allowed Columns
            3. ✓ No SELECT aliases referenced in ORDER BY
            4. ✓ No aggregates inside GROUP BY
            5. ✓ SELECT columns match what the question asks
            6. ✓ WHERE filters use the correct operators (=, LIKE, BETWEEN, >=, etc)

            ### Allowed Columns:
            {columns}

            ### User Question:
            {question}

            ### MySQL Query:
            """


def build_fix_prompt(query: str, error: str, explanation: str, schema_text: str, question: str) -> str:
    return f"""You are a MySQL expert fixing a query.

            ### Schema:
            {schema_text}

            ### Question:
            {question}

            ### Original Query:
            {query}

            ### Problem:
            {explanation}

            ### Error:
            {error}

            ### Fix Rules:
            - Fix the logic, not just syntax.
            - NEVER reference a SELECT alias in ORDER BY — repeat the full expression.
            - NEVER put aggregates inside GROUP BY.
            - Use LIKE '%keyword%' for text (strip articles like 'the', 'a', 'an').
            - Use Sample DB Values from the schema for exact string matches.
            - ONLY output the corrected SQL, nothing else.

            ### Fixed Query:
            """


def build_explain_prompt(query: str, error: str, question: str, schema_text: str) -> str:
    return f"""You are a SQL expert analyzing a failed query.

            ### Database Schema:
            {schema_text}

            ### User Question:
            {question}

            ### Generated SQL:
            {query}

            ### Execution Error or Wrong Result:
            {error}

            Explain in 2-3 sentences WHY the query is wrong. Focus on:
            - wrong aggregation function (COUNT(*) vs COUNT(DISTINCT))
            - alias referenced in ORDER BY without being defined there
            - string filter mismatch (exact vs LIKE, articles in LIKE pattern)
            - wrong JOIN type or missing table
            - extra or missing columns in SELECT

            ### Explanation:
            """


# =========================
# SEMANTIC GUARD
# =========================

def semantic_guard(sql: str, question: str) -> str | None:
    """Pre-validazione semantica della query generata."""
    q = question.lower()
    sql_lower = sql.lower()

    # DISTINCT checks Controlla se manca all'interno di count distinct (problema del modello a gestire più comandi)
    if ("different" in q or "distinct" in q) and "count(" in sql_lower and "distinct" not in sql_lower:
        return "Missing DISTINCT in COUNT — the question asks for distinct values."

    # Comparison operators checks Controlla se gli operandi siano giusti con la richiesta dell'utente
    if "exactly" in q and ">=" in sql:
        return "Should use = instead of >= — the question asks for an exact count."
    if "at least" in q and re.search(r"count.*=\s*\d", sql_lower) and ">=" not in sql:
        return "Should use >= instead of = — the question asks for 'at least N'."
    if "at most" in q and "<" in sql and "<=" not in sql:
        return "Should use <= instead of < — the question asks for 'at most N'."

    # ORDER BY alias check
    order_match = re.search(r'ORDER BY\s+(\w+)', sql, re.IGNORECASE)
    if order_match:
        alias = order_match.group(1).lower()
        if alias not in ('asc', 'desc', 'count', 'sum', 'avg', 'min', 'max') and \
           not re.search(rf'(?:AS|as)\s+{re.escape(alias)}\b', sql[:sql.upper().find('ORDER')]):
            if alias.endswith('_count') or alias.endswith('_score') or alias.endswith('_rank'):
                return f"Alias '{alias}' used in ORDER BY is not valid. Repeat the full expression like COUNT(*) instead."

    # LIMIT checks Controlla se ne restituisce solo uno se chiede il migliore
    if ("top" in q or "highest" in q) and "limit" not in sql_lower:
        return "The question asks for 'top' or 'highest' — add LIMIT."
    # Controlla se conta invece che restituire i dati e basta
    if "how many" in q and "count(" not in sql_lower:
        return "Missing COUNT() for 'how many' question."

    # GROUP BY validation
    # controlla se c'è group by
    if "group by" in sql_lower:
        # prende tutta la parte dopo group by
        group_by_part = re.search(
            r'GROUP BY\s*([^;]*?)(?:HAVING|ORDER|LIMIT|$)', sql, re.IGNORECASE)
        if group_by_part:
            gb_content = group_by_part.group(1).lower()
            # se contiene funzioni queste sono errori
            if any(agg in gb_content for agg in ['count(', 'sum(', 'avg(', 'min(', 'max(']):
                return "GROUP BY should not contain aggregate functions like COUNT(). Put them in SELECT instead."

    return None


# =========================
# POST-PROCESSING SELECT
# =========================

def enforce_select_columns(sql: str, question: str) -> str:
    """Rimuove colonne helper non richieste (count, score, rank) dal SELECT."""
    if "how many" in question.lower():
        return sql
    sql = re.sub(r",\s*COUNT\([^)]*\)\s+AS\s+\w+",
                 "", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?count\b(?!\()", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?score\b", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?rank\b(?!\()", "", sql, flags=re.IGNORECASE)
    return sql.strip()


# =========================
# VALIDAZIONE & FORMATTAZIONE
# =========================

# Controlla che le colonne che ha selezionato LLM esistano effettivamente
def validate_columns(columns_raw: str, valid_tables: set, valid_columns: dict) -> list[str]:
    """Scarta colonne allucinate non presenti in MySQL. Estrae pattern table.column esatti."""
    valid_cols = []
    lines = columns_raw.strip().splitlines()
    if DEBUG:
        print("Colonne raw dal LLM:", lines)
    for line in lines:
        col = line.strip().strip("-").strip().lower()
        if not col or col.startswith("#"):
            continue

        col_clean = col.replace("`", "")
        match = re.search(r"([a-z0-9_]+)\.([a-z0-9_]+)", col_clean)
        if match:
            table_part = match.group(1)
            col_part = match.group(2)
            fullname = f"{table_part}.{col_part}".lower()
            if DEBUG:
                print("Fullname:", fullname)
            # Se esistono le aggiunge in quelle valide
            if table_part in valid_tables and fullname in valid_columns:
                valid_cols.append(fullname)
            else:
                if DEBUG:
                    print(f"    [Scarto] {fullname} (non esiste)")
    return valid_cols

# Controlla che la sql sia valida e solo di SELECT


def validate_sql_syntax(sql: str) -> tuple[bool, str]:
    try:
        ast = sqlglot.parse_one(sql)
        if not isinstance(ast, exp.Select):
            return False, f"Bloccato: Rilevata operazione non consentita ({ast.key.upper()})."
        return True, "Query valida e sicura."
    except sqlglot.errors.ParseError as e:
        return False, f"Errore di sintassi SQL:\n{e}"
    except Exception as e:
        return False, f"Errore imprevisto:\n{e}"


# Esegue la query generata
def execute_mysql(query: str, adapter: NorthwindSchemaAdapter) -> dict:
    try:
        with adapter._get_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                cols = [desc[0]
                        for desc in cursor.description] if cursor.description else []
                data = cursor.fetchall()
                return {"success": True, "columns": cols, "data": data}
    except Exception as e:
        return {"success": False, "error": str(e)}

# Formatta il risultato in json


def format_results(result: dict, max_rows: int = 25) -> str:
    if not result.get("data"):
        return "[]"
    cols = result["columns"]
    data = result["data"][:max_rows]
    json_data = []
    for row in data:
        row_dict = {cols[i]: str(
            val) if val is not None else None for i, val in enumerate(row)}
        json_data.append(row_dict)
    out_json = json.dumps(json_data, indent=2, ensure_ascii=False)
    return out_json


def ask_yes_no(prompt: str) -> bool:
    """Chiede conferma S/N all'utente in modalità interattiva."""
    while True:
        answer = input(prompt).strip().lower()
        if answer in ('s', 'si', 'y', 'yes'):
            return True
        if answer in ('n', 'no'):
            return False
        print("Rispondi 'si' o 'no'.")


# =========================
# CORE APP
# =========================

def process_question(q: str, retriever: SPSRetriever, adapter: NorthwindSchemaAdapter, schema_text: str, valid_tables: set, valid_columns: dict) -> dict:
    """Core logic per ottenere la answer sia dalla CLI che dalle API."""

    # ── PHASE 1: Retrieval RAG  ──
    print("\n⏳ Ricerca pattern logici analoghi nel Vector DB...")
    k = 5
    similars = retriever.retrieve(q, top_k=k)
    max_sim = similars[0]["similarity"] if similars else 0

    if similars:
        print(f"   [Trovato pattern analogo con {max_sim:.2f} di vicinanza]")

    # Formatta gli esempi
    if similars and max_sim >= SIMILARITY_THRESHOLD:
        sim_context = retriever.format_examples(similars)
    else:
        sim_context = ""

    # ── PHASE 2: Chain-of-Thought Reasoning ──
    print("🧠 Ragionamento in corso...")
    try:
        cot_prompt = build_cot_prompt(q, schema_text)
        reasoning = call_ollama(cot_prompt)
        if DEBUG:
            print(f"   💭 {reasoning[:200]}")
    except ConnectionError as ce:
        print(f"❌ {ce}")
        return {"success": False, "error": str(ce)}

    # ── PHASE 3: Column Selection ──
    print("⏳ Identificazione colonne strettamente necessarie...")
    try:
        p_cols = build_columns_prompt(q, schema_text, sim_context)
        cols_raw = call_ollama(p_cols)
    except ConnectionError as ce:
        return {"success": False, "error": str(ce)}

    valid_cols = validate_columns(cols_raw, valid_tables, valid_columns)
    if not valid_cols:
        return {"success": False, "error": "Nessuna colonna legittima identificata per la domanda."}

    # ── PHASE 4: SQL Generation ──
    print("⏳ Generazione Query Target...")
    p_sql = build_sql_prompt("\n".join(valid_cols), q,schema_text, sim_context)
    sql = clean_sql(call_ollama(p_sql))
    sql = enforce_select_columns(sql, q)

    # ── Pre-validation check ──
    pre_check_error = semantic_guard(sql, q)
    if pre_check_error:
        print(f"   🔴 Pre-check: {pre_check_error[:80]}...")

    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            print(f"   🔄 AutoFix #{attempt - 1}...")

        if DEBUG:
            print("SQL: ", sql)

        # ── Placeholder check ──
        if has_placeholder(sql):
            print(f"   ⚠️  Placeholder rilevato, fix forzato...")
            sql = clean_sql(call_ollama(build_fix_prompt(
                query=sql,
                error="Contains placeholders like <value>.",
                explanation="NEVER use placeholders. Derive all values from the schema or use LIKE.",
                schema_text=schema_text,
                question=q,
            )))
            sql = enforce_select_columns(sql, q)
            continue

        # ── Syntax validation (solo SELECT) ──
        is_valid, validation_err = validate_sql_syntax(sql)
        if not is_valid:
            print(f"❌ Bloccato: {validation_err}")
            return {"success": False, "error": validation_err}

        # ── Semantic guard pre-fix ──
        sem_err = semantic_guard(sql, q)
        if sem_err and attempt < max_attempts:
            print(f"   ⚠️ Semantic guard: {sem_err}")
            sql = clean_sql(call_ollama(build_fix_prompt(
                query=sql, error=sem_err, explanation=sem_err,
                schema_text=schema_text, question=q,
            )))
            sql = enforce_select_columns(sql, q)
            continue

        # ── Execution ──
        res = execute_mysql(sql, adapter)
        if res["success"]:
            print(f"\n🖥️  QUERY ESEGUITA:\n   {sql}")
            res["sql"] = sql
            return res
        else:
            err = res["error"]
            print(f"   ❌ Errore (Execution/Syntax): {err[:350]}")
            if attempt < max_attempts:
                explain = call_ollama(
                    build_explain_prompt(sql, err, q, schema_text))
                if DEBUG:
                    print(f"   💡 Diagnosi: {explain[:100]}...")
                p_fix = build_fix_prompt(
                    sql, err, explain, schema_text, question=q)
                sql = clean_sql(call_ollama(p_fix))
                sql = enforce_select_columns(sql, q)

    print("\n❌ Impossibile generare una query MySQL valida (Tentativi esauriti).")
    return {"success": False, "error": "Tentativi esauriti.", "sql": sql}


# =========================
# INTERACTIVE LOOP
# =========================

def interactive_loop():
    print("=" * 70)
    print("🚀  NORTHWIND Text-to-SQL 🚀".center(70))
    print("   Database:   MySQL (schema 'northwind')")
    print(f"   Modello:    {LLM_MODEL}")
    print(f"   RAG Vector: Top {TOP_K} Cross-Domain Pattern da memoria")
    print("=" * 70)

    print("\n[inizializzazione in corso... attendere]")
    try:
        retriever = SPSRetriever()
        adapter = NorthwindSchemaAdapter()
        schema_data = adapter.extract_schema()
        schema_text = adapter.schema_to_text(schema_data)
        valid_tables = schema_data["valid_tables"]
        valid_columns = schema_data["valid_columns"]
        if not valid_tables:
            print(
                "❌ Impossibile leggere il database Northwind. Le credenziali sono corrette?")
            return
    except Exception as e:
        print(f"❌ Errore critico di boot: {e}")
        return

    print(
        f"✅ Pronti. Connessione a Northwind OK ({len(valid_tables)} tabelle caricate con sample data).")
    print("—" * 70)
    print("🗣️  Scrivi pure la tua domanda naturale (oppure 'esci' per chiudere)\n")

    while True:
        try:
            q = input("💬 Domanda: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n👋 Arrivederci!")
            break
        if not q:
            continue
        if q.lower() in ('esci', 'exit', 'quit', 'q'):
            print("👋 Arrivederci!")
            break
        res = process_question(q, retriever, adapter,schema_text, valid_tables, valid_columns)
        if res["success"]:
            print("\n📊 RISULTATI MySQL:")
            print(format_results(res))
            if ask_yes_no("\n✅ La query generata è corretta e vuoi salvarla nel pool RAG? [s/n]: "):
                retriever.add_example(q, res["sql"])
                print("   ✅ Query salvata nel pool.")
        else:
            print("\n❌ ERRORE:", res.get("error", "Sconosciuto"))


if __name__ == "__main__":
    interactive_loop()
