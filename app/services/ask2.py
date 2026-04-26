"""
🔎 Text to SQL
=======================================
Ti permette di fare domande in linguaggio naturale su di un dataset a scelta in SQLite.
Estrae lo schema del dataset mandato in input e lo formatta per mandarlo all'llm.
Usa un vector pool per estrarre gli esempi più vicini alla domanda dell'utente (RAG).
Genera ESCLUSIVAMENTE query di lettura (SELECT).
"""
import time
from sqlglot import exp
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

from app.core.config2 import LLM_MODEL, OLLAMA_URL, TOP_K
from app.services.retriever2 import Retriever
from app.services.schema_adapter2 import SchemaAdapter


DEBUG = False  # True per stampare dettagli di debug, False per produzione

# Similarità con la quale recupera gli esempi dal database
SIMILARITY_THRESHOLD = 0.50


# =========================
# OLLAMA & UTILS
# =========================

# Funzione per chiamare l'api di ollama


def call_ollama(prompt: str, retries=5) -> str:
    last_error = None
    for attempt in range(retries):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={"model": LLM_MODEL, "prompt": prompt, "stream": False},
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            if "response" not in data:
                raise ValueError(f"Risposta malformata: {data}")
            return data["response"].strip()
        except requests.exceptions.Timeout:
            last_error = "Timeout"
        except requests.exceptions.ConnectionError as e:
            last_error = f"Connessione fallita: {e}"
        except requests.exceptions.HTTPError as e:
            last_error = f"HTTP Error {e.response.status_code}"
            if e.response.status_code in (401, 403, 404):
                break  # Non ha senso riprovare
        except (ValueError, KeyError) as e:
            last_error = f"Risposta non valida: {e}"
        
        wait = 2 ** attempt  # Backoff esponenziale: 1s, 2s, 4s, 8s, 16s
        print(f"⏳ Attendo {wait}s prima del tentativo {attempt + 2}/{retries}...")
        time.sleep(wait)
    
    raise ConnectionError(f"Ollama non raggiungibile dopo {retries} tentativi. Ultimo errore: {last_error}")


def clean_sql(response: str) -> str:
    """Estrae codice SQL puro dai blocchi markdown."""
    # Rimuovo gli spazi ad inizio e fine stringa
    response = response.strip()
    # cerco all'interno del prompt la query sql generata
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```",response, re.DOTALL | re.IGNORECASE)
    if match:
        # se l'ho trovata la restituisco
        return match.group(1).strip()
    return response.strip()


# =========================
# PROMPT BUILDERS
# =========================

def _detect_question_features(question: str) -> dict:
    """Analizza la domanda e restituisce feature booleane per prompt dinamici."""
    q = question.lower()
    return {
        "is_aggregation":  any(w in q for w in ["how many", "count", "total", "sum", "average"]),
        "is_superlative":  any(w in q for w in ["most", "least", "top", "highest", "lowest", "best", "worst"]),
        "is_distinct":     any(w in q for w in ["different", "distinct", "unique"]),
        "is_negation":     any(w in q for w in ["not", "never", "without", "except", "excluding"]),
        "is_comparative":  any(w in q for w in ["more than", "less than", "at least", "at most", "exactly"]),
        "has_literal":     bool(re.search(r"'\S+'|\d+-\d+|\d+\.\d+%", q)),
        "is_ranking": any(w in q for w in ["rank", "order", "sorted"]) or bool(re.search(r"top\s+\d+", q)),
        "needs_join":      any(w in q for w in ["with", "and their", "along with", "related"]),
    }


def build_cot_prompt(question: str, schema_text: str) -> str:
    feats = _detect_question_features(question)

    # Costruisci solo le domande di ragionamento rilevanti
    reasoning_steps = [
            "1. Which tables are needed? List only tables from the schema.",
            "2. Which JOINs are required? Specify the exact foreign key columns.",
            # ↓ Separazione esplicita — previene Case 4
            "3. What column(s) should appear in SELECT (what to RETURN)?",
            "4. What column(s) and values should appear in WHERE (what to FILTER by)?",
            "   Note: a value mentioned in the question is usually a FILTER, not a return value.",
            "   Example: 'List the week for record 0-1' → SELECT week, WHERE record = '0-1'",
        ]
    if feats["is_aggregation"] or feats["is_superlative"]:
        reasoning_steps.append("4. Which aggregation function is needed (COUNT/SUM/AVG/MIN/MAX)?")
    if feats["is_distinct"]:
        reasoning_steps.append("5. Is DISTINCT or COUNT(DISTINCT) required?")
    if feats["is_comparative"]:
        reasoning_steps.append("6. What is the exact comparison operator (=, >=, <=, BETWEEN)?")
    if feats["has_literal"]:
        reasoning_steps.append("7. Are there literal values with hyphens/parentheses? Treat them as exact strings.")
    if feats["is_superlative"]:
        reasoning_steps.append("8. Use ORDER BY ... DESC LIMIT 1 — do NOT use a subquery unless necessary.")
    if feats["is_negation"]:
        reasoning_steps.append("9. Use EXCEPT or NOT IN/NOT EXISTS for exclusion logic.")

    steps_text = "\n".join(reasoning_steps)

    return f"""You are an expert SQLite analyst. Reason step-by-step before writing any SQL.

            {schema_text}

            ### Question:
            {question}

            ### Reasoning steps (answer each relevant step concisely):
            {steps_text}

            ### Output format — use EXACTLY this structure:
            TABLES: <comma-separated list of tables needed>
            JOINS: <join conditions, or NONE>
            FILTERS: <WHERE conditions, or NONE>
            AGGREGATION: <aggregation needed, or NONE>
            OUTPUT_COLS: <columns to return>
            NOTES: <any special handling (literals, DISTINCT, EXCEPT, etc.), or NONE>

            ### Your reasoning:"""


def build_columns_prompt(question: str,schema_text: str,similar: str,reasoning: str = "") -> str:

    reasoning_summary = ""
    if reasoning:
        relevant = [
            line for line in reasoning.splitlines()
            if any(line.startswith(k) for k in ("TABLES:", "JOINS:", "FILTERS:", "OUTPUT_COLS:"))
        ]
        reasoning_summary = "\n".join(relevant)

    # Few-shot con esempi ESPLICITI del formato corretto e di quello sbagliato
    few_shot = """### Output format — study these examples carefully:

                    CORRECT:
                    author.aid
                    author.name
                    writes.pid

                    WRONG (never do this):
                    aid          ← missing table prefix
                    name         ← missing table prefix
                    author.*     ← wildcard not allowed
                    author.aid, author.name  ← comma-separated not allowed, one per line only"""

    return f"""You are a SQLite schema analyst. List the columns needed to answer the question.

                {schema_text}

                {few_shot}

                ### Reference pattern:
                {similar if similar else "N/A"}

                ### Extracted reasoning:
                {reasoning_summary if reasoning_summary else "N/A"}

                ### Strict rules:
                - Format is ALWAYS: tablename.columnname (one per line, nothing else)
                - NEVER write a column name without its table prefix
                - NEVER write explanations, comments, or extra text
                - NEVER use wildcards like table.*
                - ONLY use tables and columns that exist in the schema above

                ### Question:
                {question}

                ### Columns (table.column format, one per line):"""
# ask2.py — aggiungilo prima di build_sql_prompt




def schema_to_text_with_hints(schema_data: dict) -> str:
    """
    Sovrascrive schema_to_text aggiungendo hint espliciti
    per colonne con spazi o caratteri speciali.
    """
    lines = []
    for table in schema_data.get("tables", []):
        lines.append(f"Table: {table['name']}")
        for col in table.get("columns", []):
            col_name = col["name"]
            needs_quoting = " " in col_name or "-" in col_name or col_name[0].isdigit()
            if needs_quoting:
                lines.append(f'  - "{col_name}"   ← MUST be quoted exactly like this in SQL')
            else:
                lines.append(f"  - {col_name}")
    return "\n".join(lines)

def build_sql_prompt(columns, question, schema_text, similar, reasoning=""):
    feats = _detect_question_features(question)


    # ── Regole dinamiche: solo quelle rilevanti ──
    rules = [
        "Output ONLY the SQL query. No explanations, no markdown, no backticks.",
        "Use only tables and columns from 'Allowed Columns' below.",
        "Never use SELECT * — list columns explicitly.",
    ]

    if feats["is_aggregation"]:
        rules.append("Use COUNT(*) for 'how many rows'. Use COUNT(DISTINCT col) only if the question says 'different' or 'distinct'.")
    if feats["is_superlative"]:
        rules.append("For 'most/highest/top': ORDER BY [expression] DESC LIMIT 1. Never reference a SELECT alias in ORDER BY — repeat the full expression.")
    if feats["is_distinct"]:
        rules.append("Use COUNT(DISTINCT col) — the question explicitly asks for distinct values.")
    if feats["is_comparative"]:
        rules.append("Map 'at least N' → HAVING COUNT(*) >= N | 'exactly N' → = N | 'at most N' → <= N.")
    if feats["has_literal"]:
        rules.append("Values with hyphens (e.g. '0-1') or parentheses (e.g. '7.5%') are EXACT strings. Use WHERE col = '0-1', never split them.")
    if feats["is_negation"]:
        rules.append("For 'X but not Y' or 'X that never': use EXCEPT or NOT EXISTS rather than complex WHERE chains.")
    if feats["needs_join"]:
        rules.append("Use INNER JOIN by default. Use LEFT JOIN only if the question implies 'all X, even without Y'.")

    # Regola sempre presente (ORDER BY alias è errore frequente)
    rules.append("NEVER put aggregate functions (COUNT, MIN, MAX) inside GROUP BY.")

    rules_text = "\n".join(f"- {r}" for r in rules)

    # Estrai solo OUTPUT_COLS e NOTES dal reasoning
    output_hint = ""
    if reasoning:
        for line in reasoning.splitlines():
            if line.startswith(("OUTPUT_COLS:", "NOTES:")):
                output_hint += line + "\n"

    few_shot = ""
    if similar:
        few_shot = f"""### Reference pattern (structural guide only — adapt to this question):
                    {similar}
                    """

    return f"""You are an elite SQLite query generator.

            {schema_text}


            {few_shot}
            ### Reasoning summary:
            {output_hint.strip() if output_hint else "N/A"}

            ### Rules:
            {rules_text}

            ### Allowed Columns:
            {columns}

            ### Question:
            {question}

            ### SQLite Query:"""


def build_fix_prompt(
    query: str,
    error: str,
    explanation: str,
    schema_text: str,
    question: str,
    reasoning: str = ""
) -> str:
    feats = _detect_question_features(question)

    # Regole di fix contestuali
    fix_hints = []
    if "ORDER BY" in query.upper() and feats["is_superlative"]:
        fix_hints.append("ORDER BY must repeat the full expression (e.g. COUNT(*)), never a SELECT alias.")
    if "GROUP BY" in query.upper():
        fix_hints.append("GROUP BY must not contain aggregate functions — move them to SELECT.")
    if feats["has_literal"]:
        fix_hints.append("Compound values like '0-1' must be matched with = '0-1', not split into numbers.")
    if "LIKE" in query.upper():
        fix_hints.append("Strip articles from LIKE patterns: 'the computer' → '%computer%'.")
    if feats["is_distinct"] and "DISTINCT" not in query.upper():
        fix_hints.append("Add DISTINCT or COUNT(DISTINCT col) — question asks for unique values.")

    fix_hints_text = "\n".join(f"- {h}" for h in fix_hints) if fix_hints else "- Fix the root cause, not just the syntax."

    return f"""You are a SQLite expert. Fix the query below — correct the LOGIC, not just the syntax.

                ### Schema:
                {schema_text}

                ### Question:
                {question}

                ### Prior reasoning:
                {reasoning.strip() if reasoning else "N/A"}

                ### Failing Query:
                {query}

                ### Error:
                {error}

                ### Diagnosis:
                {explanation}

                ### Targeted fix hints:
                {fix_hints_text}

                ### Output: ONLY the corrected SQL query, nothing else.

                ### Fixed Query:"""


def build_explain_prompt(query: str,error: str,question: str,schema_text: str) -> str:
    return f"""You are diagnosing a failed SQLite query. Be concise and precise.

                ### Schema:
                {schema_text}

                ### Question:
                {question}

                ### Generated Query:
                {query}

                ### Error or Wrong Result:
                {error}

                ### Diagnose the ROOT CAUSE in max 2 sentences. Choose the most likely category:
                - Wrong aggregation: COUNT(*) vs COUNT(DISTINCT), missing GROUP BY
                - ORDER BY alias: alias not defined at ORDER BY evaluation time
                - Text filter: exact match vs LIKE, article in LIKE pattern, wrong casing
                - Wrong JOIN: INNER vs LEFT, missing table, wrong foreign key
                - Structural: wrong columns in SELECT, extra/missing columns
                - Literal value: compound value split incorrectly (e.g. '0-1' → 0 and 1)

                ### Root cause:"""

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
    sql = re.sub(r",\s*COUNT\([^)]*\)\s+AS\s+\w+","", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?count\b(?!\()", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?score\b", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r",\s*\w*_?rank\b(?!\()", "", sql, flags=re.IGNORECASE)
    return sql.strip()


# =========================
# VALIDAZIONE & FORMATTAZIONE
# =========================

# Controlla che le colonne che ha selezionato LLM esistano effettivamente
def validate_columns(
    columns_raw: str,
    valid_tables: set,
    valid_columns: dict
) -> list[str]:
    """
    Valida le colonne restituite dall'LLM.
    Se una colonna è ambigua (senza prefisso tabella), tenta il recovery automatico.
    """
    valid_cols = []
    ambiguous = []  # Colonne senza prefisso tabella
    lines = columns_raw.strip().splitlines()

    for line in lines:
        col = line.strip().strip("-").strip().lower().replace("`", "")
        if not col or col.startswith("#"):
            continue

        # ── Caso 1: formato corretto table.column ──
        match = re.search(r"([a-z0-9_]+)\.([a-z0-9_]+)", col)
        if match:
            table_part = match.group(1)
            col_part   = match.group(2)
            fullname   = f"{table_part}.{col_part}"
            if table_part in valid_tables and fullname in valid_columns:
                valid_cols.append(fullname)
            else:
                if DEBUG:
                    print(f"[Scarto] {fullname} — non esiste nello schema")
            continue

        # ── Caso 2: solo nome colonna senza tabella (modelli piccoli) ──
        # Cerca in quali tabelle esiste una colonna con quel nome
        col_only = re.sub(r"[^a-z0-9_]", "", col)  # Rimuovi caratteri non validi
        if col_only:
            ambiguous.append(col_only)

    if DEBUG:
        print(f"Colonne validate: {valid_cols}")

    return valid_cols



def validate_sql_syntax(sql: str, valid_tables: set | None = None) -> tuple[bool, str]:
    try:
        statements = sqlglot.parse(sql, read="sqlite")

        # ── Controllo statement multipli ──
        if not statements:
            return False, "Nessuno statement SQL rilevato."
        if len(statements) > 1:
            return False, "Bloccato: rilevati statement multipli."

        ast = statements[0]

        # ── Controllo tipo operazione ──
        if not isinstance(ast, exp.Select):
            return False, f"Bloccato: operazione non consentita ({ast.key.upper()})."

        # ── Whitelist tabelle (solo se valid_tables è fornito e non vuoto) ──
        if valid_tables:  # None e set() vuoto vengono entrambi saltati
            used_tables = {t.name.lower() for t in ast.find_all(exp.Table)}
            forbidden = used_tables - valid_tables
            if forbidden:
                return False, f"Tabelle non autorizzate rilevate: {forbidden}"

        return True, "Query valida e sicura."

    except sqlglot.errors.ParseError as e:
        return False, f"Errore di sintassi SQL: {e}"
    except Exception as e:
        return False, f"Errore imprevisto durante la validazione: {e}"


# Esegue la query generata
def execute_query(query: str, adapter: SchemaAdapter) -> dict:
    try:
        with adapter._get_connection() as conn:
            cursor = conn.cursor()
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


def ask_db_path() -> str:
    """Chiede il path del DB SQLite e risolve eventuali percorsi relativi."""
    raw = input(f"📁 Path database SQLite: ").strip()
    chosen = raw.strip().strip('"').strip("'")
    chosen = os.path.expandvars(os.path.expanduser(chosen))
    if not os.path.isabs(chosen):
        chosen = os.path.join(PROJECT_DIR, chosen)
    return os.path.abspath(chosen)


# =========================
# CORE APP
# =========================

def process_question(q: str, retriever: Retriever, adapter: SchemaAdapter, schema_text: str, valid_tables: set, valid_columns: dict, progress_callback=None, previous_sql: str | None = None, user_feedback: str | None = None, current_db_id: str | None = None, Benchmark: bool | None = None) -> dict:
    """Core logic per ottenere la answer sia dalla CLI che dalle API."""
    def notify_progress(step: str, message: str = ""):
        if progress_callback:
            progress_callback(step, message)

    augmented_question = q
    if user_feedback:
        augmented_question = (
            f"{q}\n\n"
            f"Additional user feedback about previous wrong SQL:\n{user_feedback.strip()}"
        )

    notify_progress("step-1", "Ricerca pattern nel Vector DB...")
    print("\n⏳ Ricerca pattern logici analoghi nel Vector DB...")
    k = 5
    similars = retriever.retrieve(q, top_k=k, db_id=current_db_id)
    max_sim = similars[0]["similarity"] if similars else 0

    if similars:
        print(f"   [Trovato pattern analogo con {max_sim:.2f} di vicinanza]")

    # Formatta gli esempi
    if similars and max_sim >= SIMILARITY_THRESHOLD:
        sim_context = retriever.format_examples(similars)
    else:
        sim_context = ""
    if previous_sql:
        sim_context = (
            f"{sim_context}\n\n"
            "/* Previous wrong SQL (do not repeat this error) */\n"
            f"{previous_sql}"
        )
    if max_sim > 0.90:
        sim_context = f"/* Query molto simile, usala come riferimento */\n{similars[0]['query']}"

    # ── PHASE 2: Chain-of-Thought Reasoning ──
    notify_progress("step-2", "Ragionamento chain-of-thought...")
    print("🧠 Ragionamento in corso...")
    try:
        cot_prompt = build_cot_prompt(augmented_question, schema_text)
        reasoning = call_ollama(cot_prompt)
        reasoning = reasoning.strip()
        if DEBUG:
            print(f"   💭 {reasoning[:200]}")
    except ConnectionError as ce:
        print(f"❌ {ce}")
        return {"success": False, "error": str(ce)}

    # ── PHASE 3: Column Selection ──
    notify_progress("step-3", "Identificazione colonne necessarie...")
    print("⏳ Identificazione colonne strettamente necessarie...")
    try:
        p_cols = build_columns_prompt(augmented_question, schema_text, sim_context, reasoning)
        cols_raw = call_ollama(p_cols)
    except ConnectionError as ce:
        return {"success": False, "error": str(ce)}
    if(valid_tables and valid_columns):
        print("Cols Raw:", cols_raw)
        print("Tables:", valid_tables)
        print("Columns", valid_columns)
        valid_cols = validate_columns(cols_raw, valid_tables, valid_columns)
        if not valid_cols:
            return {"success": False, "error": "Nessuna colonna legittima identificata per la domanda."}

    # ── PHASE 4: SQL Generation ──
    notify_progress("step-4", "Generazione query MySQL...")
    print("⏳ Generazione Query Target...")
    if(Benchmark is None):
        p_sql = build_sql_prompt("\n".join(valid_cols), augmented_question, schema_text, sim_context, reasoning)
    else:
        p_sql = build_sql_prompt("\n".join(cols_raw), augmented_question, schema_text, sim_context, reasoning)
    sql = clean_sql(call_ollama(p_sql))
    sql = enforce_select_columns(sql, augmented_question)
    

    # ── Pre-validation check ──
    pre_check_error = semantic_guard(sql, augmented_question)
    if pre_check_error:
        print(f"   🔴 Pre-check: {pre_check_error[:80]}...")

    max_attempts = 6
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            print(f"   🔄 AutoFix #{attempt - 1}...")
            notify_progress("step-4", f"Tentativo di fix #{attempt - 1}...")
        if DEBUG:
            print("SQL: ", sql)

        # ── Syntax validation (solo SELECT) ──
        is_valid, validation_err = validate_sql_syntax(sql, valid_tables=valid_tables)
        if not is_valid:
            print(f"❌ Bloccato: {validation_err}")
            return {"success": False, "error": validation_err, "sql": sql}

        # ── Semantic guard pre-fix ──
        sem_err = semantic_guard(sql, augmented_question)
        if sem_err and attempt < max_attempts:
            print(f"   ⚠️ Semantic guard: {sem_err}")
            sql = clean_sql(call_ollama(build_fix_prompt(query=sql, error=sem_err, explanation=sem_err,schema_text=schema_text, question=augmented_question, reasoning=reasoning,)))
            sql = enforce_select_columns(sql, augmented_question)
            continue

        # ── Execution ──

        if(Benchmark is None):
            res = execute_query(sql, adapter)
            if res.get("success"):  # Usa .get() per sicurezza
                print(f"\n🖥️  QUERY ESEGUITA:\n   {sql}")
                res["sql"] = sql
                res["retrieved"] = False
                return res
            else:
                err = res["error"]
                print(f"   ❌ Errore (Execution/Syntax): {err[:350]}")
                if attempt < max_attempts:
                    explain = call_ollama(build_explain_prompt(
                        sql, err, augmented_question, schema_text))
                    if DEBUG:
                        print(f"   💡 Diagnosi: {explain[:100]}...")
                    p_fix = build_fix_prompt(sql, err, explain, schema_text, question=augmented_question, reasoning=reasoning)
                    sql = clean_sql(call_ollama(p_fix))
                    sql = enforce_select_columns(sql, augmented_question)
        else:
            return {"sql": sql}

    print("\n❌ Impossibile generare una query SQLite valida (Tentativi esauriti).")
    return {"success": False, "error": "Tentativi esauriti.", "sql": sql}


# =========================
# INTERACTIVE LOOP
# =========================

def interactive_loop():
    print("=" * 70)
    print("🚀 Text-to-SQL 🚀".center(70))
    print(f"   Modello:    {LLM_MODEL}")
    print(f"   RAG Vector: Top {TOP_K} Cross-Domain Pattern da memoria")
    print("=" * 70)

    print("\n[inizializzazione in corso... attendere]")
    try:
        retriever = Retriever()
        sqlite_path = ask_db_path()
        if not os.path.exists(sqlite_path):
            print(f"❌ File SQLite non trovato: {sqlite_path}")
            return
        adapter = SchemaAdapter(sqlite_path=sqlite_path)
        schema_data = adapter.extract_schema()
        schema_text = adapter.schema_to_text(schema_data)
        valid_tables = schema_data["valid_tables"]
        valid_columns = schema_data["valid_columns"]
        if not valid_tables:
            print("❌ Impossibile leggere il database SQLite.")
            return
    except Exception as e:
        print(f"❌ Errore critico di boot: {e}")
        return

    print(
        f"✅ Pronti. Connessione a {sqlite_path} OK ({len(valid_tables)} tabelle caricate con sample data).")
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
        current_db_id = os.path.splitext(
            os.path.basename(sqlite_path))[0].lower()
        res = process_question(q, retriever, adapter,
                               schema_text, valid_tables, valid_columns,
                               current_db_id=current_db_id)
        if res["success"]:
            print("\n📊 RISULTATI SQLite:")
            print(format_results(res))
            if res["retrieved"] == False:
                if ask_yes_no("\n✅ La query generata è corretta? [s/n]: "):
                    retriever.add_example(
                        q, res["sql"], db_id=current_db_id, is_correct=True)
                else:
                    retriever.add_example(
                        q, res["sql"], db_id=current_db_id, is_correct=False)
        else:
            print("\n❌ ERRORE:", res.get("error", "Sconosciuto"))
            failed_sql = res.get("sql")
            if failed_sql:
                retriever.add_example(q, failed_sql, db_id=current_db_id,
                                      is_correct=False, error=res.get("error", "Sconosciuto"))
                print("   📝 Query salvata automaticamente nel pool con stato: ERRATA.")


if __name__ == "__main__":
    interactive_loop()
