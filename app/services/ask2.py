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



# =========================
# OLLAMA & UTILS
# =========================

# Funzione per chiamare l'api di ollama


def call_ollama(prompt: str, retries=5, model=None) -> str:

    """Chiama l'API REST locale di Ollama con backoff esponenziale:
    attende 1s, 2s, 4s, 8s, 16s tra i tentativi
    Gestisce timeout, errori di connessione e risposte malformate"""

    last_error = None
    target_model = model or LLM_MODEL
    for attempt in range(retries):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={"model": target_model, "prompt": prompt, "stream": False},
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


def build_baseline_prompt(question: str, schema_text: str) -> str:
    """Baseline zero-shot prompt per generare SQL direttamente."""
    return f"""{schema_text}

            ### User Question:
            {question}

            ### SQLite Query:
            """


def build_columns_prompt(question: str, schema_text: str, similar: str, reasoning: str = "") -> str:
    return f"""You are a SQLite schema analyzer.
            {schema_text}

            ### Similar conceptual SQL pattern (for inspiration only):
            {similar}

            ### Prior reasoning context (from previous step):
            {reasoning}

            ### Rules:
            - Output ONLY the columns from the schema above needed to answer the question.
            - Format: table.column (one per line)
            - Include all columns needed for SELECT, JOIN, WHERE, ORDER BY.
            - Do NOT output extra text.

            ### Question:
            {question}

            ### Columns:
            """

def build_sql_prompt(columns: str, question: str, schema_text: str, similar: str, reasoning: str = "") -> str:
    return f"""You are an elite SQLite query generator.
            {schema_text}

            ### Similar Concept Reference:
            {similar}

            ### Prior reasoning context (from previous step):
            {reasoning}

            ### ━━ CRITICAL RULES ━━
            - Generate ONLY valid SQLite syntax. No explanations. No markdown.
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

            ### SQLite Query:
            """


def build_fix_prompt(query: str, error: str, explanation: str, schema_text: str, question: str, reasoning: str = "") -> str:
    return f"""You are a SQLite expert fixing a query.

            ### Schema:
            {schema_text}

            ### Question:
            {question}

            ### Prior reasoning context (from previous step):
            {reasoning}

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
    """    Controlli PRIMA dell'esecuzione:
    ✓ "different" nella domanda → COUNT(DISTINCT) obbligatorio
    ✓ "exactly" → deve usare = non >=
    ✓ "at least" → deve usare >= non =
    ✓ ORDER BY con alias non definito → errore comune
    ✓ "top/highest" senza LIMIT
    ✓ "how many" senza COUNT()
    ✓ GROUP BY che contiene funzioni aggregate (errore SQL)
    
    Ritorna una stringa di errore se trova problemi, None se ok """
    
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
# VALIDAZIONE & FORMATTAZIONE
# =========================

def validate_columns(columns_raw: str, valid_tables: set, valid_columns: dict) -> list[str]:
    """Scarta colonne allucinate non presenti in SQLite. Estrae pattern table.column esatti."""
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
        else:
            # ── Caso 2: Solo nome colonna ──
            col_only = re.sub(r'[^a-z0-9_]', '', col_clean)
            if col_only:
                found = False
                # Cerca prima nelle tabelle valide
                for valid_col in valid_columns:
                    t_part, c_part = valid_col.split('.')
                    if c_part == col_only and t_part in valid_tables:
                        valid_cols.append(valid_col)
                        found = True
                        break
                
                # Altrimenti cerca ovunque
                if not found:
                    for valid_col in valid_columns:
                        _, c_part = valid_col.split('.')
                        if c_part == col_only:
                            valid_cols.append(valid_col)
                            break
    return valid_cols



def validate_sql_syntax(sql: str, valid_tables: set | None = None) -> tuple[bool, str]:
    # Usa sqlglot per parsare la query generata:
    # ✓ Blocca statement multipli (es: SELECT...; DROP TABLE...)
    # ✓ Permette SOLO SELECT (blocca INSERT/UPDATE/DELETE/DROP)
    # ✓ Verifica che le tabelle usate siano nella whitelist
    # ✓ Verifica la sintassi SQLite
    #
    # Questo è il GUARD di sicurezza principale
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

def process_question(q: str, retriever: Retriever, adapter: SchemaAdapter, schema_text: str, valid_tables: set, valid_columns: dict, progress_callback=None, previous_sql: str | None = None, user_feedback: str | None = None, current_db_id: str | None = None, Benchmark: bool | None = None, use_baseline: bool = False, llm_model: str | None = None) -> dict:
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
    # print(augmented_question)

    reasoning = ""
    sim_context = ""
    valid_cols = []

    if use_baseline:
        notify_progress("step-4", "Generazione query MySQL (Baseline)...")
        print("⏳ Generazione Query Target (Baseline)...")
        p_sql = build_baseline_prompt(augmented_question, schema_text)
        try:
            sql = clean_sql(call_ollama(p_sql, model=llm_model))
        except ConnectionError as ce:
            return {"success": False, "error": str(ce)}
    else:
        notify_progress("step-1", "Ricerca pattern nel Vector DB...")
        print("\n⏳ Ricerca pattern logici analoghi nel Vector DB...")
        k = 5
        # ── FASE 1: RAG ──────────────────────────────────
        similars = retriever.retrieve(q, top_k=k, db_id=current_db_id)
        # Se similarità > 0.90 → usa direttamente quella query come guida
        # Se similarità > 0.50 → usala come contesto
        max_sim = similars[0]["similarity"] if similars else 0

        if similars:
            print(f"   [Trovato pattern analogo con {max_sim:.2f} di vicinanza]")
        sim_context = retriever.format_examples(similars)
        #Se aveva già generato una sql precedente vuol dire che ha commesso un errore per l'utente
        if previous_sql:
            sim_context = (
                f"{sim_context}\n\n"
                "/* Previous wrong SQL (do not repeat this error) */\n"
                f"{previous_sql}"
            )
        if max_sim > 0.90:
            sim_context = f"/* Query molto simile, usala come riferimento */\n{similars[0]['query']}"

        # ── FASE 2: Chain-of-Thought ──────────────────────
        notify_progress("step-2", "Ragionamento chain-of-thought...")
        print("🧠 Ragionamento in corso...")
        try:
            cot_prompt = build_cot_prompt(augmented_question, schema_text)
            reasoning = call_ollama(cot_prompt, model=llm_model)
            reasoning = reasoning.strip()
            if DEBUG:
                print(f"   💭 {reasoning[:200]}")
        except ConnectionError as ce:
            print(f"❌ {ce}")
            return {"success": False, "error": str(ce)}

        # ── FASE 3: Selezione colonne ─────────────────────
        notify_progress("step-3", "Identificazione colonne necessarie...")
        print("⏳ Identificazione colonne strettamente necessarie...")
        try:
            p_cols = build_columns_prompt(augmented_question, schema_text, sim_context, reasoning)
            cols_raw = call_ollama(p_cols, model=llm_model)
        except ConnectionError as ce:
            return {"success": False, "error": str(ce)}
        if(valid_tables and valid_columns):
            print("Cols Raw:", cols_raw)
            print("Tables:", valid_tables)
            print("Columns", valid_columns)
            valid_cols = validate_columns(cols_raw, valid_tables, valid_columns)
            if not valid_cols:
                return {"success": False, "error": "Nessuna colonna legittima identificata per la domanda."}

        # ── FASE 4: Generazione + loop di fix ────────────
        notify_progress("step-4", "Generazione query MySQL...")
        print("⏳ Generazione Query Target...")
        if(Benchmark is None):
            p_sql = build_sql_prompt("\n".join(valid_cols), augmented_question, schema_text, sim_context, reasoning)
        else:
            p_sql = build_sql_prompt("\n".join(cols_raw), augmented_question, schema_text, sim_context, reasoning)
        
        try:
            sql = clean_sql(call_ollama(p_sql, model=llm_model))
        except ConnectionError as ce:
            return {"success": False, "error": str(ce)}

    if use_baseline:
        max_attempts = 1
    else:
        # ── Pre-validation check ──
        pre_check_error = semantic_guard(sql, augmented_question)
        if pre_check_error:
            print(f"   🔴 Pre-check: {pre_check_error[:80]}...")
        max_attempts = 6

    for attempt in range(1, max_attempts + 1):
        # 1. Validazione sintattica (sqlglot)
        # 2. Semantic guard
        # 3. Esecuzione reale sul DB SQLite
        # → Se tutto ok: ritorna risultati
        # → Se errore: diagnosi → fix → ricomincia
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
        if not use_baseline:
            sem_err = semantic_guard(sql, augmented_question)
            if sem_err and attempt < max_attempts:
                print(f"   ⚠️ Semantic guard: {sem_err}")
                sql = clean_sql(call_ollama(build_fix_prompt(query=sql, error=sem_err, explanation=sem_err,schema_text=schema_text, question=augmented_question, reasoning=reasoning,), model=llm_model))
                continue

        # ── Execution ──

        if(Benchmark is None):
            res = execute_query(sql, adapter)
            if res.get("success"):  # Usa .get() per sicurezza
                print(f"\n🖥️  QUERY ESEGUITA:\n   {sql}")
                res["sql"] = sql
                return res
            else:
                err = res["error"]
                print(f"   ❌ Errore (Execution/Syntax): {err[:350]}")
                if attempt < max_attempts:
                    explain = call_ollama(build_explain_prompt(
                        sql, err, augmented_question, schema_text), model=llm_model)
                    if DEBUG:
                        print(f"   💡 Diagnosi: {explain[:100]}...")
                    p_fix = build_fix_prompt(sql, err, explain, schema_text, question=augmented_question, reasoning=reasoning)
                    sql = clean_sql(call_ollama(p_fix, model=llm_model))
                else:
                    return {"success": False, "error": err, "sql": sql}
        else:
            return {"sql": sql}

    print("\n❌ Impossibile generare una query SQLite valida (Tentativi esauriti).")
    return {"success": False, "error": "Tentativi esauriti.", "sql": sql}


# =========================
# INTERACTIVE LOOP
# =========================

def interactive_loop():
    
    """Loop interattivo per uso da terminale:
    1. Carica il Retriever (vector DB)
    2. Chiede il path del database SQLite
    3. Estrae schema e metadati
    4. Per ogni domanda dell'utente:
       - Processa la domanda
       - Mostra i risultati
       - Chiede se la query è corretta
       - Salva nel vector DB (con flag corretto/errato)"""
    
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
        current_db_id = os.path.splitext(os.path.basename(sqlite_path))[0].lower()
        res = process_question(q, retriever, adapter,schema_text, valid_tables, valid_columns,current_db_id=current_db_id)
        if res["success"]:
            print("\n📊 RISULTATI SQLite:")
            print(format_results(res))
            right=ask_yes_no("\n✅ La query generata è corretta? [s/n]: ")
            while right is not True:
                retriever.add_example(q, res["sql"], db_id=current_db_id, is_correct=False)
                user_feedback=input("Inserisci una spiegazione prima di rigenerare (oppure exit per inserire un altra domanda):")
                if user_feedback.lower() in ('esci', 'exit', 'quit', 'q'):
                    break;
                res=process_question(q, retriever, adapter,schema_text, valid_tables, valid_columns,current_db_id=current_db_id,previous_sql=res['sql'],user_feedback=user_feedback)
                right=ask_yes_no("\n✅ La query generata è corretta? [s/n]: ")
            if res["success"]:
                retriever.add_example(q, res["sql"], db_id=current_db_id, is_correct=True)
        else:
            print("\n❌ ERRORE:", res.get("error", "Sconosciuto"))
            failed_sql = res.get("sql")
            if failed_sql:
                retriever.add_example(q, failed_sql, db_id=current_db_id,is_correct=False, error=res.get("error", "Sconosciuto"))
                print("   📝 Query salvata automaticamente nel pool con stato: ERRATA.")


if __name__ == "__main__":
    interactive_loop()
