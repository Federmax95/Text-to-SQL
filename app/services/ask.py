"""
🔎 Text to SQL
=======================================
Ti permette di fare domande in linguaggio naturale su di un dataset a scelta in SQLite.
Estrae lo schema del dataset mandato in input e lo formatta per mandarlo all'llm.
Usa un vector pool per estrarre gli esempi più vicini alla domanda dell'utente (RAG).
Genera ESCLUSIVAMENTE query di lettura (SELECT).
"""


from app.services.schema_adapter import SchemaAdapter
from app.services.retriever import Retriever
from app.core.config import LLM_MODEL, OLLAMA_URL, TOP_K
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


DEBUG = False  # True per stampare dettagli di debug, False per produzione

# =========================
# OLLAMA & UTILS
# =========================

# Funzione per chiamare l'api di ollama


def call_ollama(prompt: str, retries=5, model=None) -> str:
    """Chiama l'API REST locale di Ollama con backoff esponenziale."""
    last_error = None
    target_model = model or LLM_MODEL
    for attempt in range(retries):
        try:
            response = requests.post(
                OLLAMA_URL,
                json={"model": target_model, "prompt": prompt,
                      "stream": False, "think": False, },
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

        wait = 2 ** attempt
        print(
            f"⏳ Attendo {wait}s prima del tentativo {attempt + 2}/{retries}...")
        time.sleep(wait)

    raise ConnectionError(
        f"Ollama non raggiungibile dopo {retries} tentativi. Ultimo errore: {last_error}")


def clean_sql(response: str) -> str:
    """Estrae codice SQL puro dai blocchi markdown."""
    # Rimuovo gli spazi ad inizio e fine stringa
    response = response.strip()
    # cerco all'interno del prompt la query sql generata
    match = re.search(r"```(?:sql)?\s*\n?(.*?)```",
                      response, re.DOTALL | re.IGNORECASE)
    if match:
        # se l'ho trovata la restituisco
        return match.group(1).strip()
    return response.strip()


# =========================
# PROMPT BUILDERS
# =========================
def build_baseline_prompt(question: str, schema_text: str) -> str:
    return f"""

            {schema_text}

            CRITICAL INSTRUCTIONS:
            - Return ONLY and EXCLUSIVELY the raw SQL query.
            - DO NOT add explanations, comments, conversational text, or greetings.
            - DO NOT wrap the code in markdown formatting blocks (e.g., do NOT use ```sql or ```). Your output must be plain, directly executable text.

            ### User Question:
            {question}

            ### SQLite Query:
            """


def build_columns_prompt(question: str, schema_text: str) -> str:
    return f"""You are a SQLite schema analyzer.
            {schema_text}

            ### Rules:
            - Output ONLY the columns from the schema above needed to answer the question.
            - Format: table.column (one per line)
            - Include all columns needed for SELECT, JOIN, WHERE, ORDER BY.
            - Do NOT output extra text.

            ### Question:
            {question}

            ### Output format:
            - table1.column1
            - table1.column2
            - table2.column3
            """


def build_sql_prompt(columns: str, question: str, similar: str) -> str:
    return f"""You are an expert SQLite assistant. Generate ONLY a valid SELECT query.

        ### Allowed columns (use only these, never invent):
        {columns}

        ### Similar example (use only as structure reference, do NOT copy values):
        {similar}

        ### User question:
        {question}

        ### STRICT RULES (violating any will cause failure):
        1. **No extra filters** – add a WHERE clause ONLY if the question explicitly mentions a condition.
        2. **Aggregation**:
        - "how many rows" → COUNT(*)
        - "how many X" (X is a column) → COUNT(X)
        - "total / sum / overall" → SUM(column)
        - "average / mean" → AVG(column)
        - "how many downloads / revenue / amount" → SUM(column), NOT COUNT
        3. **Group By** – if the question asks for "per X", "each Y", "by Z", you MUST use GROUP BY X/Y/Z.
        4. **No alias in ORDER BY** – repeat the full expression, e.g. ORDER BY COUNT(*) DESC.
        5. **No SELECT *** – list only columns needed to answer the question.
        6. **No JOIN unless required** – use INNER JOIN only if columns from another table are explicitly needed.

        ### Examples of correct queries:
        - Q: "What is the total revenue from sales?" → SELECT SUM(revenue) FROM sales;
        - Q: "How many accidents for SpaceX and Blue Origin?" → SELECT launch_provider, COUNT(*) FROM Accidents GROUP BY launch_provider;
        - Q: "Maximum quantity of seafood sold in a single transaction" → SELECT MAX(quantity) FROM sales;

        CRITICAL: Return ONLY the SQL query, no explanations, no markdown.
        SQLite query:"""


def is_result_semantically_correct(question: str, sql: str, result_data: dict, model: str = None) -> tuple[bool, str]:
    """
    Usa un LLM per stabilire se i risultati della query rispondono alla domanda.
    Restituisce (corretto, spiegazione).
    """
    # Estrai un campione significativo dei risultati (max 10 righe)
    data_sample = result_data.get("data", [])[:10]
    columns = result_data.get("columns", [])

    if not data_sample:
        # Nessun risultato: spesso è un errore se la domanda prevede almeno una riga
        return False, "La query non ha restituito alcuna riga, ma la domanda richiedeva un risultato."

    # Crea una rappresentazione testuale dei risultati
    import json
    results_str = json.dumps([dict(zip(columns, row))
                             for row in data_sample], indent=2, ensure_ascii=False)

    prompt = f"""You are a strict judge. Given a user question and the SQL query executed, along with a sample of the results, decide if the results CORRECTLY answer the question.

                ### User question:
                {question}

                ### Executed SQL:
                {sql}

                ### Sample of results (first {len(data_sample)} rows):
                {results_str}

                ### Rules:
                - Answer ONLY "YES" if the results fully answer the question (even if the SQL style differs from an ideal query).
                - Answer "NO" and give a ONE-SENTENCE explanation if:
                - The results are empty when they shouldn't be.
                - The results are missing required columns (e.g., question asks "per region" but results have no region column).
                - The aggregation is wrong (e.g., question asks "total downloads" but results show a count of rows).
                - The results contain extra filters not requested.
                - Do NOT criticize formatting or column names unless they change the meaning.

                ### Output format:
                - First line: YES or NO
                - Second line: explanation (only if NO)

                Your answer:"""

    response = call_ollama(prompt, model=model, retries=2)
    lines = response.strip().splitlines()
    if lines and lines[0].strip().upper() == "YES":
        return True, ""
    else:
        explanation = lines[1] if len(
            lines) > 1 else "Results do not answer the question"
        return False, explanation


def build_semantic_fix_prompt(question: str, sql: str, explanation: str, schema_text: str, columns: str) -> str:
    return f"""The SQL query executed successfully but returned results that do NOT correctly answer the user's question.

            ### Database schema:
            {schema_text}

            ### Allowed columns:
            {columns}

            ### User question:
            {question}

            ### Original SQL:
            {sql}

            ### Why it's wrong:
            {explanation}

            ### Fix instructions:
            - Do NOT change the meaning of the question.
            - Correct ONLY the logical error described above.
            - Keep the same tables and columns unless the error requires different ones.
            - Return ONLY the corrected SQL query, no extra text.

            ### Corrected SQL:
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


def build_fix_prompt(query: str, error: str, explanation: str, schema_text: str, question: str) -> str:
    return f"""You are a SQLite expert fixing a query.

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

# =========================
# VALIDAZIONE & FORMATTAZIONE
# =========================

# Controlla che le colonne che ha selezionato LLM esistano effettivamente


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
    return valid_cols

# Controlla che la sql sia valida e solo di SELECT


def validate_sql_syntax(sql: str) -> tuple[bool, str]:
    try:
        ast = sqlglot.parse_one(sql, read="sqlite")
        if not isinstance(ast, exp.Select):
            return False, f"Bloccato: Rilevata operazione non consentita ({ast.key.upper()})."
        return True, "Query valida e sicura."
    except sqlglot.errors.ParseError as e:
        return False, f"Errore di sintassi SQL:\n{e}"
    except Exception as e:
        return False, f"Errore imprevisto:\n{e}"


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


def ask_context_path() -> str | None:
    """Chiede il path di un file txt per aggiungere contesto extra."""
    raw = input(f"📄 Path file di contesto (.txt) [Opzionale, premi Invio per saltare]: ").strip()
    if not raw:
        return None
    chosen = raw.strip().strip('"').strip("'")
    chosen = os.path.expandvars(os.path.expanduser(chosen))
    if not os.path.isabs(chosen):
        chosen = os.path.join(PROJECT_DIR, chosen)
    return os.path.abspath(chosen)


# =========================
# CORE APP
# =========================

def process_question(q: str, retriever: Retriever, adapter: SchemaAdapter, schema_text: str,
                     valid_tables: set, valid_columns: dict, progress_callback=None,
                     previous_sql: str | None = None, user_feedback: str | None = None,
                     current_db_id: str | None = None, Benchmark: bool | None = None,
                     use_baseline: bool = False, use_rag: bool = True, use_cols: bool = True,
                     max_attempts_override: int | None = None, llm_model: str | None = None,
                     use_improved_prompt: bool = True, additional_context: str | None = None) -> dict:
    """
    Core logic per ottenere la answer sia dalla CLI che dalle API.
    Integra validazione semantica post-esecuzione e auto-correzione.
    """
    def notify_progress(step: str, message: str = ""):
        if progress_callback:
            progress_callback(step, message)

    augmented_question = q
    judge_calls = 0
    max_attempts = max_attempts_override if max_attempts_override is not None else (1 if use_baseline else 6)
    
    if use_baseline:
        use_rag = False
        use_cols = False
        use_improved_prompt = False

    if user_feedback:
        augmented_question = (
            f"{q}\n\n"
            f"Additional user feedback about previous wrong SQL:\n{user_feedback.strip()}"
        )

    if additional_context:
        augmented_question = (
            f"{augmented_question}\n\n"
            f"### Additional Context:\n{additional_context.strip()}"
        )

    # ========== FASE 1: Generazione SQL ==========
    notify_progress("step-1", "Ricerca pattern nel Vector DB...")
    sim_context = ""
    if use_rag:
        print("\n🔍 Ricerca pattern logici analoghi nel Vector DB...")
        k = 5
        similars = retriever.retrieve(q, top_k=k, db_id=current_db_id)
        max_sim = similars[0]["similarity"] if similars else 0
        if similars:
            print(
                f"   [Trovato pattern analogo con {max_sim:.2f} di vicinanza]")
            sim_context = retriever.format_examples(similars)
    
    if previous_sql:
        sim_context = (
            f"{sim_context}\n\n"
            "/* Previous wrong SQL (do not repeat this error) */\n"
            f"{previous_sql}"
        )

    # Selezione colonne
    valid_cols = list(valid_columns.keys())
    if use_cols:
        notify_progress("step-2", "Identificazione colonne necessarie...")
        print("🚀 Identificazione colonne strettamente necessarie...")
        try:
            p_cols = build_columns_prompt(augmented_question, schema_text)
            cols_raw = call_ollama(p_cols, model=llm_model)
            valid_cols = validate_columns(cols_raw, valid_tables, valid_columns)
            if not valid_cols:
                if DEBUG:
                    print(
                        "   ⚠️ Nessuna colonna estratta, passo l'intero schema come fallback.")
                valid_cols = list(valid_columns.keys())
        except ConnectionError as ce:
            return {"success": False, "error": str(ce), "judge_calls": judge_calls}

    # Generazione SQL
    notify_progress("step-3", "Generazione query MySQL...")
    print("⏳ Generazione Query Target...")
    
    if not use_improved_prompt:
        q_for_baseline = augmented_question
        if use_cols and valid_cols:
            q_for_baseline += "\n\nConsider ONLY these tables/columns to answer:\n" + "\n".join(valid_cols)
        if use_rag and sim_context:
            q_for_baseline += "\n\nReference structure from these similar examples (do not copy values):\n" + sim_context
        p_sql = build_baseline_prompt(q_for_baseline, schema_text)
    else:
        p_sql = build_sql_prompt(
            "\n".join(valid_cols), augmented_question, sim_context)
            
    try:
        sql = clean_sql(call_ollama(p_sql, model=llm_model))
    except ConnectionError as ce:
        return {"success": False, "error": str(ce), "judge_calls": judge_calls}

    # ========== FASE 2: Loop di auto-correzione (sintassi + semantica) ==========
    for attempt in range(1, max_attempts + 1):
        if attempt > 1:
            print(f"   🔄 AutoFix #{attempt - 1}...")
            notify_progress("step-3", f"Tentativo di fix #{attempt - 1}...")

        # ---- 2.1 Controllo sintassi (solo SELECT) ----
        is_valid, validation_err = validate_sql_syntax(sql)
        if not is_valid:
            print(f"❌ Bloccato: {validation_err}")
            return {"success": False, "error": validation_err, "sql": sql, "judge_calls": judge_calls}

        res = execute_query(sql, adapter)
        if not res.get("success"):
            # Errore di esecuzione: fix sintattico/logico (come già facevi)
            err = res["error"]
            print(f"   ❌ Errore (Execution/Syntax): {err[:350]}")
            if attempt < max_attempts:
                explain = call_ollama(build_explain_prompt(
                    sql, err, augmented_question, schema_text), model=llm_model)
                if DEBUG:
                    print(f"   💡 Diagnosi: {explain[:100]}...")
                p_fix = build_fix_prompt(
                    sql, err, explain, schema_text, question=augmented_question)
                sql = clean_sql(call_ollama(p_fix, model=llm_model))
            else:
                return {"success": False, "error": err, "sql": sql, "judge_calls": judge_calls}
            continue   # riprova con la nuova SQL

        # ---- 2.3 Esecuzione riuscita -> validazione semantica ----
        if max_attempts == 1:
            # Bypass judge per le ablazioni base
            if DEBUG:
                print(f"\n✅ QUERY GENERATA (Bypass Judge):\n   {sql}")
            res["sql"] = sql
            res["judge_calls"] = judge_calls
            return res

        judge_calls += 1
        is_correct, explanation = is_result_semantically_correct(augmented_question, sql, res, model="llama3.2:3b")
        if is_correct:
            print(f"\n🖥️  QUERY ESEGUITA:\n   {sql}")
            res["sql"] = sql
            res["judge_calls"] = judge_calls
            return res
        else:
            print(
                f"   ❌ Risultato semanticamente scorretto: {explanation[:200]}")
            if attempt < max_attempts:
                # Prepara le colonne consentite (se in modalità baseline, usa lista vuota)
                cols_for_fix = "\n".join(
                    valid_cols) if 'valid_cols' in locals() else ""
                p_fix = build_semantic_fix_prompt(
                    augmented_question, sql, explanation, schema_text, cols_for_fix)
                sql = clean_sql(call_ollama(p_fix, model=llm_model))
            else:
                return {"success": False, "error": f"Errore semantico: {explanation}", "sql": sql, "judge_calls": judge_calls}

    print("\n❌ Impossibile generare una query SQLite valida (Tentativi esauriti).")
    return {"success": False, "error": "Tentativi esauriti.", "sql": sql, "judge_calls": judge_calls}


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
    context_text = None
    try:
        retriever = Retriever()
        sqlite_path = ask_db_path()
        if not os.path.exists(sqlite_path):
            print(f"❌ File SQLite non trovato: {sqlite_path}")
            return
            
        context_path = ask_context_path()
        if context_path:
            if os.path.exists(context_path):
                with open(context_path, 'r', encoding='utf-8') as f:
                    context_text = f.read()
                print(f"✅ Contesto caricato: {context_path}")
            else:
                print(f"⚠️ File di contesto non trovato: {context_path}. Verrà ignorato.")
                
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
                               current_db_id=current_db_id,
                               additional_context=context_text)
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
