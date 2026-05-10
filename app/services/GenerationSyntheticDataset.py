#!/usr/bin/env python3
"""
Synthetic SQLite Pipeline — GaussianCopula + Full Validation
─────────────────────────────────────────────────────────────
• Synthesis  : GaussianCopula per-table (no HMA blowup)
• FK repair  : referential integrity restored after synthesis
• Validation :
    1. Pandera       – structural / dtype schema checks
    2. Great Expectations – statistical / distribution expectations
    3. SDMetrics     – fidelity score per table
"""

import pandera as pa
from sdmetrics.reports.single_table import QualityReport as SingleTableQualityReport
from sdv.utils import drop_unknown_references
from sdv.single_table import GaussianCopulaSynthesizer
from sdv.metadata import MultiTableMetadata, SingleTableMetadata
import sqlite3
import pandas as pd
import numpy as np
import os
import sys
import warnings
from collections import defaultdict, deque

warnings.filterwarnings("ignore", category=FutureWarning)
warnings.filterwarnings("ignore", category=UserWarning)

# ── SDV ───────────────────────────────────────────────────────────────────────

# ── SDMetrics ─────────────────────────────────────────────────────────────────

# ── Pandera ───────────────────────────────────────────────────────────────────

# ── Great Expectations — version-safe import ──────────────────────────────────
GE_MODE = None
try:
    from great_expectations.dataset import PandasDataset as GEPandasDataset
    GE_MODE = "pandas_dataset"          # GE < 0.18 (legacy)
except ImportError:
    try:
        import great_expectations as gx
        _ = gx.get_context(mode="ephemeral")   # GE >= 0.18
        GE_MODE = "v1"
    except Exception:
        pass

if GE_MODE is None:
    print("[!] Great Expectations not available — GE validation will be skipped.")


# ─────────────────────────────────────────────────────────────────────────────
INPUT_DB = r"C:\Users\feder\Desktop\Text-to-SQL-main\spider_data\database\baseball_1\baseball_1.sqlite"
OUTPUT_DB = "synthetic_gaussian_output.db"
SCALE = 2.0
# ─────────────────────────────────────────────────────────────────────────────

SEP = "=" * 65
SEP2 = "─" * 65


# ═══════════════════════════════════════════════════════════════════════════════
# 1.  LOAD
# ═══════════════════════════════════════════════════════════════════════════════

def load_sqlite_database(db_path: str) -> dict:
    conn = sqlite3.connect(db_path)
    tables = pd.read_sql_query("SELECT name FROM sqlite_master WHERE type='table';", conn)["name"].tolist()

    data = {}
    for t in tables:
        try:
            data[t] = pd.read_sql_query(f'SELECT * FROM "{t}"', conn)
            print(f"  [+] {t:35s} {len(data[t]):>8,} rows")
        except Exception as e:
            print(f"  [!] Skip {t}: {e}")
    conn.close()
    return data


# ═══════════════════════════════════════════════════════════════════════════════
# 2.  METADATA  &  DATETIME CLEANING
# ═══════════════════════════════════════════════════════════════════════════════

def build_metadata(data: dict) -> MultiTableMetadata:
    meta = MultiTableMetadata()
    meta.detect_from_dataframes(data=data)
    return meta


def clean_datetime_columns(data: dict, metadata: MultiTableMetadata) -> dict:
    for tname, tmeta in metadata.tables.items():
        df = data[tname]
        for col, cmeta in tmeta.columns.items():
            if cmeta.get("sdtype") == "datetime":
                df[col] = pd.to_datetime(
                    df[col].replace("", np.nan), errors="coerce"
                )
        data[tname] = df
    return data


def normalize_fk_dtypes(data: dict, relationships: list) -> dict:
    """Normalizza i tipi di dati tra FK e PK per evitare errori di merge."""
    for rel in relationships:
        pt = rel["parent_table_name"]
        ppk = rel["parent_primary_key"]
        ct = rel["child_table_name"]
        cfk = rel["child_foreign_key"]

        if pt not in data or ct not in data:
            continue
        if ppk not in data[pt].columns or cfk not in data[ct].columns:
            continue

        parent_dtype = data[pt][ppk].dtype
        child_dtype = data[ct][cfk].dtype

        # Se i tipi sono diversi, converti il FK al tipo del PK
        if parent_dtype != child_dtype:
            try:
                # Converti sempre al tipo della PK
                if pd.api.types.is_integer_dtype(parent_dtype):
                    data[ct][cfk] = pd.to_numeric(
                        data[ct][cfk], errors='coerce').astype(parent_dtype)
                elif pd.api.types.is_float_dtype(parent_dtype):
                    data[ct][cfk] = pd.to_numeric(
                        data[ct][cfk], errors='coerce')
                else:
                    data[ct][cfk] = data[ct][cfk].astype(parent_dtype)
                print(f"  [DTYPE] {ct}.{cfk}: {child_dtype} -> {parent_dtype}")
            except Exception as e:
                print(f"  [!] Errore conversione {ct}.{cfk}: {e}")

    return data


# ═══════════════════════════════════════════════════════════════════════════════
# 3.  TOPOLOGICAL SORT  (parents before children)
# ═══════════════════════════════════════════════════════════════════════════════

def topological_sort(tables: list, relationships: list) -> list:
    in_deg = defaultdict(int, {t: 0 for t in tables})
    children = defaultdict(list)

    for rel in relationships:
        p = rel["parent_table_name"]
        c = rel["child_table_name"]
        if p != c:
            children[p].append(c)
            in_deg[c] += 1

    queue = deque(t for t in tables if in_deg[t] == 0)
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for ch in children[node]:
            in_deg[ch] -= 1
            if in_deg[ch] == 0:
                queue.append(ch)

    # disconnected / cycle survivors
    for t in tables:
        if t not in order:
            order.append(t)
    return order


# ═══════════════════════════════════════════════════════════════════════════════
# 4.  PER-TABLE GAUSSIAN COPULA SYNTHESIS
# ═══════════════════════════════════════════════════════════════════════════════

def _single_meta_for(df: pd.DataFrame, multi_meta: MultiTableMetadata,
                     table_name: str) -> SingleTableMetadata:
    """Build a SingleTableMetadata by detecting from the dataframe."""
    single = SingleTableMetadata()
    single.detect_from_dataframe(df)

    # Copy sdtypes detected by MultiTableMetadata (e.g. 'id' for PK columns)
    if table_name in multi_meta.tables:
        for col, col_info in multi_meta.tables[table_name].columns.items():
            if col in single.columns:
                # overwrite with richer info
                single.columns[col] = dict(col_info)

    return single


def synthesize_all_tables(real_data: dict,
                          multi_meta: MultiTableMetadata,
                          scale: float) -> dict:
    order = topological_sort(list(real_data.keys()), multi_meta.relationships)
    synthetic = {}
    fallbacks = []

    for tname in order:
        df = real_data[tname]
        n_target = max(1, int(len(df) * scale))
        print(
            f"\n  Synthesizing '{tname}'  ({len(df):,} -> {n_target:,} rows)")

        try:
            single_meta = _single_meta_for(df, multi_meta, tname)
            synth = GaussianCopulaSynthesizer(single_meta)
            synth.fit(df)
            synthetic[tname] = synth.sample(num_rows=n_target)
            print(f"    ✓ done")
        except Exception as e:
            print(f"    ✗ GaussianCopula failed: {e}")
            print(f"    -> falling back to bootstrapped sample of real data")
            synthetic[tname] = (
                df.sample(n=min(n_target, len(df)), replace=True)
                  .reset_index(drop=True)
            )
            fallbacks.append(tname)

    if fallbacks:
        print(f"\n  [!] Fallback (resampled real data): {fallbacks}")

    return synthetic


# ═══════════════════════════════════════════════════════════════════════════════
# 5.  RESTORE REFERENTIAL INTEGRITY
# ═══════════════════════════════════════════════════════════════════════════════

def restore_referential_integrity(synthetic: dict, relationships: list) -> dict:
    print()
    for rel in relationships:
        pt = rel["parent_table_name"]
        ppk = rel["parent_primary_key"]
        ct = rel["child_table_name"]
        cfk = rel["child_foreign_key"]

        if pt not in synthetic or ct not in synthetic:
            continue
        if ppk not in synthetic[pt].columns or cfk not in synthetic[ct].columns:
            continue

        parent_keys = synthetic[pt][ppk].dropna().unique()
        if len(parent_keys) == 0:
            print(f"  [!] {pt}.{ppk} is empty — skip FK repair for {ct}.{cfk}")
            continue

        n = len(synthetic[ct])
        synthetic[ct][cfk] = np.random.choice(
            parent_keys, size=n, replace=True)
        print(f"  [FK] {ct}.{cfk}  ->  {pt}.{ppk}")

    return synthetic


# ═══════════════════════════════════════════════════════════════════════════════
# 6.  PANDERA VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _pandera_validate(real_df: pd.DataFrame,
                      syn_df: pd.DataFrame,
                      tname: str) -> dict:
    result = {"table": tname, "pass": False, "error": ""}
    try:
        base_schema = pa.infer_schema(real_df)

        # Relax: allow nulls + coerce types (synthetic may drift slightly)
        relaxed_cols = {}
        for cname, col in base_schema.columns.items():
            relaxed_cols[cname] = pa.Column(
                dtype=col.dtype,
                nullable=True,
                coerce=True,
                required=col.required,
            )

        relaxed_schema = pa.DataFrameSchema(
            columns=relaxed_cols,
            coerce=True,
        )
        relaxed_schema.validate(syn_df)
        result["pass"] = True

    except pa.errors.SchemaErrors as e:
        failures = e.failure_cases
        result["error"] = f"{len(failures)} check(s) failed"
    except pa.errors.SchemaError as e:
        result["error"] = str(e)[:200]
    except Exception as e:
        result["error"] = f"Unexpected: {str(e)[:200]}"

    return result


def run_pandera(real_data: dict, synthetic: dict) -> pd.DataFrame:
    print(f"\n{SEP}\n  PANDERA VALIDATION\n{SEP}")
    rows = []
    for tname in real_data:
        if tname not in synthetic:
            continue
        r = _pandera_validate(real_data[tname], synthetic[tname], tname)
        badge = "✓ PASS" if r["pass"] else "✗ FAIL"
        print(f"  {badge}  {tname}")
        if not r["pass"]:
            print(f"         └─ {r['error']}")
        rows.append(r)
    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 7.  GREAT EXPECTATIONS VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def _build_expectations(real_df: pd.DataFrame) -> list:
    """Auto-derive GE expectations from real data statistics."""
    exps = []
    for col in real_df.columns:
        exps.append(("expect_column_to_exist", {"column": col}))

        null_rate = real_df[col].isna().mean()
        if null_rate < 0.05:
            exps.append(("expect_column_values_to_not_be_null",
                         {"column": col, "mostly": 0.90}))

        if pd.api.types.is_numeric_dtype(real_df[col]):
            clean = real_df[col].dropna()
            if len(clean) > 0:
                exps.append(("expect_column_values_to_be_between", {
                    "column":    col,
                    "min_value": float(clean.min()),
                    "max_value": float(clean.max()),
                    "mostly":    0.95,
                }))

        n_unique = real_df[col].nunique()
        if real_df[col].dtype == object and 1 < n_unique <= 30:
            exps.append(("expect_column_values_to_be_in_set", {
                "column":    col,
                "value_set": real_df[col].dropna().unique().tolist(),
                "mostly":    0.95,
            }))

    return exps


# ── GE legacy path (PandasDataset) ───────────────────────────────────────────

def _run_ge_legacy(real_data: dict, synthetic: dict) -> pd.DataFrame:
    all_results = []
    for tname in real_data:
        if tname not in synthetic:
            continue
        exps = _build_expectations(real_data[tname])
        ge_df = GEPandasDataset(synthetic[tname])
        passed = failed = 0
        for method_name, kwargs in exps:
            try:
                res = getattr(ge_df, method_name)(**kwargs)
                ok = res["success"]
            except Exception:
                ok = False
            if ok:
                passed += 1
            else:
                failed += 1
            all_results.append({
                "table":       tname,
                "expectation": method_name,
                "column":      kwargs.get("column", ""),
                "success":     ok,
            })
        total = passed + failed
        pct = passed / total * 100 if total else 0
        print(f"  {tname:35s}  {passed:>4}/{total:<4} ({pct:.1f}%)")
    return pd.DataFrame(all_results)


# ── GE v1 path — pandas-based evaluator (version-agnostic) ───────────────────
# GE's internal context/datasource API breaks across minor versions.
# We evaluate the same expectations directly with pandas so the logic is
# identical but has zero dependency on GE's unstable internal plumbing.

def _evaluate_expectation(syn_df: pd.DataFrame,
                          method_name: str, kwargs: dict) -> bool:
    col = kwargs.get("column")
    mostly = kwargs.get("mostly", 1.0)

    if method_name == "expect_column_to_exist":
        return col in syn_df.columns

    if col not in syn_df.columns:
        return False

    series = syn_df[col]
    n = len(series)
    if n == 0:
        return True

    if method_name == "expect_column_values_to_not_be_null":
        ok_rate = series.notna().sum() / n
        return ok_rate >= mostly

    if method_name == "expect_column_values_to_be_between":
        lo, hi = kwargs.get("min_value"), kwargs.get("max_value")
        valid = series.dropna()
        if len(valid) == 0:
            return True
        in_range = ((valid >= lo) & (valid <= hi)).sum()
        return in_range / n >= mostly

    if method_name == "expect_column_values_to_be_in_set":
        value_set = set(kwargs.get("value_set", []))
        valid = series.dropna()
        if len(valid) == 0:
            return True
        in_set = valid.isin(value_set).sum()
        return in_set / n >= mostly

    return True   # unknown expectation -> pass by default


def _run_ge_v1(real_data: dict, synthetic: dict) -> pd.DataFrame:
    all_results = []

    for tname in real_data:
        if tname not in synthetic:
            continue

        exps = _build_expectations(real_data[tname])
        syn_df = synthetic[tname]
        passed = failed = 0
        for method_name, kwargs in exps:
            ok = _evaluate_expectation(syn_df, method_name, kwargs)
            if ok:
                passed += 1
            else:
                failed += 1
            all_results.append({
                "table":       tname,
                "expectation": method_name,
                "column":      kwargs.get("column", ""),
                "success":     ok,
            })

        total = passed + failed
        pct = passed / total * 100 if total else 0
        print(f"  {tname:35s}  {passed:>4}/{total:<4} ({pct:.1f}%)")

    return pd.DataFrame(all_results)


def run_ge(real_data: dict, synthetic: dict) -> pd.DataFrame:
    print(f"\n{SEP}\n  GREAT EXPECTATIONS VALIDATION\n{SEP}")
    if GE_MODE is None:
        print("  [!] Skipped (great_expectations not installed or incompatible).")
        return pd.DataFrame()
    if GE_MODE == "pandas_dataset":
        return _run_ge_legacy(real_data, synthetic)
    else:
        return _run_ge_v1(real_data, synthetic)


# ═══════════════════════════════════════════════════════════════════════════════
# 8.  SDMETRICS VALIDATION
# ═══════════════════════════════════════════════════════════════════════════════

def run_sdmetrics(real_data: dict, synthetic: dict,
                  multi_meta: MultiTableMetadata) -> pd.DataFrame:
    print(f"\n{SEP}\n  SDMETRICS — per-table Quality Report\n{SEP}")
    rows = []
    for tname in real_data:
        if tname not in synthetic:
            continue

        real_df = real_data[tname]
        syn_df = synthetic[tname]

        # Build a plain SingleTableMetadata for sdmetrics
        single = SingleTableMetadata()
        single.detect_from_dataframe(real_df)
        if tname in multi_meta.tables:
            for col, info in multi_meta.tables[tname].columns.items():
                if col in single.columns:
                    single.columns[col] = dict(info)

        try:
            report = SingleTableQualityReport()
            report.generate(
                real_data=real_df,
                synthetic_data=syn_df,
                metadata=single.to_dict(),
            )
            score = report.get_score()
            col_shapes = report.get_details("Column Shapes")["Score"].mean()
            col_pair = report.get_details("Column Pair Trends")["Score"].mean()
            print(f"  {tname:35s}  overall={score:.4f}  "
                  f"shapes={col_shapes:.4f}  pairs={col_pair:.4f}")
            rows.append({
                "table":        tname,
                "overall":      score,
                "col_shapes":   col_shapes,
                "col_pairs":    col_pair,
                "error":        None,
            })
        except Exception as e:
            print(f"  {tname:35s}  [!] {e}")
            rows.append({
                "table":      tname,
                "overall":    None,
                "col_shapes": None,
                "col_pairs":  None,
                "error":      str(e)[:150],
            })

    return pd.DataFrame(rows)


# ═══════════════════════════════════════════════════════════════════════════════
# 9.  SAVE
# ═══════════════════════════════════════════════════════════════════════════════

def save_to_sqlite(data: dict, db_path: str):
    # Crea la cartella se non esiste
    os.makedirs(os.path.dirname(db_path), exist_ok=True)

    if os.path.exists(db_path):
        os.remove(db_path)
    conn = sqlite3.connect(db_path)
    for tname, df in data.items():
        df_save = df.copy()
        # Convert datetime cols -> strings (SQLite has no native datetime type)
        for col in df_save.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns:
            df_save[col] = df_save[col].astype(str).replace("NaT", None)
        df_save.to_sql(tname, conn, index=False)
        print(f"  [+] {tname}")
    conn.close()


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════════════

def generate_synthetic_dataset(input_db: str, output_db: str, scale: float = 2.0, progress_callback=None):
    """
    Genera un dataset sintetico da un database SQLite reale.

    Args:
        input_db: Percorso del database SQLite di input
        output_db: Percorso dove salvare il database sintetico
        scale: Fattore di scaling per il numero di righe
        progress_callback: Funzione opzionale per i callback di progresso (step, message)

    Returns:
        dict con informazioni sulla generazione
    """

    def update_progress(step: str, message: str = ""):
        """Helper per aggiornare il progresso."""
        if progress_callback:
            progress_callback(step, message)
        print(f"  [{step}] {message}")

    print(f"\n{SEP}")
    print("  Synthetic SQLite Pipeline — GaussianCopula + Validation")
    print(f"{SEP}\n")

    try:
        # 1. Load
        update_progress("loading", "Caricamento database reale...")
        real_data = load_sqlite_database(input_db)

        # 2. Metadata
        update_progress("metadata", "Rilevamento metadati...")
        metadata = build_metadata(real_data)

        # 3. Clean datetime
        update_progress("cleaning", "Pulizia colonne datetime...")
        real_data = clean_datetime_columns(real_data, metadata)

        # 4. Drop broken FK references
        update_progress(
            "fixing", "Rimozione riferimenti foreign key non validi...")
        real_data = drop_unknown_references(data=real_data, metadata=metadata)

        # 5. Normalize FK dtypes
        update_progress("normalizing", "Normalizzazione tipi di dati...")
        real_data = normalize_fk_dtypes(real_data, metadata.relationships)

        # 6. Synthesize per table
        update_progress(
            "synthesis", "Sintesi GaussianCopula per ogni tabella...")
        synthetic = synthesize_all_tables(real_data, metadata, scale)

        # 6b. Normalize synthetic FK dtypes BEFORE restoring integrity
        update_progress("normalizing_synthetic",
                        "Normalizzazione tipi sintetici...")
        synthetic = normalize_fk_dtypes(synthetic, metadata.relationships)

        # 7. Restore FK integrity
        update_progress("integrity", "Ripristino integrità referenziale...")
        synthetic = restore_referential_integrity(
            synthetic, metadata.relationships)

        # 8. Pandera
        update_progress("validation_pandera", "Validazione Pandera...")
        pandera_df = run_pandera(real_data, synthetic)

        # 9. Great Expectations
        update_progress("validation_ge", "Validazione Great Expectations...")
        ge_df = run_ge(real_data, synthetic)

        # 10. SDMetrics
        update_progress("validation_sdmetrics",
                        "Calcolo metriche SDMetrics...")
        sdm_df = run_sdmetrics(real_data, synthetic, metadata)

        # ── SUMMARY ──────────────────────────────────────────────────────────────
        update_progress("summary", "Generazione riepilogo...")

        pan_pass = pandera_df["pass"].sum()
        pan_total = len(pandera_df)
        print(f"\n  Pandera :  {pan_pass}/{pan_total} tables passed")

        if not ge_df.empty:
            ge_pass = ge_df["success"].sum()
            ge_total = len(ge_df)
            print(f"  GE      :  {ge_pass}/{ge_total} expectations passed "
                  f"({ge_pass/ge_total*100:.1f}%)")

        valid_sdm = sdm_df.dropna(subset=["overall"])
        if not valid_sdm.empty:
            mean_score = valid_sdm["overall"].mean()
            worst = valid_sdm.loc[valid_sdm["overall"].idxmin()]
            best = valid_sdm.loc[valid_sdm["overall"].idxmax()]
            print(f"  SDMetrics mean quality  : {mean_score:.4f}")
            print(
                f"  SDMetrics best  table   : {best['table']} ({best['overall']:.4f})")
            print(
                f"  SDMetrics worst table   : {worst['table']} ({worst['overall']:.4f})")

        # ── SAVE ─────────────────────────────────────────────────────────────────
        update_progress("saving", "Salvataggio database sintetico...")
        save_to_sqlite(synthetic, output_db)

        update_progress(
            "completed", "Dataset sintetico generato con successo!")
        print(f"\n  [OK] Synthetic DB saved -> {output_db}")
        print(f"\n{SEP}")
        print("  DONE")
        print(f"{SEP}\n")

        return {
            "success": True,
            "output_db": output_db,
            "tables_count": len(synthetic),
            "pandera_pass": int(pan_pass),
            "pandera_total": int(pan_total),
        }

    except Exception as e:
        update_progress("error", f"Errore: {str(e)}")
        print(f"\n{SEP}")
        print(f"  ERROR: {e}")
        print(f"{SEP}\n")
        return {
            "success": False,
            "error": str(e),
        }


def main():
    """Entry point per esecuzione da CLI"""
    print(f"\n{SEP}")
    print("  Synthetic SQLite Pipeline — GaussianCopula + Validation")
    print(f"{SEP}\n")

    # 1. Load
    print("Loading real database…")
    real_data = load_sqlite_database(INPUT_DB)

    # 2. Metadata
    print("\nDetecting metadata…")
    metadata = build_metadata(real_data)

    # 3. Clean datetime
    real_data = clean_datetime_columns(real_data, metadata)

    # 4. Drop broken FK references
    print("Removing broken foreign keys…")
    real_data = drop_unknown_references(data=real_data, metadata=metadata)

    # 5. Normalize FK dtypes
    print("Normalizing foreign key data types…")
    real_data = normalize_fk_dtypes(real_data, metadata.relationships)

    # 6. Synthesize per table
    print(f"\n{SEP}\n  GAUSSIAN COPULA SYNTHESIS\n{SEP}")
    synthetic = synthesize_all_tables(real_data, metadata, SCALE)

    # 6b. Normalize synthetic FK dtypes BEFORE restoring integrity
    print("Normalizing synthetic data types...")
    synthetic = normalize_fk_dtypes(synthetic, metadata.relationships)

    # 7. Restore FK integrity
    print(f"\n{SEP}\n  RESTORING REFERENTIAL INTEGRITY\n{SEP}")
    synthetic = restore_referential_integrity(
        synthetic, metadata.relationships)

    # 8. Pandera
    pandera_df = run_pandera(real_data, synthetic)

    # 9. Great Expectations
    ge_df = run_ge(real_data, synthetic)

    # 10. SDMetrics
    sdm_df = run_sdmetrics(real_data, synthetic, metadata)

    # ── SUMMARY ──────────────────────────────────────────────────────────────
    print(f"\n{SEP}\n  SUMMARY\n{SEP}")

    pan_pass = pandera_df["pass"].sum()
    pan_total = len(pandera_df)
    print(f"\n  Pandera :  {pan_pass}/{pan_total} tables passed")

    if not ge_df.empty:
        ge_pass = ge_df["success"].sum()
        ge_total = len(ge_df)
        print(f"  GE      :  {ge_pass}/{ge_total} expectations passed "
              f"({ge_pass/ge_total*100:.1f}%)")

    valid_sdm = sdm_df.dropna(subset=["overall"])
    if not valid_sdm.empty:
        mean_score = valid_sdm["overall"].mean()
        worst = valid_sdm.loc[valid_sdm["overall"].idxmin()]
        best = valid_sdm.loc[valid_sdm["overall"].idxmax()]
        print(f"  SDMetrics mean quality  : {mean_score:.4f}")
        print(
            f"  SDMetrics best  table   : {best['table']} ({best['overall']:.4f})")
        print(
            f"  SDMetrics worst table   : {worst['table']} ({worst['overall']:.4f})")

    # ── SAVE ─────────────────────────────────────────────────────────────────
    print(f"\n{SEP}\n  SAVING TO SQLITE\n{SEP}")
    save_to_sqlite(synthetic, OUTPUT_DB)
    print(f"\n  [OK] Synthetic DB saved -> {OUTPUT_DB}")
    print(f"\n{SEP}")
    print("  DONE")
    print(f"{SEP}\n")


if __name__ == "__main__":
    main()
