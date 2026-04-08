import pandas as pd
import json
import os
import mysql.connector
import numpy as np

def main():
    print("================================================================")
    print("Allineamento Relazionale (Foreign Keys) Dataset Sintetici")
    print("================================================================")

    out_dir = "Dataset_Sintetico"
    
    # 1. Recupera le chiavi esterne dal database originale
    conn = mysql.connector.connect(
        host="localhost", user="root", password="[PASSWORD]", database="northwind"
    )
    cursor = conn.cursor()
    cursor.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA = 'northwind'
    """)
    fks_raw = cursor.fetchall()
    fks = [{"table": f[0], "col": f[1], "ref_table": f[2], "ref_col": f[3]} for f in fks_raw]
    conn.close()

    # 2. Carica i dataframe sintetici estratti
    dfs = {}
    for table_name in set([f["table"] for f in fks] + [f["ref_table"] for f in fks]):
        filepath = os.path.join(out_dir, f"{table_name}.csv")
        if os.path.exists(filepath):
            dfs[table_name] = pd.read_csv(filepath, dtype=str) # Carica come stringa per facilitare mapping

    # 3. Allineamento FKs -> PKs
    for fk in fks:
        child_table = fk["table"]
        child_col = fk["col"]
        parent_table = fk["ref_table"]
        parent_col = fk["ref_col"]

        print(f"Allineamento: {child_table}.{child_col} -> {parent_table}.{parent_col}")

        if child_table in dfs and parent_table in dfs:
            df_child = dfs[child_table]
            df_parent = dfs[parent_table]

            if parent_col in df_parent.columns and child_col in df_child.columns:
                valid_pks = df_parent[parent_col].dropna().unique()
                
                if len(valid_pks) == 0:
                    print(f"  [ATTENZIONE] Nessuna Primary Key valida in {parent_table}.{parent_col}")
                    continue

                # Gestiamo i missing values pre-esistenti generated (se previsto)
                mask = df_child[child_col].notna()
                
                # Assegnazione di chiavi esterne per ri-mappare la relazionalita' perfetta
                valid_fks = np.random.choice(valid_pks, size=mask.sum(), replace=True)
                df_child.loc[mask, child_col] = valid_fks

    # 4. Sovrascrittura dei vecchi datasets con i nuovi datasets referenziati
    print("\nSalvataggio dataset aggiornati...")
    for table_name, df in dfs.items():
        filepath = os.path.join(out_dir, f"{table_name}.csv")
        df.to_csv(filepath, index=False)
        print(f" -> Aggiornato: {table_name}.csv")

    print(f"\nOperazione completata! Tutte le foreign keys mappano chiavi primarie esistenti.")

if __name__ == "__main__":
    main()
