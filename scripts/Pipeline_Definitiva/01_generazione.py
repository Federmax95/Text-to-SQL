import pandas as pd
import mysql.connector
import os
import warnings
import numpy as np
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

warnings.filterwarnings("ignore")

def main():
    print("================================================================")
    print("Generazione Dati Sintetici con SDV (Preservazione NULL)")
    print("================================================================")
    
    try:
        conn = mysql.connector.connect(
            host="localhost", user="root", password="", database="northwind"
        )
        cursor = conn.cursor()
        cursor.execute("SHOW TABLES")
        tables = [t[0] for t in cursor.fetchall()]
    except Exception as e:
        print(f"Errore di connessione: {e}")
        return

    print(f"\n[1/3] Estrazione dati: {len(tables)} tabelle trovate.")
    out_dir = "Dataset_Sintetico"
    os.makedirs(out_dir, exist_ok=True)
    
    print("\n[2/3] Addestramento e Generazione...")
    for table_name in tables:
        df = pd.read_sql(f"SELECT * FROM `{table_name}`", conn)
        
        if df.empty:
            print(f"- {table_name}: vuota. Salto.")
            continue
            
        print(f"Elaborazione: {table_name} ({len(df)} righe)")

        # --- GESTIONE ID E STRINGHE ---
        # Evitiamo di convertire i NULL in stringhe "None"
        for col in df.columns:
            if 'id' in col.lower() or df[col].dtype == 'object':
                # Converte in stringa ma mantiene i NaN reali di Pandas/Numpy
                df[col] = df[col].astype(str).mask(df[col].isna())

        # --- LOGICA DI FILTRO ---
        # 1. Identifica colonne totalmente vuote
        all_null_cols = [col for col in df.columns if df[col].isnull().all()]
        # 2. Salva la maschera dei nulli per le colonne parzialmente vuote
        null_mask = df.isnull()

        try:
            # Creiamo i metadati sul dataframe originale per mantenere la struttura
            metadata = SingleTableMetadata()
            metadata.detect_from_dataframe(df)
            
            # Addestramento
            synthesizer = GaussianCopulaSynthesizer(metadata)
            synthesizer.fit(df)
            
            # Generazione
            synthetic_df = synthesizer.sample(num_rows=len(df))

            # --- RIPRISTINO REALE DEI NULL ---
            # Se l'originale aveva un NULL in quella cella, lo forziamo anche nel sintetico
            # Questo impedisce a SDV di "inventare" contenuti dove non c'erano
            synthetic_df = synthetic_df.mask(null_mask)
            
            # Per le colonne che erano 100% NULL, assicuriamoci che lo siano ancora
            for col in all_null_cols:
                synthetic_df[col] = np.nan

            synthetic_df.to_csv(os.path.join(out_dir, f"{table_name}.csv"), index=False)
            print(f"  -> {table_name}.csv salvato (Struttura NULL preservata).")
            
        except Exception as e:
            print(f"  -> ERRORE nella tabella {table_name}: {e}")
            
    conn.close()
    print(f"\n[3/3] Completato! I file sono in: {out_dir}")

if __name__ == "__main__":
    main()