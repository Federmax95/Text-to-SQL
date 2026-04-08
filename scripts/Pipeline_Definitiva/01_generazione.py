import pandas as pd
import mysql.connector
import os
import warnings
from sdv.metadata import SingleTableMetadata
from sdv.single_table import GaussianCopulaSynthesizer

warnings.filterwarnings("ignore")

def main():
    print("================================================================")
    print("Generazione Dati Sintetici con SDV (Approccio Single Table)")
    print("Nota: Utilizzato per compatibilita con Pandas 2.x")
    print("================================================================")
    
    conn = mysql.connector.connect(
        host="localhost", user="root", password="[PASSWORD]", database="northwind"
    )
    cursor = conn.cursor()
    
    # 1. Recupera tabelle
    cursor.execute("SHOW TABLES")
    tables = [t[0] for t in cursor.fetchall()]
    
    print("\n[1/3] Estrazione dati da database MySQL...")
    data = {}
    for table in tables:
        data[table] = pd.read_sql(f"SELECT * FROM `{table}`", conn)
    conn.close()
    print(f"Estratte {len(data)} tabelle dal database.")
    
    out_dir = "Dataset_Sintetico"
    os.makedirs(out_dir, exist_ok=True)
    
    print("\n[2/3] Addestramento Modelli Singoli e Generazione...")
    for table_name in tables:
        df = data[table_name]
        if df.empty:
            print(f"- {table_name}: vuota. Salto.")
            continue
            
        print(f"Modellazione tabella: {table_name} ({len(df)} righe)")
        
        # Converte colonne IDs e FKs ipotetiche in stringhe per evitare inferenze numeriche anomale
        for col in df.columns:
            if 'id' in col.lower() or df[col].dtype == 'object':
                df[col] = df[col].astype(str)
                
        # Estrazione metadati singoli
        metadata = SingleTableMetadata()
        try:
            metadata.detect_from_dataframe(df)
            
            # Se ci sono primary_keys esplicite, aggiorniamole ma SingleTable puo farne a meno.
            # Rimuoviamo il tag PII automatico perche puo dare problemi in GaussianCopula senza specificare formattatori.
            # SDV handle PII fields as anonymized directly!
            
            synthesizer = GaussianCopulaSynthesizer(metadata)
            synthesizer.fit(df)
            
            # 3. Generazione Dati
            synthetic_df = synthesizer.sample(num_rows=len(df))
            synthetic_df.to_csv(os.path.join(out_dir, f"{table_name}.csv"), index=False)
            print(f"  -> {table_name}.csv salvato con successo.")
        except Exception as e:
            print(f"  -> ERRORE nella tabella {table_name}: {e}")
            
    print(f"\n[3/3] Operazione completata! Dataset sintetici salvati in: {out_dir}")

if __name__ == "__main__":
    main()
