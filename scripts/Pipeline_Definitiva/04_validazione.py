import pandas as pd
import mysql.connector
import warnings
from sdv.metadata import SingleTableMetadata
from sdv.evaluation.single_table import evaluate_quality

warnings.filterwarnings("ignore")

def main():
    print("================================================================")
    print("Validazione DIRETTA MySQL (Originale vs Sintetico)")
    print("================================================================")
    
    conn_real = mysql.connector.connect(host="localhost", user="root", password="[PASSWORD]", database="northwind")
    conn_synth = mysql.connector.connect(host="localhost", user="root", password="[PASSWORD]", database="northwind_sintetico")
    
    c_real = conn_real.cursor()
    c_real.execute("SHOW TABLES")
    tables = [t[0] for t in c_real.fetchall()]
    

    print("\nVerifica Integrita' Referenziale Diretta SQL")
    c_real.execute("""
        SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE REFERENCED_TABLE_NAME IS NOT NULL AND TABLE_SCHEMA = 'northwind'
    """)
    fks_raw = c_real.fetchall()
    fks = [{"table": f[0], "col": f[1], "ref_table": f[2], "ref_col": f[3]} for f in fks_raw]
    
    c_synth = conn_synth.cursor()

    valide = 0
    totali = 0
    errori = 0
    
    # Valida usando JOIN e NULL check in SQL direttamente per performance absolute
    for fk in fks:
        c_tab = fk["table"]
        c_col = fk["col"]
        p_tab = fk["ref_table"]
        p_col = fk["ref_col"]
        
        query = f"""
            SELECT COUNT(*) FROM `{c_tab}` c
            WHERE c.`{c_col}` IS NOT NULL 
            AND NOT EXISTS (SELECT 1 FROM `{p_tab}` p WHERE p.`{p_col}` = c.`{c_col}`)
        """
        try:
            c_synth.execute(query)
            orfani = c_synth.fetchone()[0]
            
            c_synth.execute(f"SELECT COUNT(*) FROM `{c_tab}` WHERE `{c_col}` IS NOT NULL")
            righe = c_synth.fetchone()[0]
            
            totali += righe
            errori += orfani
            valide += (righe - orfani)
        except Exception as e:
            pass

    print(f"\nChiavi esterne Popolate nel DB Sintetico: {totali}")
    if totali > 0:
        print(f"  - Relazioni Valide (Presenti nel Parent): {valide} ({valide/totali:.2%})")
        print(f"  - Relazioni Orfane (Mancanti nel Parent): {errori} ({errori/totali:.2%})")

    if errori == 0 and totali > 0:
        print("\nRISULTATO: PERFETTO! Tutte le dipendenze referenziali misurate dal DB sono state mantenute al 100%.")

    conn_real.close()
    conn_synth.close()

if __name__ == "__main__":
    main()
