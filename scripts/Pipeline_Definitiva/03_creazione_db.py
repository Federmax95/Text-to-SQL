import pandas as pd
import os
import mysql.connector

def main():
    print("Creazione DB northwind_sintetico in corso...")
    
    # Crea DB
    conn = mysql.connector.connect(host="localhost", user="root", password="[PASSWORD]")
    cursor = conn.cursor()
    cursor.execute("DROP DATABASE IF EXISTS `northwind_sintetico`;")
    cursor.execute("CREATE DATABASE `northwind_sintetico`;")
    cursor.close()
    conn.close()
    
    # Importiamo usando sqlalchemy tramite URI
    try:
        from sqlalchemy import create_engine
    except ImportError:
        print("Devi installare sqlalchemy (pip install sqlalchemy)")
        return
        
    engine = create_engine("mysql+mysqlconnector://root:[PASSWORD]@localhost/northwind_sintetico")
    
    out_dir = "Dataset_Sintetico"
    csv_files = [f for f in os.listdir(out_dir) if f.endswith('.csv')]
    
    for file in csv_files:
        table_name = file.replace('.csv', '')
        filepath = os.path.join(out_dir, file)
        
        df = pd.read_csv(filepath)
        if df.empty:
            continue
            
        print(f"Importando {table_name}...")
        # if_exists='replace' crea la tabella in base ai tipi di pandas
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
    print("\nCompletato! Il database `northwind_sintetico` contiene tutte le tabelle.")

if __name__ == "__main__":
    main()
