"""
🗃️ Northwind Schema Adapter
============================
Si connette a MySQL per estrarre programmaticamente le definizioni
delle tabelle, le foreign keys e i dati di esempio (sample data)
per abbattere le instabilità logiche del LLM (RAG Context).
"""

import mysql.connector
from typing import Dict, List, Tuple
from app.core.config import DB_CONFIG

class NorthwindSchemaAdapter:
    def __init__(self):
        self.config = DB_CONFIG
        
    def _get_connection(self):
        return mysql.connector.connect(**self.config)

    def extract_schema(self) -> Dict:
        """Estrae l'intero schema del database (tabelle, colonne, fks, data)."""
        schema = {
            "tables": {},
            "relationships": [],
            "valid_tables": set(),
            "valid_columns": {}
        }
        
        try:
            with self._get_connection() as conn:
                with conn.cursor() as cursor:
                    # 1. Recupera tutte le tabelle
                    cursor.execute("SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
                    tables = [row[0] for row in cursor.fetchall()]
                    
                    for table in tables:
                        schema["valid_tables"].add(table.lower())
                        schema["tables"][table] = {"columns": [], "sample_data": []}
                        
                        # 2. Recupera le colonne per tabella
                        cursor.execute(f"DESCRIBE `{table}`")
                        columns_info = cursor.fetchall()
                        for col in columns_info:
                            col_name = col[0]
                            col_type = col[1]
                            schema["tables"][table]["columns"].append(f"{col_name} [{col_type}]")
                            schema["valid_columns"][f"{table}.{col_name}".lower()] = True
                            
                        try:
                            cursor.execute(f"SELECT * FROM `{table}` LIMIT 2")
                            rows = cursor.fetchall()
                            if rows:
                                schema["tables"][table]["sample_data"] = rows
                        except Exception as e:
                            print(f"⚠️ Attenzione: Impossibile estrarre sample data da {table} ({e})")

                    db_name = self.config["database"]
                    fk_query = f"""
                        SELECT 
                            TABLE_NAME, COLUMN_NAME,
                            REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                        WHERE REFERENCED_TABLE_SCHEMA = '{db_name}'
                          AND TABLE_SCHEMA = '{db_name}';
                    """
                    cursor.execute(fk_query)
                    fks = cursor.fetchall()
                    for fk in fks:
                        t1, c1, t2, c2 = fk
                        schema["relationships"].append(f"{t1}.{c1} -> {t2}.{c2}")
                        
        except mysql.connector.Error as e:
            print(f"❌ Errore critico connessione MySQL: {e}")
            
        return schema

    def schema_to_text(self, schema: Dict) -> str:
        """Converte il dict schema nel testo finale da infilare nei prompt."""
        parts = ["### Tables:"]
        
        for table_name, info in schema["tables"].items():
            cols = info["columns"]
            parts.append(f"{table_name}({', '.join(cols)})")
            if info["sample_data"]:
                rows_str = str(info["sample_data"])
                if len(rows_str) > 250:
                    rows_str = rows_str[:247] + "..."
                parts.append(f"  Example rows: {rows_str}")
        
        parts.append("")
        parts.append("### Relationships:")
        for rel in schema["relationships"]:
            parts.append(rel)
            
        return "\n".join(parts)