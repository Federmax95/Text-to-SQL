"""
🗃️ Northwind Schema Adapter (SQLite)
==================================================
Supporta:
- SQLite
"""

import sqlite3
from typing import Dict

class SchemaAdapter:
    def __init__(self, sqlite_path: str = None):
        self.sqlite_path = sqlite_path

    def _get_connection(self):
        """Apre una connessione SQLite"""
        if self.sqlite_path:
            return sqlite3.connect(self.sqlite_path)

    def extract_schema(self) -> Dict:
        if self.sqlite_path:
            return self._extract_from_sqlite()

    # =========================
    # 🔹 SQLITE
    # =========================
    def _extract_from_sqlite(self) -> Dict:

        schema = {
            "tables": {},
            "relationships": [],
            "valid_tables": set(),
            "valid_columns": {}
        }

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()

                cursor.execute(
                    "SELECT name FROM sqlite_master WHERE type='table' "
                    "AND name NOT LIKE 'sqlite_%' ORDER BY name"
                )
                tables = [row[0] for row in cursor.fetchall()]

                for table in tables:
                    schema["valid_tables"].add(table.lower())
                    schema["tables"][table] = {"columns": [], "sample_data": []}
                    cursor.execute(f"PRAGMA table_info('{table}')")
                    columns_info = cursor.fetchall()

                    for col in columns_info:
                        col_name = col[1]
                        #in SQLite il tipo può essere nullo per questo si usa come fallback text per passarlo all'llm
                        col_type = col[2] if col[2] else "TEXT"

                        schema["tables"][table]["columns"].append(f"{col_name} [{col_type}]")
                        #Aggiungo tutte le colonne valide in modo da fare un check se llm ha delle allucinazioni
                        schema["valid_columns"][f"{table}.{col_name}".lower()] = True

                    try:
                        cursor.execute(f"SELECT * FROM '{table}' LIMIT 2")
                        rows = cursor.fetchall()
                        if rows:
                            schema["tables"][table]["sample_data"] = rows
                    except Exception:
                        pass

                    cursor.execute(f"PRAGMA foreign_key_list('{table}')")
                    for fk in cursor.fetchall():
                        ref_table = fk[2]
                        from_col = fk[3]
                        to_col = fk[4]
                        #Aggiungo le relazioni da passare all'llm
                        schema["relationships"].append(f"{table}.{from_col} -> {ref_table}.{to_col}")

        except Exception as e:
            print(f"❌ Errore SQLite: {e}")

        return schema

    # =========================
    # 🔹 FORMAT
    # =========================
    def schema_to_text(self, schema: Dict) -> str:
        #Formatto il testo in md in quanto è la topologia che llm comprende meglio
        parts = ["### Tables:"]

        for table_name, info in schema["tables"].items():
            cols = [c.split(" ")[0] for c in info["columns"]]
            parts.append(f"{table_name}({', '.join(cols)})")

        parts.append("")
        parts.append("### Relationships:")

        for rel in schema["relationships"]:
            parts.append(rel)

        return "\n".join(parts)
