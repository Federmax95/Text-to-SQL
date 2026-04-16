"""
🗃️ Northwind Schema Adapter (MySQL + SQLite + File)
==================================================
Supporta:
- MySQL
- SQLite
- Schema da JSON o SQL
"""

import sqlite3
import json
import re
from typing import Dict
from app.core.config import DB_CONFIG

try:
    import mysql.connector
except ImportError:
    mysql = None


class NorthwindSchemaAdapter:
    def __init__(self, schema_file: str = None, sqlite_path: str = None):
        self.config = DB_CONFIG
        self.schema_file = schema_file
        self.sqlite_path = sqlite_path

    # =========================
    # 🔹 CONNECTION
    # =========================
    def _get_connection(self):
        if self.sqlite_path:
            return sqlite3.connect(self.sqlite_path)
        if mysql is None:
            raise ImportError(
                "mysql-connector-python non installato. Usa sqlite_path o installa il connector MySQL.")
        return mysql.connector.connect(**self.config)

    # =========================
    # 🔹 ENTRY POINT
    # =========================
    def extract_schema(self) -> Dict:
        if self.sqlite_path:
            return self._extract_from_sqlite()

        if self.schema_file:
            if self.schema_file.endswith(".json"):
                return self._extract_from_json()
            elif self.schema_file.endswith(".sql"):
                return self._extract_from_sql()

        return self._extract_from_db()

    # =========================
    # 🔹 MYSQL (ORIGINALE)
    # =========================
    def _extract_from_db(self) -> Dict:
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
                    "SHOW FULL TABLES WHERE Table_type = 'BASE TABLE'")
                tables = [row[0] for row in cursor.fetchall()]

                for table in tables:
                    schema["valid_tables"].add(table.lower())
                    schema["tables"][table] = {
                        "columns": [], "sample_data": []}

                    cursor.execute(f"DESCRIBE `{table}`")
                    columns_info = cursor.fetchall()

                    for col in columns_info:
                        col_name = col[0]
                        col_type = col[1]

                        schema["tables"][table]["columns"].append(
                            f"{col_name} [{col_type}]"
                        )
                        schema["valid_columns"][f"{table}.{col_name}".lower(
                        )] = True

                    try:
                        cursor.execute(f"SELECT * FROM `{table}` LIMIT 2")
                        rows = cursor.fetchall()
                        if rows:
                            schema["tables"][table]["sample_data"] = rows
                    except:
                        pass

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
                for t1, c1, t2, c2 in cursor.fetchall():
                    schema["relationships"].append(f"{t1}.{c1} -> {t2}.{c2}")

        except Exception as e:
            print(f"❌ Errore DB: {e}")

        return schema

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
                    schema["tables"][table] = {
                        "columns": [], "sample_data": []}

                    cursor.execute(f"PRAGMA table_info('{table}')")
                    columns_info = cursor.fetchall()

                    for col in columns_info:
                        col_name = col[1]
                        col_type = col[2] if col[2] else "TEXT"

                        schema["tables"][table]["columns"].append(
                            f"{col_name} [{col_type}]"
                        )
                        schema["valid_columns"][f"{table}.{col_name}".lower(
                        )] = True

                    try:
                        cursor.execute(f"SELECT * FROM '{table}' LIMIT 2")
                        rows = cursor.fetchall()
                        if rows:
                            schema["tables"][table]["sample_data"] = rows
                    except Exception:
                        pass

                    cursor.execute(f"PRAGMA foreign_key_list('{table}')")
                    for fk in cursor.fetchall():
                        # SQLite pragma columns:
                        # (id, seq, table, from, to, on_update, on_delete, match)
                        ref_table = fk[2]
                        from_col = fk[3]
                        to_col = fk[4]
                        schema["relationships"].append(
                            f"{table}.{from_col} -> {ref_table}.{to_col}"
                        )

        except Exception as e:
            print(f"❌ Errore SQLite: {e}")

        return schema

    # =========================
    # 🔹 JSON
    # =========================
    def _extract_from_json(self) -> Dict:
        schema = {
            "tables": {},
            "relationships": [],
            "valid_tables": set(),
            "valid_columns": {}
        }

        with open(self.schema_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        schema["tables"] = data.get("tables", {})
        schema["relationships"] = data.get("relationships", [])

        for table, info in schema["tables"].items():
            schema["valid_tables"].add(table.lower())

            for col in info.get("columns", []):
                col_name = col.split(" ")[0]
                schema["valid_columns"][f"{table}.{col_name}".lower()] = True

        return schema

    # =========================
    # 🔹 SQL PARSER
    # =========================
    def _extract_from_sql(self) -> Dict:
        schema = {
            "tables": {},
            "relationships": [],
            "valid_tables": set(),
            "valid_columns": {}
        }

        with open(self.schema_file, "r", encoding="utf-8") as f:
            sql = f.read()

        create_tables = re.findall(
            r"CREATE TABLE `?(\w+)`? \((.*?)\);",
            sql,
            re.S | re.IGNORECASE
        )

        for table_name, body in create_tables:
            columns = []

            for line in body.split("\n"):
                match = re.match(r"\s*`?(\w+)`?\s+([^\s,]+)", line)
                if match:
                    col_name, col_type = match.groups()
                    columns.append(f"{col_name} [{col_type}]")
                    schema["valid_columns"][f"{table_name}.{col_name}".lower(
                    )] = True

            schema["tables"][table_name] = {
                "columns": columns,
                "sample_data": []
            }

            schema["valid_tables"].add(table_name.lower())

        fk_matches = re.findall(
            r"FOREIGN KEY \(`?(\w+)`?\) REFERENCES `?(\w+)`? \(`?(\w+)`?\)",
            sql,
            re.IGNORECASE
        )

        for col, ref_table, ref_col in fk_matches:
            for table_name, info in schema["tables"].items():
                if any(col in c for c in info["columns"]):
                    schema["relationships"].append(
                        f"{table_name}.{col} -> {ref_table}.{ref_col}"
                    )

        return schema

    # =========================
    # 🔹 FORMAT
    # =========================
    def schema_to_text(self, schema: Dict) -> str:
        parts = ["### Tables:"]

        for table_name, info in schema["tables"].items():
            cols = [c.split(" ")[0] for c in info["columns"]]
            parts.append(f"{table_name}({', '.join(cols)})")

        parts.append("")
        parts.append("### Relationships:")

        for rel in schema["relationships"]:
            parts.append(rel)

        return "\n".join(parts)
