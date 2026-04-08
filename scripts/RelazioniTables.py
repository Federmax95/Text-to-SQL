import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="[PASSWORD]",
    database="northwind"
)


cursor = conn.cursor()

query = """
SELECT 
    TABLE_NAME, 
    COLUMN_NAME, 
    REFERENCED_TABLE_NAME, 
    REFERENCED_COLUMN_NAME
FROM 
    INFORMATION_SCHEMA.KEY_COLUMN_USAGE
WHERE 
    REFERENCED_TABLE_NAME IS NOT NULL
    AND TABLE_SCHEMA = 'northwind';
"""

cursor.execute(query)

relations = cursor.fetchall()

print("\n🔗 RELAZIONI TRA TABELLE:\n")

for rel in relations:
    table, column, ref_table, ref_column = rel
    print(f"{table}.{column} → {ref_table}.{ref_column}")

cursor.close()
conn.close()
