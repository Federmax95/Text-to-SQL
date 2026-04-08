import mysql.connector

conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="[PASSWORD]",
    database="northwind"
)


cursor = conn.cursor()

cursor.execute("SHOW TABLES")
tables = cursor.fetchall()

for table in tables:
    print(f"\nTable: {table[0]}")
    cursor.execute(f"DESCRIBE {table[0]}")
    for col in cursor.fetchall():
        print(f"- {col[0]} ({col[1]})")
