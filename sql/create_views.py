import sqlite3

conn = sqlite3.connect("sql/organizational_insights.db")

with open("sql/role_views.sql", "r") as f:
    sql_script = f.read()

conn.executescript(sql_script)
conn.commit()
conn.close()

print("SQL views for role-based access created.")
