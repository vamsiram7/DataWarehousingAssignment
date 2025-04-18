import sqlite3
from datetime import datetime

DB_PATH = "sql/organizational_insights.db"

def log_etl_run(table_name, action, records_inserted):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Create audit table if it doesn't exist
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            table_name TEXT,
            action TEXT,
            records_inserted INTEGER,
            timestamp TEXT
        );
    """)

    # Insert log entry
    cursor.execute("""
        INSERT INTO audit_log (table_name, action, records_inserted, timestamp)
        VALUES (?, ?, ?, ?);
    """, (
        table_name,
        action,
        records_inserted,
        datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    ))

    conn.commit()
    conn.close()
