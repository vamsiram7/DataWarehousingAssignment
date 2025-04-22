import mysql.connector
import configparser
from datetime import datetime

config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def log_etl_run(table_name, action, records_inserted, status="success"):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS audit_log (
            id INT AUTO_INCREMENT PRIMARY KEY,
            table_name VARCHAR(100),
            action VARCHAR(100),
            records_inserted INT,
            status VARCHAR(20),
            timestamp DATETIME
        )
    """)

    cursor.execute("""
        INSERT INTO audit_log (table_name, action, records_inserted, status, timestamp)
        VALUES (%s, %s, %s, %s, %s)
    """, (table_name, action, records_inserted, status, datetime.now()))

    conn.commit()
    conn.close()
