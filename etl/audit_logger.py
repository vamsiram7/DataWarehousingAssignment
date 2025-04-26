import mysql.connector
import configparser

# Read database config
config = configparser.ConfigParser()
config.read('sql/db_config.ini')

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def log_audit_event(table_name, operation_type, row_count):
    """Logs an audit event into the audit_log table."""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    insert_query = """
        INSERT INTO audit_log (operation_type, table_name, row_count)
        VALUES (%s, %s, %s)
    """
    cursor.execute(insert_query, (operation_type, table_name, row_count))
    conn.commit()

    cursor.close()
    conn.close()

if __name__ == "__main__":
    print("Audit Logger ready.")
