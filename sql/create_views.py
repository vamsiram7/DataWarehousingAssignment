import mysql.connector
import configparser
import os

def get_mysql_config():
    config = configparser.ConfigParser()
    config_path = "sql/db_config.ini"

    if os.path.exists(config_path):
        config.read(config_path)
        return {
            "host": config['mysql']['host'],
            "user": config['mysql']['user'],
            "password": config['mysql']['password'],
            "database": config['mysql']['database']
        }
    else:
        return {
            "host": "localhost",
            "user": "root",
            "password": "your_password_here",
            "database": "organizational_insights"
        }

if __name__ == "__main__":
    mysql_config = get_mysql_config()
    conn = mysql.connector.connect(**mysql_config)
    cursor = conn.cursor()

    with open("sql/role_views.sql", "r") as f:
        sql_script = f.read()

    for statement in sql_script.split(";"):
        if statement.strip():
            cursor.execute(statement)

    conn.commit()
    cursor.close()
    conn.close()

    print("Role-based SQL views created in MySQL.")
