import mysql.connector
import json
import os
import configparser
import getpass
import pandas as pd

def get_user_credentials(username, password):
    """Validate user credentials and return role if valid."""
    with open("sql/roles_config.json", "r") as f:
        roles_config = json.load(f)

    user = roles_config.get(username)
    if user and user["password"] == password:
        return user["role"]
    return None

def get_mysql_config():
    """Fetch MySQL connection info from db_config.ini"""
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
        print("Config file not found. Using default credentials.")
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

    # Create SQL views if not already created
    with open("sql/role_views.sql", "r") as f:
        sql_script = f.read()

    for statement in sql_script.split(";"):
        if statement.strip():
            cursor.execute(statement)

    conn.commit()
    cursor.close()
    conn.close()

    print("SQL views for role-based access created in MySQL.")

    # Prompt for user credentials
    username = input("Enter your username: ")
    password = getpass.getpass("Enter your password: ")

    role = get_user_credentials(username, password)

    if role == "HR":
        print("Accessing HR data...")
        query = "SELECT * FROM view_hr_user"
    elif role == "Finance":
        print("Accessing Finance data...")
        query = "SELECT * FROM view_finance_user"
    elif role == "Operations":
        print("Accessing Operations data...")
        query = "SELECT * FROM view_operations_user"
    elif role == "Super":
        print("Accessing all data...")
        query = "SELECT * FROM view_super_user"
    else:
        print("Access denied.")
        query = None

    if query:
        conn = mysql.connector.connect(**mysql_config)
        cursor = conn.cursor()
        cursor.execute(query)
        results = cursor.fetchall()
        df = pd.DataFrame(results, columns=[desc[0] for desc in cursor.description])
        print(df)
        conn.close()
