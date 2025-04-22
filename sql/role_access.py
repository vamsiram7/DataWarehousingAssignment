import mysql.connector
import configparser
import getpass
import json
import pandas as pd
import os

def get_user_credentials(username, password):
    with open("sql/roles_config.json", "r") as f:
        roles_config = json.load(f)
    user = roles_config.get(username)
    if user and user["password"] == password:
        return user["role"]
    return None

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
