import pandas as pd
import mysql.connector
import configparser

config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

conn = mysql.connector.connect(**DB_CONFIG)
df = pd.read_sql("SELECT * FROM audit_log ORDER BY timestamp DESC", conn)
conn.close()

print(df)
