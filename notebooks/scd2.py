import pandas as pd
import mysql.connector
import configparser

# Load DB Config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

# Connect and load table
with mysql.connector.connect(**DB_CONFIG) as conn:
    df = pd.read_sql("SELECT * FROM dim_employee_scd2", conn)

print(df)
