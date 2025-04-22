import pandas as pd
import mysql.connector
import configparser

# Load MySQL DB config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

# Load data
conn = mysql.connector.connect(**DB_CONFIG)
fact_ops = pd.read_sql("SELECT * FROM fact_operations", conn)
dim_proc = pd.read_sql("SELECT * FROM dim_process", conn)
conn.close()

# Merge Dimension Table to Get Process Info
ops = fact_ops.merge(dim_proc, on='ProcessID', how='left')

# Downtime by Process and Department
downtime_proc_dept = ops.groupby(['ProcessName', 'Department'])['DowntimeHours'].sum().reset_index()
print("Total Downtime by Process and Department:")
print(downtime_proc_dept)





