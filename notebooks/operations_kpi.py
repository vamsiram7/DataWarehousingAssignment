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
fact_operations = pd.read_sql("SELECT * FROM fact_operations", conn)
dim_process = pd.read_sql("SELECT * FROM dim_process", conn)
dim_department = pd.read_sql("SELECT * FROM dim_department", conn)
conn.close()

# Merge Dimension Tables to Get Process Info and Department Info
ops = fact_operations.merge(dim_process, on='processid', how='left')
ops = ops.merge(dim_department, on='departmentid', how='left')

# Downtime by Process and Department
downtime_proc_dept = ops.groupby(['processname', 'department'])['downtimehours'].sum().reset_index()

# Display result
print("Total Downtime by Process and Department:")
print(downtime_proc_dept)
