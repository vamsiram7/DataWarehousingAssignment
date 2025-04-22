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

# Connect and load tables from MySQL
conn = mysql.connector.connect(**DB_CONFIG)

fact_fin = pd.read_sql("SELECT * FROM fact_finance", conn)
dim_exp = pd.read_sql("SELECT * FROM dim_expensetype", conn)
dim_dept = pd.read_sql("SELECT * FROM dim_department", conn)
fact_hr = pd.read_sql("SELECT * FROM fact_hr", conn)

conn.close()

# Merge fact_finance with dim_expensetype to get ExpenseTypeName
finance = fact_fin.merge(dim_exp, on='ExpenseTypeID', how='left')

# Merge with fact_hr to get DepartmentID using EmployeeID
finance = finance.merge(fact_hr[['EmployeeID', 'DepartmentID']], on='EmployeeID', how='left')

# Merge with dim_department to get DepartmentName
finance = finance.merge(dim_dept, on='DepartmentID', how='left')

# Convert DateKey to datetime and extract Month
finance['Date'] = pd.to_datetime(finance['DateKey'], format='%Y%m%d', errors='coerce')
finance = finance[~finance['Date'].isna()]
finance['Month'] = finance['Date'].dt.to_period('M')

# Group by Month, DepartmentName, and ExpenseTypeName
monthly_exp_dept_type = finance.groupby(['Month', 'DepartmentName', 'ExpenseTypeName'])['ExpenseAmount'].sum().reset_index()

# Display result
print("Monthly Expenses by Department and Expense Type:")
print(monthly_exp_dept_type)
