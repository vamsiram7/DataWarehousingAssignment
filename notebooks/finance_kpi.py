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

fact_finance = pd.read_sql("SELECT * FROM fact_finance", conn)
dim_expensetype = pd.read_sql("SELECT * FROM dim_expensetype", conn)
dim_department = pd.read_sql("SELECT * FROM dim_department", conn)
fact_hr = pd.read_sql("SELECT * FROM fact_hr", conn)

conn.close()

# Merge fact_finance with dim_expensetype to get expensetype
finance = fact_finance.merge(dim_expensetype, on='expensetypeid', how='left')

# Merge with fact_hr to get departmentid using employeeid
finance = finance.merge(fact_hr[['employeeid', 'departmentid']], on='employeeid', how='left')

# Merge with dim_department to get departmentname
finance = finance.merge(dim_department, on='departmentid', how='left')

# Convert DateKey to datetime and extract Month
finance['Date'] = pd.to_datetime(finance['datekey'], format='%Y%m%d', errors='coerce')
finance = finance[~finance['Date'].isna()]
finance['Month'] = finance['Date'].dt.to_period('M')

# Group by Month, Department, and ExpenseType
monthly_exp_dept_type = finance.groupby(['Month', 'department', 'expensetype'])['expenseamount'].sum().reset_index()

# Display result
print("Monthly Expenses by Department and Expense Type:")
print(monthly_exp_dept_type)
