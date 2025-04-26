import pandas as pd
import mysql.connector
import configparser

# Load DB config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

# Connect to MySQL and fetch data
conn = mysql.connector.connect(**DB_CONFIG)

# Load tables as DataFrames
fact_hr = pd.read_sql("SELECT * FROM fact_hr", conn)
dim_employee = pd.read_sql("SELECT * FROM dim_employee", conn)

conn.close()

# Merge to enrich fact_hr with gender
hr = fact_hr.merge(dim_employee, on='employeeid', how='left')

# Headcount (Current Employees)
headcount = hr[hr['status'].str.lower() == 'active']['employeeid'].nunique()
print("Current Headcount:", headcount)

# Attrition Rate
resigned = hr[hr['status'].str.lower() == 'resigned']['employeeid'].nunique()
total = hr['employeeid'].nunique()
attrition_rate = (resigned / total) * 100
print("Attrition Rate (%):", round(attrition_rate, 2))

# Average Salary by Gender
avg_salary_by_gender = hr.groupby('gender')['salary'].mean().reset_index()
print("Average Salary by Gender:")
print(avg_salary_by_gender)
