import pandas as pd
import os
from datetime import datetime
import mysql.connector

# File paths
INPUT_PATH = "./data/HR_Dataset_Dirty.xlsx"
OUTPUT_EMPLOYEE = "./outputs/dim_employee.csv"
OUTPUT_DEPARTMENT = "./outputs/dim_department.csv"
OUTPUT_FACT_HR = "./outputs/fact_hr.csv"

# MySQL connection details
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Adavi@14",
    "database": "organizational_insights"
}

def clean_hr_data():
    hr_df = pd.read_excel(INPUT_PATH)

    # Convert Department names to uppercase
    hr_df['Department'] = hr_df['Department'].astype(str).str.strip().str.upper()

    # Handle missing values
    hr_df['Gender'] = hr_df['Gender'].fillna('Unknown')
    hr_df['Salary'] = hr_df['Salary'].fillna(hr_df['Salary'].median())
    hr_df.dropna(subset=['EmployeeID', 'Department'], inplace=True)

    # Change the Date Format
    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)  # First try standard ISO
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)  # Then try dayfirst
            except:
                return pd.NaT

    hr_df['DateOfJoining'] = hr_df['DateOfJoining'].apply(parse_dates)
    hr_df = hr_df[~hr_df['DateOfJoining'].isnull()]

    # Remove duplicates
    hr_df.drop_duplicates(inplace=True)

    # Create Dimension Table Dim_Department
    dim_department = pd.DataFrame(hr_df['Department'].unique(), columns=['DepartmentName'])
    dim_department['DepartmentID'] = range(1, len(dim_department) + 1)

    # Merge to add DepartmentID
    hr_df = hr_df.merge(dim_department, left_on='Department', right_on='DepartmentName', how='left')

    # Create Dimension Table Dim_Employee
    dim_employee = hr_df[['EmployeeID', 'Name', 'Gender', 'ManagerID', 'DateOfJoining']].drop_duplicates()

    # Create Fact Table Fact_HR
    fact_hr = hr_df[['EmployeeID', 'DepartmentID', 'Salary', 'Status', 'DateOfJoining']].copy()
    fact_hr['DateKey'] = fact_hr['DateOfJoining'].dt.strftime('%Y%m%d').astype(int)
    fact_hr.drop(columns='DateOfJoining', inplace=True)

    # Export output files
    print("Saving cleaned output to /outputs/ directory...")
    dim_employee.to_csv(OUTPUT_EMPLOYEE, index=False)
    dim_department.to_csv(OUTPUT_DEPARTMENT, index=False)
    fact_hr.to_csv(OUTPUT_FACT_HR, index=False)

    print("HR ETL complete.")

def load_to_mysql(dim_employee, dim_department, fact_hr):
    """Load the cleaned data into MySQL tables."""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Load Dim_Employee
    cursor.execute("TRUNCATE TABLE dim_employee")
    for _, row in dim_employee.iterrows():
        cursor.execute(
            "INSERT INTO dim_employee (EmployeeID, Name, Gender, ManagerID, DateOfJoining) VALUES (%s, %s, %s, %s, %s)",
            (row['EmployeeID'], row['Name'], row['Gender'], row['ManagerID'], row['DateOfJoining'])
        )

    # Load Dim_Department
    cursor.execute("TRUNCATE TABLE dim_department")
    for _, row in dim_department.iterrows():
        cursor.execute(
            "INSERT INTO dim_department (DepartmentName, DepartmentID) VALUES (%s, %s)",
            (row['DepartmentName'], row['DepartmentID'])
        )

    # Load Fact_HR
    cursor.execute("TRUNCATE TABLE fact_hr")
    for _, row in fact_hr.iterrows():
        cursor.execute(
            "INSERT INTO fact_hr (EmployeeID, DepartmentID, Salary, Status, DateKey) VALUES (%s, %s, %s, %s, %s)",
            (row['EmployeeID'], row['DepartmentID'], row['Salary'], row['Status'], row['DateKey'])
        )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    clean_hr_data()

    # Load cleaned data into MySQL
    dim_employee = pd.read_csv(OUTPUT_EMPLOYEE)
    dim_department = pd.read_csv(OUTPUT_DEPARTMENT)
    fact_hr = pd.read_csv(OUTPUT_FACT_HR)

    print("Loading data into MySQL...")
    load_to_mysql(dim_employee, dim_department, fact_hr)
    print("Data successfully loaded into MySQL.")