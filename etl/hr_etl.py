import pandas as pd
import os
from datetime import datetime

# Setup paths
INPUT_PATH = "./data/HR_Dataset_Dirty.xlsx"
OUTPUT_EMPLOYEE = "./outputs/dim_employee.csv"
OUTPUT_DEPARTMENT = "./outputs/dim_department.csv"
OUTPUT_FACT_HR = "./outputs/fact_hr.csv"

def clean_hr_data():
    print("Reading raw HR data...")
    hr_df = pd.read_excel(INPUT_PATH)

    # Convert Department to uppercase (for uniformity)
    print("Converting department names to uppercase...")
    hr_df['Department'] = hr_df['Department'].astype(str).str.strip().str.upper()

    # Handle missing values
    print("Handling missing values...")
    hr_df['Gender'] = hr_df['Gender'].fillna('Unknown')
    hr_df['Salary'] = hr_df['Salary'].fillna(hr_df['Salary'].median())
    hr_df.dropna(subset=['EmployeeID', 'Department'], inplace=True)

    # Format joining dates
    print("Formatting joining dates...")
    hr_df['DateOfJoining'] = pd.to_datetime(hr_df['DateOfJoining'], errors='coerce')
    hr_df = hr_df[~hr_df['DateOfJoining'].isnull()]

    # Remove duplicates
    hr_df.drop_duplicates(inplace=True)

    # Create Dim_Department
    print("Creating Dim_Department...")
    dim_department = pd.DataFrame(hr_df['Department'].unique(), columns=['DepartmentName'])
    dim_department['DepartmentID'] = range(1, len(dim_department) + 1)

    # Merge to add DepartmentID
    hr_df = hr_df.merge(dim_department, left_on='Department', right_on='DepartmentName', how='left')

    # Create Dim_Employee
    print("Creating Dim_Employee...")
    dim_employee = hr_df[['EmployeeID', 'Name', 'Gender', 'ManagerID', 'DateOfJoining']].drop_duplicates()

    # Create Fact_HR
    print("Creating Fact_HR...")
    fact_hr = hr_df[['EmployeeID', 'DepartmentID', 'Salary', 'Status', 'DateOfJoining']].copy()
    fact_hr['DateKey'] = fact_hr['DateOfJoining'].dt.strftime('%Y%m%d').astype(int)
    fact_hr.drop(columns='DateOfJoining', inplace=True)

    # Export output files
    print("Saving cleaned output to /outputs/ directory...")
    dim_employee.to_csv(OUTPUT_EMPLOYEE, index=False)
    dim_department.to_csv(OUTPUT_DEPARTMENT, index=False)
    fact_hr.to_csv(OUTPUT_FACT_HR, index=False)

    print("HR ETL complete.")

if __name__ == "__main__":
    clean_hr_data()
