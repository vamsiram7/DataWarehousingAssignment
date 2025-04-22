import pandas as pd
from datetime import datetime
from audit_logger import log_etl_run

INPUT_PATH = "./data/HR_Dataset_Dirty.xlsx"
OUTPUT_EMPLOYEE = "./outputs/dim_employee.csv"
OUTPUT_DEPARTMENT = "./outputs/dim_department.csv"
OUTPUT_FACT_HR = "./outputs/fact_hr.csv"

def clean_hr_data():
    hr_df = pd.read_excel(INPUT_PATH)

    hr_df['Department'] = hr_df['Department'].astype(str).str.strip().str.upper()
    hr_df['Gender'] = hr_df['Gender'].fillna('Unknown')
    hr_df['Salary'] = hr_df['Salary'].fillna(hr_df['Salary'].median())
    hr_df.dropna(subset=['EmployeeID', 'Department'], inplace=True)

    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)
            except:
                return pd.NaT

    hr_df['DateOfJoining'] = hr_df['DateOfJoining'].apply(parse_dates)
    hr_df = hr_df[~hr_df['DateOfJoining'].isnull()]
    hr_df.drop_duplicates(inplace=True)

    dim_department = pd.DataFrame(hr_df['Department'].unique(), columns=['DepartmentName'])
    dim_department['DepartmentID'] = range(1, len(dim_department) + 1)
    hr_df = hr_df.merge(dim_department, left_on='Department', right_on='DepartmentName', how='left')

    dim_employee = hr_df[['EmployeeID', 'Name', 'Gender', 'ManagerID', 'DateOfJoining']].drop_duplicates()

    fact_hr = hr_df[['EmployeeID', 'DepartmentID', 'Salary', 'Status', 'DateOfJoining']].copy()
    fact_hr['DateKey'] = fact_hr['DateOfJoining'].dt.strftime('%Y%m%d').astype(int)
    fact_hr.drop(columns='DateOfJoining', inplace=True)

    dim_employee.to_csv(OUTPUT_EMPLOYEE, index=False)
    dim_department.to_csv(OUTPUT_DEPARTMENT, index=False)
    fact_hr.to_csv(OUTPUT_FACT_HR, index=False)

    log_etl_run("dim_employee", "clean_transform", len(dim_employee))
    log_etl_run("dim_department", "clean_transform", len(dim_department))
    log_etl_run("fact_hr", "clean_transform", len(fact_hr))

if __name__ == "__main__":
    clean_hr_data()
    print("HR ETL complete.")
