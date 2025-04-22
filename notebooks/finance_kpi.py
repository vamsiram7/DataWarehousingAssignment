import pandas as pd

# Load data
fact_fin = pd.read_csv('outputs/fact_finance.csv')
dim_exp = pd.read_csv('outputs/dim_expensetype.csv')
dim_dept = pd.read_csv('outputs/dim_department.csv')
fact_hr = pd.read_csv('outputs/fact_hr.csv')

# Merge fact_finance with dim_expensetype to get ExpenseTypeName
finance = fact_fin.merge(dim_exp, on='ExpenseTypeID', how='left')

# Merge with fact_hr to get DepartmentID using EmployeeID
finance = finance.merge(fact_hr[['EmployeeID', 'DepartmentID']], on='EmployeeID', how='left')

# Merge with dim_department to get DepartmentName
finance = finance.merge(dim_dept, on='DepartmentID', how='left')

# Convert DateKey to datetime and extract Month
finance['Date'] = pd.to_datetime(finance['DateKey'], format='%Y%m%d')
finance['Month'] = finance['Date'].dt.to_period('M')

# Group by Month, DepartmentName, and ExpenseTypeName
monthly_exp_dept_type = finance.groupby(['Month', 'DepartmentName', 'ExpenseTypeName'])['ExpenseAmount'].sum().reset_index()

# Display result
print("Monthly Expenses by Department and Expense Type:")
print(monthly_exp_dept_type)

# Check for missing DepartmentID in fact_hr
#missing_dept_id = finance[finance['DepartmentID'].isna()]
#print(missing_dept_id)

# Check for unmatched DepartmentID in dim_department
#unmatched_dept = finance[~finance['DepartmentID'].isin(dim_dept['DepartmentID'])]
#print(unmatched_dept)