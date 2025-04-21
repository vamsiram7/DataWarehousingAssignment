import pandas as pd

# Load cleaned data
fact_hr = pd.read_csv('outputs/fact_hr.csv')
dim_employee = pd.read_csv('outputs/dim_employee.csv')

# Merge to enrich fact_hr with gender
hr = fact_hr.merge(dim_employee, on='EmployeeID', how='left')

# Headcount (Current Employees)
headcount = hr[hr['Status'].str.lower() == 'active']['EmployeeID'].nunique()
print("Current Headcount:", headcount)

# Attrition Rate (Resigned employee percentage)
resigned = hr[hr['Status'].str.lower() == 'resigned']['EmployeeID'].nunique()
total = hr['EmployeeID'].nunique()
attrition_rate = (resigned / total) * 100
print("Attrition Rate (%):", round(attrition_rate, 2))

# Average Salary by Gender
avg_salary_by_gender = hr.groupby('Gender')['Salary'].mean().reset_index()
print("Average Salary by Gender:")
print(avg_salary_by_gender)
