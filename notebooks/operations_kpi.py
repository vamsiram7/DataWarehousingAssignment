import pandas as pd

# Load data
fact_ops = pd.read_csv('outputs/fact_operations.csv')
dim_proc = pd.read_csv('outputs/dim_process.csv')

# Merge Dimension Table to Get Process Info
ops = fact_ops.merge(dim_proc, on='ProcessID', how='left')

# Downtime by Process and Department
downtime_proc_dept = ops.groupby(['ProcessName', 'Department'])['DowntimeHours'].sum().reset_index()
print("Total Downtime by Process and Department:")
print(downtime_proc_dept)





