#!/usr/bin/env python
# coding: utf-8

# In[3]:


import pandas as pd

# Load data
fact_ops = pd.read_csv('../outputs/fact_operations.csv')
dim_proc = pd.read_csv('../outputs/dim_process.csv')

# Merge process info
ops = fact_ops.merge(dim_proc, on='ProcessID', how='left')

# Downtime by Process
downtime_process = ops.groupby('ProcessName')['DowntimeHours'].sum().reset_index()
print("Total Downtime by Process:")
print(downtime_process)

# Downtime by Process and Department
downtime_proc_dept = ops.groupby(['ProcessName', 'Department'])['DowntimeHours'].sum().reset_index()
print("Total Downtime by Process and Department:")
print(downtime_proc_dept)


# In[ ]:





# In[ ]:




