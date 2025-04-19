#!/usr/bin/env python
# coding: utf-8

# In[7]:


import pandas as pd

# Load data
fact_fin = pd.read_csv('outputs/fact_finance.csv')
dim_exp = pd.read_csv('outputs/dim_expensetype.csv')

# Merge for ExpenseType
finance = fact_fin.merge(dim_exp, on='ExpenseTypeID', how='left')

# Convert DateKey to datetime
finance['Date'] = pd.to_datetime(fact_fin['DateKey'], format='%Y%m%d')
finance['Month'] = finance['Date'].dt.to_period('M')

# Monthly Expense by Type
monthly_expenses = finance.groupby(['Month', 'ExpenseTypeName'])['ExpenseAmount'].sum().reset_index()
print("Monthly Expenses by Type:")
print(monthly_expenses)


# In[ ]:




