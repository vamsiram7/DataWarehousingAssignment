#!/usr/bin/env python
# coding: utf-8

# In[5]:


import sqlite3
import pandas as pd

conn = sqlite3.connect("../sql/organizational_insights.db")
df = pd.read_sql("SELECT * FROM audit_log ORDER BY timestamp DESC", conn)
conn.close()

print(df)


# In[ ]:




