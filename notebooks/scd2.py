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

# Connect to MySQL
conn = mysql.connector.connect(**DB_CONFIG)
df = pd.read_sql("SELECT * FROM dim_employee_scd2", conn)
conn.close()

# Split current and historical records
old = df[df['IsCurrent'] == 0].copy()
new = df[df['IsCurrent'] == 1].copy()

# Join on EmployeeID
merged = old.merge(new, on='EmployeeID', suffixes=('_OLD', '_NEW'))

# Track changes
change_rows = []

for _, row in merged.iterrows():
    changes = []
    for col in ['Name', 'Gender', 'ManagerID', 'DateOfJoining']:
        old_val = row[f"{col}_OLD"]
        new_val = row[f"{col}_NEW"]
        if old_val != new_val:
            changes.append(f"{col}: {old_val} → {new_val}")
    
    if changes:
        change_rows.append({
            'EmployeeID': row['EmployeeID'],
            'Changes': "; ".join(changes),
            'Status': row['Status_NEW']
        })

# Output changes
changes_df = pd.DataFrame(change_rows)
print("SCD Type 2 - Changed Fields Summary with Values:")
print(changes_df)
