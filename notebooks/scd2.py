import pandas as pd
import mysql.connector
import configparser

# Load DB Config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

# Connect and load table
with mysql.connector.connect(**DB_CONFIG) as conn:
    df = pd.read_sql("SELECT * FROM dim_employee_scd2", conn)

# Identify employees with both old and new rows
changed_ids = df.groupby('EmployeeID').filter(lambda x: x['IsCurrent'].nunique() > 1)['EmployeeID'].unique()

# Filter and merge old + new records
old_df = df[(df['EmployeeID'].isin(changed_ids)) & (df['IsCurrent'] == 0)]
new_df = df[(df['EmployeeID'].isin(changed_ids)) & (df['IsCurrent'] == 1)]

merged = old_df.merge(new_df, on="EmployeeID", suffixes=('_OLD', '_NEW'))

# Filter only those where tracked fields actually changed
tracked_fields = ['Name', 'Gender', 'ManagerID', 'DateOfJoining']
has_diff = []

for _, row in merged.iterrows():
    for field in tracked_fields:
        if str(row[f'{field}_OLD']) != str(row[f'{field}_NEW']):
            has_diff.append(row)
            break  # No need to check further if any one field is different

# Convert to DataFrame and print
diff_df = pd.DataFrame(has_diff)
pd.set_option('display.max_columns', None)

print("\n SCD Type 2 - Real Changes (OLD vs NEW):\n")
if not diff_df.empty:
    print(diff_df.sort_values(by='EmployeeID'))
else:
    print("No actual changes in tracked fields detected.")
