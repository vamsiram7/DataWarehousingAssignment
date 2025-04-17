import sqlite3
import pandas as pd
import os

# Database path
db_path = os.path.join("sql", "organizational_insights.db")

# Create SQLite connection
conn = sqlite3.connect(db_path)

# Mapping of cleaned output CSVs to table names
files = {
    "dim_employee": "outputs/dim_employee.csv",
    "dim_department": "outputs/dim_department.csv",
    "dim_expensetype": "outputs/dim_expensetype.csv",
    "dim_process": "outputs/dim_process.csv",
    "fact_hr": "outputs/fact_hr.csv",
    "fact_finance": "outputs/fact_finance.csv",
    "fact_operations": "outputs/fact_operations.csv"
}

# Load each CSV into the SQLite database
for table_name, csv_path in files.items():
    df = pd.read_csv(csv_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Loaded {table_name} into database.")

conn.close()
print("All tables loaded into sql/organizational_insights.db")
