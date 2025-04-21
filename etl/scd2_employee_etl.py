import pandas as pd
import mysql.connector
from datetime import datetime
import configparser

# --- Load DB Config ---
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

INPUT_PATH = "outputs/dim_employee.csv"
TABLE_NAME = "dim_employee_scd2"

# --- Initialize SCD2 Table ---
def initialize_scd2_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            EmployeeID INTEGER,
            Name TEXT,
            Gender TEXT,
            ManagerID INTEGER,
            DateOfJoining DATE,
            StartDate DATE,
            EndDate DATE,
            IsCurrent INTEGER,
            Status TEXT
        )
    """)

# --- SCD2 Logic ---
def apply_scd2(cursor):
    # Load dim_employee and fact_hr datasets
    dim_employee = pd.read_csv("outputs/dim_employee.csv")
    fact_hr = pd.read_csv("outputs/fact_hr.csv")

    # Convert DateOfJoining to standard YYYY-MM-DD format
    dim_employee['DateOfJoining'] = pd.to_datetime(dim_employee['DateOfJoining'], errors='coerce')
    dim_employee['DateOfJoining'] = dim_employee['DateOfJoining'].dt.strftime('%Y-%m-%d')

    if 'Status' not in fact_hr.columns:
        raise ValueError("The 'Status' column is missing in fact_hr.csv")

    # Merge to bring in Status
    merged_df = dim_employee.merge(fact_hr[['EmployeeID', 'Status']], on='EmployeeID', how='left')

    # Add required columns
    merged_df['StartDate'] = merged_df['DateOfJoining']
    merged_df['EndDate'] = None
    merged_df['IsCurrent'] = merged_df['Status'].apply(lambda x: 1 if x == "Active" else 0)

    # Fetch existing SCD2 data
    cursor.execute(f"SELECT * FROM {TABLE_NAME}")
    existing_data = cursor.fetchall()
    existing_columns = [desc[0] for desc in cursor.description]
    existing_df = pd.DataFrame(existing_data, columns=existing_columns)

    inserts = []

    for _, new_row in merged_df.iterrows():
        matched = existing_df[
            (existing_df['EmployeeID'] == new_row['EmployeeID']) & 
            (existing_df['IsCurrent'] == 1)
        ]

        if matched.empty:
            inserts.append(new_row)
        else:
            for _, current in matched.iterrows():
                if (
                    current['Name'] != new_row['Name'] or
                    current['Gender'] != new_row['Gender'] or
                    current['ManagerID'] != new_row['ManagerID'] or
                    current['DateOfJoining'] != new_row['DateOfJoining'] or
                    current['IsCurrent'] != new_row['IsCurrent']
                ):
                    # Expire current row
                    cursor.execute(f"""
                        UPDATE {TABLE_NAME}
                        SET EndDate = %s, IsCurrent = 0
                        WHERE EmployeeID = %s AND IsCurrent = 1
                    """, (datetime.today().strftime('%Y-%m-%d'), new_row['EmployeeID']))
                    inserts.append(new_row)

    # Insert updated records
    if inserts:
        for _, row in pd.DataFrame(inserts).iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join(row.index)
            insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))

# --- Runner ---
if __name__ == "__main__":
    with mysql.connector.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cursor:
            initialize_scd2_table(cursor)
            apply_scd2(cursor)
        conn.commit()
    print("SCD Type 2 changes applied to dim_employee_scd2.")
