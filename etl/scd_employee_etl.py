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

# --- Initialize SCD2 Table (drop + create cleanly) ---
def initialize_scd2_table(cursor):
    cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
    cursor.execute(f"""
        CREATE TABLE {TABLE_NAME} (
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

    # # Debugging: Print column names of both datasets
    # print("dim_employee columns:", dim_employee.columns)
    # print("fact_hr columns:", fact_hr.columns)

    # Ensure the Status column exists in fact_hr
    if 'Status' not in fact_hr.columns:
        raise ValueError("The 'Status' column is missing in fact_hr.csv")

    # Merge dim_employee with fact_hr to include the Status column
    merged_df = dim_employee.merge(fact_hr[['EmployeeID', 'Status']], on='EmployeeID', how='left')


    # Add StartDate, EndDate, and IsCurrent columns
    merged_df['StartDate'] = merged_df['DateOfJoining']
    merged_df['EndDate'] = None
    merged_df['IsCurrent'] = merged_df['Status'].apply(lambda x: 1 if x == "Active" else 0)

    # Fetch existing data from the SCD-2 table
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
                # Check for any change
                if (
                    current['Name'] != new_row['Name'] or
                    current['Gender'] != new_row['Gender'] or
                    current['ManagerID'] != new_row['ManagerID'] or
                    current['DateOfJoining'] != new_row['DateOfJoining'] or
                    current['IsCurrent'] != new_row['IsCurrent']
                ):
                    # Expire current
                    cursor.execute(f"""
                        UPDATE {TABLE_NAME}
                        SET EndDate = %s, IsCurrent = 0
                        WHERE EmployeeID = %s AND IsCurrent = 1
                    """, (datetime.today().strftime('%Y-%m-%d'), new_row['EmployeeID']))
                    inserts.append(new_row)

    # Insert new versions
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
