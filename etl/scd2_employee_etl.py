import pandas as pd
import mysql.connector
import configparser
from datetime import datetime

# --- Load DB Config ---
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

TABLE_NAME = "dim_employee_scd2"

# --- Initialize SCD2 Table (create if not exists, NO DROP) ---
def initialize_scd2_table(cursor):
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
            employeeid INT,
            name VARCHAR(255),
            gender VARCHAR(255),
            managerid INT,
            startdate DATE,
            enddate DATE,
            iscurrent TINYINT,
            status VARCHAR(255),
            PRIMARY KEY (employeeid, startdate)
        )
    """)

# --- Apply SCD2 Logic ---
def apply_scd2(cursor, conn):
    # Load current dim_employee and fact_hr directly with mysql-connector
    dim_employee = pd.read_sql("SELECT * FROM dim_employee", conn)
    fact_hr = pd.read_sql("SELECT * FROM fact_hr", conn)

    # Check for data availability
    if dim_employee.empty or fact_hr.empty:
        print("dim_employee or fact_hr table is empty. Please run ETL first.")
        return

    # Merge employee info with status
    merged_df = dim_employee.merge(fact_hr[['employeeid', 'status', 'datekey']], on='employeeid', how='inner')
    merged_df['startdate'] = pd.to_datetime(merged_df['datekey'], format='%Y%m%d', errors='coerce')
    merged_df = merged_df[~merged_df['startdate'].isna()]
    merged_df['enddate'] = None
    merged_df['iscurrent'] = merged_df['status'].apply(lambda x: 1 if str(x).lower() == 'active' else 0)

    print(f"Merged new incoming employee records: {len(merged_df)}")

    if merged_df.empty:
        print("No valid new records to process.")
        return

    # Fetch existing active records
    try:
        cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE iscurrent = 1")
        existing_data = cursor.fetchall()

        if cursor.description:
            existing_columns = [desc[0] for desc in cursor.description]
        else:
            existing_columns = [
                'employeeid', 'name', 'gender', 'managerid',
                'startdate', 'enddate', 'iscurrent', 'status'
            ]

        existing_df = pd.DataFrame(existing_data, columns=existing_columns)

    except mysql.connector.Error as e:
        print(f"MySQL error: {e}")
        existing_df = pd.DataFrame(columns=[
            'employeeid', 'name', 'gender', 'managerid',
            'startdate', 'enddate', 'iscurrent', 'status'
        ])

    inserts = []

    for _, new_row in merged_df.iterrows():
        if not existing_df.empty:
            matched = existing_df[
                existing_df['employeeid'] == new_row['employeeid']
            ]
        else:
            matched = pd.DataFrame()

        if matched.empty:
            # New employee
            inserts.append(new_row)
        else:
            current = matched.iloc[0]
            # Compare important fields
            if (
                current['name'] != new_row['name'] or
                current['gender'] != new_row['gender'] or
                current['managerid'] != new_row['managerid'] or
                current['status'] != new_row['status']
            ):
                # Expire old active record
                cursor.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET enddate = %s, iscurrent = 0
                    WHERE employeeid = %s AND iscurrent = 1
                """, (datetime.today().strftime('%Y-%m-%d'), new_row['employeeid']))
                inserts.append(new_row)

    # Insert new active records
    if inserts:
        for _, row in pd.DataFrame(inserts).iterrows():
            placeholders = ", ".join(["%s"] * 8)
            columns = "employeeid, name, gender, managerid, startdate, enddate, iscurrent, status"
            insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(
                row[col] if pd.notna(row[col]) else None for col in [
                    'employeeid', 'name', 'gender', 'managerid', 'startdate', 'enddate', 'iscurrent', 'status'
                ]
            ))

# --- Runner ---
if __name__ == "__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        initialize_scd2_table(cursor)
        apply_scd2(cursor, conn)
    conn.commit()
    conn.close()
    print("✅ SCD2 changes applied successfully without SQLAlchemy, full working!")
