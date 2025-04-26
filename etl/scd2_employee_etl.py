import pandas as pd
import mysql.connector
import configparser
from datetime import datetime
from audit_logger import log_audit_event

# Load DB Config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

TABLE_NAME = "dim_employee_scd2"

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

def apply_scd2(cursor, conn):
    dim_employee = pd.read_sql("SELECT * FROM dim_employee", conn)
    fact_hr = pd.read_sql("SELECT * FROM fact_hr", conn)

    if dim_employee.empty or fact_hr.empty:
        print("dim_employee or fact_hr table is empty. Please run ETL first.")
        return

    merged_df = dim_employee.merge(fact_hr[['employeeid', 'status', 'datekey']], on='employeeid', how='inner')
    merged_df['startdate'] = pd.to_datetime(merged_df['datekey'], format='%Y%m%d', errors='coerce')
    merged_df = merged_df.dropna(subset=['startdate'])
    merged_df['enddate'] = None
    merged_df['iscurrent'] = merged_df['status'].apply(lambda x: 1 if str(x).lower() == 'active' else 0)

    print(f"Merged incoming employee records: {len(merged_df)}")

    cursor.execute(f"SELECT * FROM {TABLE_NAME} WHERE iscurrent = 1")
    existing_data = cursor.fetchall()
    existing_columns = [desc[0] for desc in cursor.description]
    existing_df = pd.DataFrame(existing_data, columns=existing_columns)

    updates = 0
    inserts = 0

    for _, new_row in merged_df.iterrows():
        employeeid = new_row['employeeid']
        original_startdate = new_row['startdate'].date()

        if not existing_df.empty:
            matched = existing_df[existing_df['employeeid'] == employeeid]
        else:
            matched = pd.DataFrame()

        if matched.empty:
            cursor.execute(f"""
                SELECT COUNT(*) FROM {TABLE_NAME}
                WHERE employeeid = %s AND startdate = %s
            """, (employeeid, original_startdate))
            exists = cursor.fetchone()[0]

            if exists == 0:
                placeholders = ", ".join(["%s"] * 8)
                columns = "employeeid, name, gender, managerid, startdate, enddate, iscurrent, status"
                insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, tuple([
                    employeeid,
                    new_row['name'],
                    new_row['gender'],
                    new_row['managerid'],
                    original_startdate,
                    None,
                    new_row['iscurrent'],
                    new_row['status']
                ]))
                inserts += 1
        else:
            current = matched.iloc[0]
            if (
                current['name'] != new_row['name'] or
                current['gender'] != new_row['gender'] or
                current['managerid'] != new_row['managerid'] or
                current['status'] != new_row['status']
            ):
                cursor.execute(f"""
                    UPDATE {TABLE_NAME}
                    SET enddate = %s, iscurrent = 0
                    WHERE employeeid = %s AND iscurrent = 1
                """, (datetime.today().strftime('%Y-%m-%d'), employeeid))

                placeholders = ", ".join(["%s"] * 8)
                columns = "employeeid, name, gender, managerid, startdate, enddate, iscurrent, status"
                insert_query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_query, (
                    new_row['employeeid'],
                    new_row['name'],
                    new_row['gender'],
                    new_row['managerid'],
                    datetime.today().strftime('%Y-%m-%d'),
                    None,
                    1,
                    new_row['status']
                ))
                updates += 1
                inserts += 1
            else:
                continue

    if updates > 0:
        log_audit_event(TABLE_NAME, 'UPDATE', updates)

    if inserts > 0:
        log_audit_event(TABLE_NAME, 'INSERT', inserts)

if __name__ == "__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    with conn.cursor() as cursor:
        initialize_scd2_table(cursor)
        apply_scd2(cursor, conn)
    conn.commit()
    conn.close()
    print("SCD2 ETL completed successfully.")
