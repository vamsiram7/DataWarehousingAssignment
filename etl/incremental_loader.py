import pandas as pd
import mysql.connector
import configparser
import subprocess
from audit_logger import log_audit_event

# --- Load DB Config ---
config = configparser.ConfigParser()
config.read('sql/db_config.ini')

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def incremental_dim_employee(cursor, conn):
    print("Incremental loading: dim_employee...")
    df = pd.read_excel('./data/HR_Dataset_Dirty.xlsx')
    inserted = 0
    for _, row in df.iterrows():
        if pd.notna(row['EmployeeID']):
            cursor.execute("""
                INSERT IGNORE INTO dim_employee (employeeid, name, gender, managerid)
                VALUES (%s, %s, %s, %s)
            """, (
                int(row['EmployeeID']),
                row['Name'],
                row['Gender'] if pd.notna(row['Gender']) else None,
                int(row['ManagerID']) if pd.notna(row['ManagerID']) else None
            ))
            inserted += cursor.rowcount
    conn.commit()
    log_audit_event('dim_employee', 'INCREMENTAL_INSERT', inserted)
    print(f"dim_employee incremental load completed. Rows inserted: {inserted}")

def incremental_dim_department(cursor, conn):
    print("Incremental loading: dim_department...")
    df = pd.read_excel('./data/HR_Dataset_Dirty.xlsx')
    inserted = 0
    departments = df['Department'].dropna().unique()
    for department in departments:
        cursor.execute("""
            INSERT IGNORE INTO dim_department (department)
            VALUES (%s)
        """, (department.strip().upper(),))
        inserted += cursor.rowcount
    conn.commit()
    log_audit_event('dim_department', 'INCREMENTAL_INSERT', inserted)
    print(f"dim_department incremental load completed. Rows inserted: {inserted}")

def incremental_fact_hr(cursor, conn):
    print("Incremental loading: fact_hr...")
    df = pd.read_excel('./data/HR_Dataset_Dirty.xlsx')
    inserted = 0
    for _, row in df.iterrows():
        if pd.notna(row['EmployeeID']) and pd.notna(row['Department']):
            cursor.execute("SELECT departmentid FROM dim_department WHERE department = %s", (row['Department'].strip().upper(),))
            dept_id = cursor.fetchone()
            if dept_id:
                cursor.fetchall()  # Clear unread results
                cursor.execute("""
                    INSERT INTO fact_hr (employeeid, departmentid, salary, status, datekey)
                    VALUES (%s, %s, %s, %s, %s)
                """, (
                    int(row['EmployeeID']),
                    dept_id[0],
                    float(row['Salary']) if pd.notna(row['Salary']) else None,
                    row['Status'],
                    pd.to_datetime(row['DateOfJoining']).strftime('%Y%m%d') if pd.notna(row['DateOfJoining']) else None
                ))
                inserted += cursor.rowcount
            else:
                cursor.fetchall()  # Clear if no match
    conn.commit()
    log_audit_event('fact_hr', 'INCREMENTAL_INSERT', inserted)
    print(f"fact_hr incremental load completed. Rows inserted: {inserted}")

def incremental_dim_expensetype(cursor, conn):
    print("Incremental loading: dim_expensetype...")
    df = pd.read_excel('./data/Finance_Dataset_Dirty.xlsx')
    inserted = 0
    expensetypes = df['ExpenseType'].dropna().unique()
    for expensetype in expensetypes:
        cursor.execute("""
            INSERT IGNORE INTO dim_expensetype (expensetype)
            VALUES (%s)
        """, (expensetype.strip().upper(),))
        inserted += cursor.rowcount
    conn.commit()
    log_audit_event('dim_expensetype', 'INCREMENTAL_INSERT', inserted)
    print(f"dim_expensetype incremental load completed. Rows inserted: {inserted}")

def incremental_fact_finance(cursor, conn):
    print("Incremental loading: fact_finance...")
    df = pd.read_excel('./data/Finance_Dataset_Dirty.xlsx')
    inserted = 0
    for _, row in df.iterrows():
        if pd.notna(row['EmployeeID']) and pd.notna(row['ExpenseType']):
            cursor.execute("SELECT expensetypeid FROM dim_expensetype WHERE expensetype = %s", (row['ExpenseType'].strip().upper(),))
            expensetypeid = cursor.fetchone()
            if expensetypeid:
                cursor.fetchall()
                cursor.execute("SELECT employeeid FROM dim_employee WHERE employeeid = %s", (row['EmployeeID'],))
                emp_check = cursor.fetchone()
                if emp_check:
                    cursor.fetchall()
                    cursor.execute("""
                        INSERT INTO fact_finance (employeeid, expensetypeid, expenseamount, approvedby, datekey)
                        VALUES (%s, %s, %s, %s, %s)
                    """, (
                        int(row['EmployeeID']),
                        expensetypeid[0],
                        float(row['ExpenseAmount']) if pd.notna(row['ExpenseAmount']) else None,
                        int(row['ApprovedBy']) if pd.notna(row['ApprovedBy']) else None,
                        pd.to_datetime(row['ExpenseDate']).strftime('%Y%m%d') if pd.notna(row['ExpenseDate']) else None
                    ))
                    inserted += cursor.rowcount
                else:
                    cursor.fetchall()
            else:
                cursor.fetchall()
    conn.commit()
    log_audit_event('fact_finance', 'INCREMENTAL_INSERT', inserted)
    print(f"fact_finance incremental load completed. Rows inserted: {inserted}")

def incremental_dim_process(cursor, conn):
    print("Incremental loading: dim_process...")
    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')
    inserted = 0
    processes = df['ProcessName'].dropna().unique()
    for process in processes:
        cursor.execute("""
            INSERT IGNORE INTO dim_process (processname)
            VALUES (%s)
        """, (process.strip().upper(),))
        inserted += cursor.rowcount
    conn.commit()
    log_audit_event('dim_process', 'INCREMENTAL_INSERT', inserted)
    print(f"dim_process incremental load completed. Rows inserted: {inserted}")

def incremental_dim_location(cursor, conn):
    print("Incremental loading: dim_location...")
    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')
    inserted = 0
    locations = df['Location'].dropna().unique()
    for location in locations:
        cursor.execute("""
            INSERT IGNORE INTO dim_location (location)
            VALUES (%s)
        """, (location.strip().title(),))
        inserted += cursor.rowcount
    conn.commit()
    log_audit_event('dim_location', 'INCREMENTAL_INSERT', inserted)
    print(f"dim_location incremental load completed. Rows inserted: {inserted}")

def incremental_fact_operations(cursor, conn):
    print("Incremental loading: fact_operations...")
    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')
    inserted = 0
    for _, row in df.iterrows():
        if pd.notna(row['ProcessName']) and pd.notna(row['Location']) and pd.notna(row['Department']):
            cursor.execute("SELECT processid FROM dim_process WHERE processname = %s", (row['ProcessName'].strip().upper(),))
            processid = cursor.fetchone()
            if processid:
                cursor.fetchall()
                cursor.execute("SELECT locationid FROM dim_location WHERE location = %s", (row['Location'].strip().title(),))
                locationid = cursor.fetchone()
                if locationid:
                    cursor.fetchall()
                    cursor.execute("SELECT departmentid FROM dim_department WHERE department = %s", (row['Department'].strip().upper(),))
                    departmentid = cursor.fetchone()
                    if departmentid:
                        cursor.fetchall()
                        cursor.execute("""
                            INSERT INTO fact_operations (processid, locationid, departmentid, downtimehours, datekey)
                            VALUES (%s, %s, %s, %s, %s)
                        """, (
                            processid[0],
                            locationid[0],
                            departmentid[0],
                            float(row['DowntimeHours']) if pd.notna(row['DowntimeHours']) else None,
                            pd.to_datetime(row['ProcessDate']).strftime('%Y%m%d') if pd.notna(row['ProcessDate']) else None
                        ))
                        inserted += cursor.rowcount
                    else:
                        cursor.fetchall()
                else:
                    cursor.fetchall()
            else:
                cursor.fetchall()
    conn.commit()
    log_audit_event('fact_operations', 'INCREMENTAL_INSERT', inserted)
    print(f"fact_operations incremental load completed. Rows inserted: {inserted}")

def run_scd2():
    print("Running SCD2 process...")
    subprocess.run(["python", "etl/scd2_employee_etl.py"])

if __name__ == "__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    incremental_dim_department(cursor, conn)
    incremental_dim_employee(cursor, conn)
    incremental_fact_hr(cursor, conn)

    incremental_dim_expensetype(cursor, conn)
    incremental_fact_finance(cursor, conn)

    incremental_dim_process(cursor, conn)
    incremental_dim_location(cursor, conn)
    incremental_fact_operations(cursor, conn)

    run_scd2()

    cursor.close()
    conn.close()
    print("All incremental loading completed successfully.")
