import pandas as pd
import mysql.connector
import configparser
import subprocess
from audit_logger import log_audit_event
from apply_scd2 import initialize_scd2_table, apply_scd2

# --- Load DB Config ---
config = configparser.ConfigParser()
config.read('sql/db_config.ini')

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def load_staging_hr(cursor, conn):
    print("Loading staging_hr...")
    df = pd.read_excel('./data/HR_Dataset_Dirty.xlsx')

    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Gender'] = df['Gender'].fillna('Unknown')
    df['Salary'] = df['Salary'].fillna(df['Salary'].median())
    df['ManagerID'] = pd.to_numeric(df['ManagerID'], errors='coerce')
    df.dropna(subset=['EmployeeID', 'Department'], inplace=True)

    def parse_dates(x):
        try:
            return pd.to_datetime(x)
        except:
            return pd.NaT

    df['DateOfJoining'] = df['DateOfJoining'].apply(parse_dates)
    df = df[~df['DateOfJoining'].isnull()]
    df.drop_duplicates(inplace=True)

    cursor.execute("DROP TABLE IF EXISTS staging_hr")
    cursor.execute("""
        CREATE TABLE staging_hr (
            EmployeeID INT,
            Name VARCHAR(255),
            Gender VARCHAR(255),
            ManagerID INT,
            Department VARCHAR(255),
            Salary FLOAT,
            Status VARCHAR(255),
            DateOfJoining DATE
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging_hr (EmployeeID, Name, Gender, ManagerID, Department, Salary, Status, DateOfJoining)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(row['EmployeeID']),
            row['Name'],
            row['Gender'],
            int(row['ManagerID']) if pd.notna(row['ManagerID']) else None,
            row['Department'],
            float(row['Salary']),
            row['Status'],
            row['DateOfJoining']
        ))
    conn.commit()

def load_staging_finance(cursor, conn):
    print("Loading staging_finance...")
    df = pd.read_excel('./data/Finance_Dataset_Dirty.xlsx')

    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ExpenseAmount'] = pd.to_numeric(df['ExpenseAmount'], errors='coerce')
    df['ApprovedBy'] = pd.to_numeric(df['ApprovedBy'], errors='coerce')

    typo_corrections = {'TRAVELL': 'TRAVEL'}
    df['ExpenseType'] = df['ExpenseType'].replace(typo_corrections)

    df = df[df['ExpenseType'].str.strip() != '']
    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseAmount'], inplace=True)

    def parse_dates(x):
        try:
            return pd.to_datetime(x)
        except:
            return pd.NaT

    df['ExpenseDate'] = df['ExpenseDate'].apply(parse_dates)
    df = df[~df['ExpenseDate'].isnull()]
    df.drop_duplicates(inplace=True)

    cursor.execute("DROP TABLE IF EXISTS staging_finance")
    cursor.execute("""
        CREATE TABLE staging_finance (
            EmployeeID INT,
            ExpenseType VARCHAR(255),
            ExpenseAmount FLOAT,
            ApprovedBy INT,
            ExpenseDate DATE
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging_finance (EmployeeID, ExpenseType, ExpenseAmount, ApprovedBy, ExpenseDate)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            int(row['EmployeeID']),
            row['ExpenseType'],
            float(row['ExpenseAmount']),
            int(row['ApprovedBy']) if pd.notna(row['ApprovedBy']) else None,
            row['ExpenseDate']
        ))
    conn.commit()

def load_staging_operations(cursor, conn):
    print("Loading staging_operations...")
    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')

    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()
    df['DowntimeHours'] = pd.to_numeric(df['DowntimeHours'], errors='coerce')

    df = df[(df['ProcessName'].str.strip() != '') & (df['Department'].str.strip() != '') & (df['Location'].str.strip() != '')]
    df.dropna(subset=['ProcessName', 'Department', 'Location', 'DowntimeHours'], inplace=True)

    def parse_dates(x):
        try:
            return pd.to_datetime(x)
        except:
            return pd.NaT

    df['ProcessDate'] = df['ProcessDate'].apply(parse_dates)
    df = df[~df['ProcessDate'].isnull()]
    df.drop_duplicates(inplace=True)

    cursor.execute("DROP TABLE IF EXISTS staging_operations")
    cursor.execute("""
        CREATE TABLE staging_operations (
            ProcessName VARCHAR(255),
            Department VARCHAR(255),
            Location VARCHAR(255),
            DowntimeHours FLOAT,
            ProcessDate DATE
        )
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO staging_operations (ProcessName, Department, Location, DowntimeHours, ProcessDate)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['ProcessName'],
            row['Department'],
            row['Location'],
            float(row['DowntimeHours']),
            row['ProcessDate']
        ))
    conn.commit()

def incremental_hr(cursor, conn):
    print("Incrementally loading HR data...")

    cursor.execute("""
        INSERT IGNORE INTO dim_department (department)
        SELECT DISTINCT department FROM staging_hr
        WHERE department NOT IN (SELECT department FROM dim_department)
    """)
    conn.commit()
    log_audit_event('dim_department', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT IGNORE INTO dim_employee (employeeid, name, gender, managerid)
        SELECT DISTINCT employeeid, name, gender, managerid
        FROM staging_hr
        WHERE employeeid NOT IN (SELECT employeeid FROM dim_employee)
    """)
    conn.commit()
    log_audit_event('dim_employee', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        UPDATE dim_employee de
        JOIN staging_hr s ON de.employeeid = s.employeeid
        SET
            de.name = s.name,
            de.gender = s.gender,
            de.managerid = s.managerid
        WHERE
            de.name <> s.name
            OR de.gender <> s.gender
            OR de.managerid <> s.managerid
    """)
    conn.commit()

    cursor.execute("""
        INSERT INTO fact_hr (employeeid, departmentid, salary, status, datekey)
        SELECT 
            s.employeeid,
            d.departmentid,
            s.salary,
            s.status,
            DATE_FORMAT(s.dateofjoining, '%Y%m%d')
        FROM staging_hr s
        JOIN dim_department d ON s.department = d.department
        LEFT JOIN fact_hr f ON f.employeeid = s.employeeid AND f.datekey = DATE_FORMAT(s.dateofjoining, '%Y%m%d')
        WHERE f.employeeid IS NULL
    """)
    conn.commit()
    log_audit_event('fact_hr', 'INCREMENTAL_INSERT', cursor.rowcount)

def incremental_finance(cursor, conn):
    print("Incrementally loading Finance data...")

    cursor.execute("""
        INSERT IGNORE INTO dim_expensetype (expensetype)
        SELECT DISTINCT expensetype FROM staging_finance
        WHERE expensetype NOT IN (SELECT expensetype FROM dim_expensetype)
    """)
    conn.commit()
    log_audit_event('dim_expensetype', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT INTO fact_finance (employeeid, expensetypeid, expenseamount, approvedby, datekey)
        SELECT 
            s.employeeid,
            e.expensetypeid,
            s.expenseamount,
            s.approvedby,
            DATE_FORMAT(s.expensedate, '%Y%m%d')
        FROM staging_finance s
        JOIN dim_expensetype e ON s.expensetype = e.expensetype
        JOIN dim_employee de ON s.employeeid = de.employeeid
        LEFT JOIN fact_finance f ON f.employeeid = s.employeeid AND f.expensetypeid = e.expensetypeid AND f.datekey = DATE_FORMAT(s.expensedate, '%Y%m%d')
        WHERE f.employeeid IS NULL
    """)
    conn.commit()
    log_audit_event('fact_finance', 'INCREMENTAL_INSERT', cursor.rowcount)

def incremental_operations(cursor, conn):
    print("Incrementally loading Operations data...")

    cursor.execute("""
        INSERT IGNORE INTO dim_process (processname)
        SELECT DISTINCT processname FROM staging_operations
        WHERE processname NOT IN (SELECT processname FROM dim_process)
    """)
    conn.commit()
    log_audit_event('dim_process', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT IGNORE INTO dim_location (location)
        SELECT DISTINCT location FROM staging_operations
        WHERE location NOT IN (SELECT location FROM dim_location)
    """)
    conn.commit()
    log_audit_event('dim_location', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT INTO fact_operations (processid, locationid, departmentid, downtimehours, datekey)
        SELECT 
            p.processid,
            l.locationid,
            d.departmentid,
            s.downtimehours,
            DATE_FORMAT(s.processdate, '%Y%m%d')
        FROM staging_operations s
        JOIN dim_process p ON s.processname = p.processname
        JOIN dim_location l ON s.location = l.location
        JOIN dim_department d ON s.department = d.department
        LEFT JOIN fact_operations f ON f.processid = p.processid AND f.locationid = l.locationid AND f.departmentid = d.departmentid AND f.datekey = DATE_FORMAT(s.processdate, '%Y%m%d')
        WHERE f.processid IS NULL
    """)
    conn.commit()
    log_audit_event('fact_operations', 'INCREMENTAL_INSERT', cursor.rowcount)

def run_scd2(cursor, conn):
    print("Running SCD2 process...")
    initialize_scd2_table(cursor)
    apply_scd2(cursor, conn)

if __name__ == "__main__":
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    load_staging_hr(cursor, conn)
    incremental_hr(cursor, conn)

    load_staging_finance(cursor, conn)
    incremental_finance(cursor, conn)

    load_staging_operations(cursor, conn)
    incremental_operations(cursor, conn)

    run_scd2(cursor, conn)

    cursor.close()
    conn.close()
    print("All ETL and SCD2 processes completed successfully.")
