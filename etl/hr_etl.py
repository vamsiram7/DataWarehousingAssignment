import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_audit_event
from apply_scd2 import initialize_scd2_table, apply_scd2

def hr_etl():
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

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

    # SCD2 Handling here
    initialize_scd2_table(cursor)
    apply_scd2(cursor, conn)

    print("HR ETL completed successfully including SCD2 update.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    hr_etl()
