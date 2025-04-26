import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_audit_event

def finance_etl():
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

    df = pd.read_excel('./data/Finance_Dataset_Dirty.xlsx')

    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ExpenseAmount'] = pd.to_numeric(df['ExpenseAmount'], errors='coerce')
    df['ApprovedBy'] = pd.to_numeric(df['ApprovedBy'], errors='coerce')

    df = df[df['ExpenseType'].str.strip() != '']
    typo_corrections = {'TRAVELL': 'TRAVEL'}
    df['ExpenseType'] = df['ExpenseType'].replace(typo_corrections)

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

    print("Finance ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    finance_etl()
