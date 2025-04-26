import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_audit_event  #  Added import

def finance_etl():
    # Load database config
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

    # Read Finance dirty data
    df = pd.read_excel('./data/Finance_Dataset_Dirty.xlsx')

    # Basic cleaning
    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ExpenseAmount'] = pd.to_numeric(df['ExpenseAmount'], errors='coerce')
    df['ApprovedBy'] = pd.to_numeric(df['ApprovedBy'], errors='coerce')

    df = df[df['ExpenseType'].str.strip() != '']
    typo_corrections = {'TRAVELL': 'TRAVEL'}
    df['ExpenseType'] = df['ExpenseType'].replace(typo_corrections)

    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseAmount'], inplace=True)

    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            return pd.NaT

    df['ExpenseDate'] = df['ExpenseDate'].apply(parse_dates)
    df = df[~df['ExpenseDate'].isnull()]
    df.drop_duplicates(inplace=True)

    # Create staging table
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
        clean_row = (
            int(row['EmployeeID']) if not pd.isna(row['EmployeeID']) else None,
            row['ExpenseType'],
            float(row['ExpenseAmount']) if not pd.isna(row['ExpenseAmount']) else None,
            int(row['ApprovedBy']) if not pd.isna(row['ApprovedBy']) else None,
            row['ExpenseDate'] if not pd.isna(row['ExpenseDate']) else None
        )
        cursor.execute("""
            INSERT INTO staging_finance (EmployeeID, ExpenseType, ExpenseAmount, ApprovedBy, ExpenseDate)
            VALUES (%s, %s, %s, %s, %s)
        """, clean_row)

    conn.commit()

    # Load into dim_expensetype
    cursor.execute("""
        INSERT IGNORE INTO dim_expensetype (expensetype)
        SELECT DISTINCT ExpenseType
        FROM staging_finance
    """)
    conn.commit()
    log_audit_event('dim_expensetype', 'INSERT', cursor.rowcount)  #  Audit log

    # Load into fact_finance
    cursor.execute("""
        INSERT INTO fact_finance (employeeid, expensetypeid, expenseamount, approvedby, datekey)
        SELECT 
            s.EmployeeID,
            e.expensetypeid,
            s.ExpenseAmount,
            s.ApprovedBy,
            DATE_FORMAT(s.ExpenseDate, '%Y%m%d')
        FROM staging_finance s
        JOIN dim_expensetype e ON s.ExpenseType = e.expensetype
        JOIN dim_employee de ON s.EmployeeID = de.employeeid
    """)
    conn.commit()
    log_audit_event('fact_finance', 'INSERT', cursor.rowcount)  #  Audit log

    print("Finance ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    finance_etl()
