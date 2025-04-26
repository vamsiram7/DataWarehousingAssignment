import pandas as pd
import mysql.connector
import configparser

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

    # 🚨 NEW CLEANING: Remove empty ExpenseType
    df = df[df['ExpenseType'].str.strip() != '']

    # 🚨 NEW CLEANING: Correct known typos
    typo_corrections = {
        'TRAVELL': 'TRAVEL'
    }
    df['ExpenseType'] = df['ExpenseType'].replace(typo_corrections)

    # Drop rows where important columns are missing
    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseAmount'], inplace=True)

    # Parse ExpenseDate
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

    # Load into fact_finance (only valid employeeids)
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
    print("Finance ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    finance_etl()
