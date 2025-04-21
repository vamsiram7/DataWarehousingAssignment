import pandas as pd
import os
from datetime import datetime
import mysql.connector

# File paths
INPUT_PATH = "./data/Finance_Dataset_Dirty.xlsx"
OUTPUT_EXPENSETYPE = "./outputs/dim_expensetype.csv"
OUTPUT_FACT_FINANCE = "./outputs/fact_finance.csv"

# MySQL connection details
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Adavi@14",
    "database": "organizational_insights"
}

def clean_finance_data():
    df = pd.read_excel(INPUT_PATH)

    # Clean text columns
    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ApprovedBy'] = df['ApprovedBy'].astype(str).str.strip().str.title()

    # Handle missing values
    df['ExpenseAmount'] = df['ExpenseAmount'].fillna(df['ExpenseAmount'].median())
    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseDate'], inplace=True)

    # Change the Date Format
    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)  # First try standard ISO
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)  # Then try dayfirst
            except:
                return pd.NaT

    df['ExpenseDate'] = df['ExpenseDate'].apply(parse_dates)
    df = df[~df['ExpenseDate'].isnull()]

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Create Dimension Table Dim_ExpenseType
    dim_expensetype = pd.DataFrame(df['ExpenseType'].unique(), columns=['ExpenseTypeName'])
    dim_expensetype['ExpenseTypeID'] = range(1, len(dim_expensetype) + 1)

    # Merge to add ExpenseTypeID
    df = df.merge(dim_expensetype, how='left', left_on='ExpenseType', right_on='ExpenseTypeName')

    # Create Fact Table Fact_Finance
    fact_finance = df[['EmployeeID', 'ExpenseTypeID', 'ExpenseAmount', 'ExpenseDate', 'ApprovedBy']].copy()
    fact_finance['DateKey'] = fact_finance['ExpenseDate'].dt.strftime('%Y%m%d').astype(int)
    fact_finance.drop(columns='ExpenseDate', inplace=True)

    # Export outputs
    print("Saving cleaned outputs to /outputs/ directory...")
    dim_expensetype.to_csv(OUTPUT_EXPENSETYPE, index=False)
    fact_finance.to_csv(OUTPUT_FACT_FINANCE, index=False)

    print("Finance ETL complete.")

def load_to_mysql(dim_expensetype, fact_finance):
    """Load the cleaned data into MySQL tables."""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Load Dim_ExpenseType
    cursor.execute("TRUNCATE TABLE dim_expensetype")
    for _, row in dim_expensetype.iterrows():
        cursor.execute(
            "INSERT INTO dim_expensetype (ExpenseTypeName, ExpenseTypeID) VALUES (%s, %s)",
            (row['ExpenseTypeName'], row['ExpenseTypeID'])
        )

    # Load Fact_Finance
    cursor.execute("TRUNCATE TABLE fact_finance")
    for _, row in fact_finance.iterrows():
        cursor.execute(
            "INSERT INTO fact_finance (EmployeeID, ExpenseTypeID, ExpenseAmount, ApprovedBy, DateKey) VALUES (%s, %s, %s, %s, %s)",
            (row['EmployeeID'], row['ExpenseTypeID'], row['ExpenseAmount'], row['ApprovedBy'], row['DateKey'])
        )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    clean_finance_data()

    # Load cleaned data into MySQL
    dim_expensetype = pd.read_csv(OUTPUT_EXPENSETYPE)
    fact_finance = pd.read_csv(OUTPUT_FACT_FINANCE)

    print("Loading data into MySQL...")
    load_to_mysql(dim_expensetype, fact_finance)
    print("Data successfully loaded into MySQL.")