import pandas as pd
import os
from datetime import datetime
import mysql.connector

# File paths
INPUT_PATH = "./data/Operations_Dataset_Dirty.xlsx"
OUTPUT_PROCESS = "./outputs/dim_process.csv"
OUTPUT_FACT_OPERATIONS = "./outputs/fact_operations.csv"

# MySQL connection details
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "Adavi@14",
    "database": "organizational_insights"
}

def clean_operations_data():
    df = pd.read_excel(INPUT_PATH)

    # Standardize string fields
    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()

    # Handle missing values
    df.dropna(subset=['ProcessName', 'DowntimeHours', 'ProcessDate'], inplace=True)

    # Change the Date Format
    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)  # First try standard ISO
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)  # Then try dayfirst
            except:
                return pd.NaT

    df['ProcessDate'] = df['ProcessDate'].apply(parse_dates)
    df = df[~df['ProcessDate'].isnull()]

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Create Dimension Table Dim_Process
    dim_process = df[['ProcessName', 'Location']].drop_duplicates().reset_index(drop=True)
    dim_process['ProcessID'] = range(1, len(dim_process) + 1)

    # Merge ProcessID
    df = df.merge(dim_process, how='left', on=['ProcessName', 'Location'])

    # Create Fact Table Fact_Operations
    fact_operations = df[['Department', 'ProcessID', 'DowntimeHours', 'ProcessDate']].copy()
    fact_operations['DateKey'] = fact_operations['ProcessDate'].dt.strftime('%Y%m%d').astype(int)
    fact_operations.drop(columns='ProcessDate', inplace=True)

    # Export
    print("Saving outputs to /outputs/...")
    dim_process.to_csv(OUTPUT_PROCESS, index=False)
    fact_operations.to_csv(OUTPUT_FACT_OPERATIONS, index=False)

    print("Operations ETL complete.")

def load_to_mysql(dim_process, fact_operations):
    """Load the cleaned data into MySQL tables."""
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()

    # Load Dim_Process
    cursor.execute("TRUNCATE TABLE dim_process")
    for _, row in dim_process.iterrows():
        cursor.execute(
            "INSERT INTO dim_process (ProcessName, Location, ProcessID) VALUES (%s, %s, %s)",
            (row['ProcessName'], row['Location'], row['ProcessID'])
        )

    # Load Fact_Operations
    cursor.execute("TRUNCATE TABLE fact_operations")
    for _, row in fact_operations.iterrows():
        cursor.execute(
            "INSERT INTO fact_operations (Department, ProcessID, DowntimeHours, DateKey) VALUES (%s, %s, %s, %s)",
            (row['Department'], row['ProcessID'], row['DowntimeHours'], row['DateKey'])
        )

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    clean_operations_data()

    # Load cleaned data into MySQL
    dim_process = pd.read_csv(OUTPUT_PROCESS)
    fact_operations = pd.read_csv(OUTPUT_FACT_OPERATIONS)

    print("Loading data into MySQL...")
    load_to_mysql(dim_process, fact_operations)
    print("Data successfully loaded into MySQL.")
