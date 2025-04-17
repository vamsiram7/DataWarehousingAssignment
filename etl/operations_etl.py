import pandas as pd
import os
from datetime import datetime

# File paths
INPUT_PATH = "./data/Operations_Dataset_Dirty.xlsx"
OUTPUT_PROCESS = "./outputs/dim_process.csv"
OUTPUT_FACT_OPERATIONS = "./outputs/fact_operations.csv"

def clean_operations_data():
    print("Reading raw Operations data...")
    df = pd.read_excel(INPUT_PATH)

    # Standardize string fields
    print("Standardizing text columns...")
    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()

    # Handle missing values
    print("Handling missing values...")
    df.dropna(subset=['ProcessName', 'DowntimeHours', 'ProcessDate'], inplace=True)

    # Convert ProcessDate
    print("Formatting ProcessDate...")
    df['ProcessDate'] = pd.to_datetime(df['ProcessDate'], errors='coerce')
    df = df[~df['ProcessDate'].isnull()]

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Create Dim_Process
    print("Creating Dim_Process table...")
    dim_process = df[['ProcessName', 'Location']].drop_duplicates().reset_index(drop=True)
    dim_process['ProcessID'] = range(1, len(dim_process) + 1)

    # Merge ProcessID
    df = df.merge(dim_process, how='left', on=['ProcessName', 'Location'])

    # Create Fact_Operations
    print("Creating Fact_Operations table...")
    fact_operations = df[['Department', 'ProcessID', 'DowntimeHours', 'ProcessDate']].copy()
    fact_operations['DateKey'] = fact_operations['ProcessDate'].dt.strftime('%Y%m%d').astype(int)
    fact_operations.drop(columns='ProcessDate', inplace=True)

    # Export
    print("Saving outputs to /outputs/...")
    dim_process.to_csv(OUTPUT_PROCESS, index=False)
    fact_operations.to_csv(OUTPUT_FACT_OPERATIONS, index=False)

    print("Operations ETL complete.")

if __name__ == "__main__":
    clean_operations_data()
