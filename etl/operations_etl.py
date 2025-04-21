import pandas as pd
from datetime import datetime

# File paths
INPUT_PATH = "./data/Operations_Dataset_Dirty.xlsx"
OUTPUT_PROCESS = "./outputs/dim_process.csv"
OUTPUT_FACT_OPERATIONS = "./outputs/fact_operations.csv"

def clean_operations_data():
    df = pd.read_excel(INPUT_PATH)

    # Standardize string fields
    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()

    # Handle missing values
    df.dropna(subset=['ProcessName', 'DowntimeHours', 'ProcessDate'], inplace=True)

    # Parse dates
    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)
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
    print("Saving outputs to /outputs/ directory...")
    dim_process.to_csv(OUTPUT_PROCESS, index=False)
    fact_operations.to_csv(OUTPUT_FACT_OPERATIONS, index=False)

    print("Operations ETL complete.")

if __name__ == "__main__":
    clean_operations_data()

