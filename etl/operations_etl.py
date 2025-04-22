import pandas as pd
from datetime import datetime
from audit_logger import log_etl_run

INPUT_PATH = "./data/Operations_Dataset_Dirty.xlsx"
OUTPUT_PROCESS = "./outputs/dim_process.csv"
OUTPUT_FACT_OPERATIONS = "./outputs/fact_operations.csv"

def clean_operations_data():
    df = pd.read_excel(INPUT_PATH)

    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()
    df.dropna(subset=['ProcessName', 'DowntimeHours', 'ProcessDate'], inplace=True)

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
    df.drop_duplicates(inplace=True)

    dim_process = df[['ProcessName', 'Location']].drop_duplicates().reset_index(drop=True)
    dim_process['ProcessID'] = range(1, len(dim_process) + 1)
    df = df.merge(dim_process, how='left', on=['ProcessName', 'Location'])

    fact_operations = df[['Department', 'ProcessID', 'DowntimeHours', 'ProcessDate']].copy()
    fact_operations['DateKey'] = fact_operations['ProcessDate'].dt.strftime('%Y%m%d').astype(int)
    fact_operations.drop(columns='ProcessDate', inplace=True)

    dim_process.to_csv(OUTPUT_PROCESS, index=False)
    fact_operations.to_csv(OUTPUT_FACT_OPERATIONS, index=False)

    log_etl_run("dim_process", "clean_transform", len(dim_process))
    log_etl_run("fact_operations", "clean_transform", len(fact_operations))

if __name__ == "__main__":
    clean_operations_data()
    print("Operations ETL complete.")
