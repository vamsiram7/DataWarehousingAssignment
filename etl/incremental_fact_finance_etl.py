import pandas as pd
import sqlite3
from etl.audit_logger import log_etl_run

DB_PATH = "sql/organizational_insights.db"
INPUT_PATH = "outputs/fact_finance.csv"
TABLE_NAME = "fact_finance"

def apply_incremental_load():
    conn = sqlite3.connect(DB_PATH)

    # Load new data
    new_data = pd.read_csv(INPUT_PATH)
    new_data['DateKey'] = new_data['DateKey'].astype(int)

    # Try reading existing table
    try:
        existing_data = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)
        existing_data['DateKey'] = existing_data['DateKey'].astype(int)
    except:
        print(f"No existing table found. Inserting all rows from scratch into {TABLE_NAME}...")
        new_data.to_sql(TABLE_NAME, conn, if_exists='replace', index=False)
        conn.close()
        return

    # Merge on identifying columns to detect new records
    merged = new_data.merge(
        existing_data,
        on=['EmployeeID', 'ExpenseTypeID', 'ExpenseAmount', 'DateKey'],
        how='left',
        indicator=True
    )

    delta = merged[merged['_merge'] == 'left_only']

    # Get original rows from new_data that are not in existing_data
    final_new_data = new_data.loc[delta.index]

    # Insert new rows
    if not final_new_data.empty:
        final_new_data.to_sql(TABLE_NAME, conn, if_exists='append', index=False)
        log_etl_run(TABLE_NAME, "incremental_load", len(final_new_data))
        print(f"Inserted {len(final_new_data)} new rows into {TABLE_NAME}.")
    else:
        print("No new data to insert.")

    conn.commit()
    conn.close()

if __name__ == "__main__":
    apply_incremental_load()
