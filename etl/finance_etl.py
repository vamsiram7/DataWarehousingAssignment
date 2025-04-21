import pandas as pd
from datetime import datetime

# File paths
INPUT_PATH = "./data/Finance_Dataset_Dirty.xlsx"
OUTPUT_EXPENSETYPE = "./outputs/dim_expensetype.csv"
OUTPUT_FACT_FINANCE = "./outputs/fact_finance.csv"

def clean_finance_data():
    df = pd.read_excel(INPUT_PATH)

    # Clean text columns
    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ApprovedBy'] = df['ApprovedBy'].astype(str).str.strip().str.title()

    # Handle missing values
    df['ExpenseAmount'] = df['ExpenseAmount'].fillna(df['ExpenseAmount'].median())
    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseDate'], inplace=True)

    # Parse dates
    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            try:
                return pd.to_datetime(date_str, dayfirst=True)
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

if __name__ == "__main__":
    clean_finance_data()
