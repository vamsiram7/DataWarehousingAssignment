import pandas as pd
import os
from datetime import datetime

# File paths
INPUT_PATH = "./data/Finance_Dataset_Dirty.xlsx"
OUTPUT_EXPENSETYPE = "./outputs/dim_expensetype.csv"
OUTPUT_FACT_FINANCE = "./outputs/fact_finance.csv"

def clean_finance_data():
    print("Reading raw Finance data...")
    df = pd.read_excel(INPUT_PATH)

    # Clean text columns
    print("Converting text columns to consistent format...")
    df['ExpenseType'] = df['ExpenseType'].astype(str).str.strip().str.upper()
    df['ApprovedBy'] = df['ApprovedBy'].astype(str).str.strip().str.title()

    # Handle missing values
    print("Handling missing values...")
    df['ExpenseAmount'] = df['ExpenseAmount'].fillna(df['ExpenseAmount'].median())
    df.dropna(subset=['EmployeeID', 'ExpenseType', 'ExpenseDate'], inplace=True)

    # Convert dates
    print("Formatting date columns...")
    df['ExpenseDate'] = pd.to_datetime(df['ExpenseDate'], errors='coerce')
    df = df[~df['ExpenseDate'].isnull()]

    # Remove duplicates
    df.drop_duplicates(inplace=True)

    # Create Dim_ExpenseType
    print("Creating Dim_ExpenseType table...")
    dim_expensetype = pd.DataFrame(df['ExpenseType'].unique(), columns=['ExpenseTypeName'])
    dim_expensetype['ExpenseTypeID'] = range(1, len(dim_expensetype) + 1)

    # Merge to add ExpenseTypeID
    df = df.merge(dim_expensetype, how='left', left_on='ExpenseType', right_on='ExpenseTypeName')

    # Create Fact_Finance table
    print("Creating Fact_Finance table...")
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
