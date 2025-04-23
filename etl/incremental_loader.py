import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_etl_run

# Load DB config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def apply_incremental_load(input_path, table_name, key_columns):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    new_data = pd.read_csv(input_path)

    try:
        cursor.execute(f"SELECT * FROM {table_name}")
        existing_data = pd.DataFrame(cursor.fetchall(), columns=[desc[0] for desc in cursor.description])
    except:
        for _, row in new_data.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join(new_data.columns)
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        log_etl_run(table_name, "initial_load", len(new_data))
        conn.close()
        return

    for col in key_columns:
        try:
            new_data[col] = new_data[col].astype(existing_data[col].dtype)
        except:
            continue

    merged = new_data.merge(
        existing_data,
        on=key_columns,
        how='left',
        indicator=True
    )

    delta = merged[merged['_merge'] == 'left_only']
    final_new_data = new_data.loc[delta.index]

    if not final_new_data.empty:
        print(f"New rows for {table_name}:")
        print(final_new_data)

        for _, row in final_new_data.iterrows():
            placeholders = ", ".join(["%s"] * len(row))
            columns = ", ".join(final_new_data.columns)
            insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
            cursor.execute(insert_query, tuple(row))
        conn.commit()
        log_etl_run(table_name, "incremental_load", len(final_new_data))
    else:
        print(f"No new data to insert into {table_name}.")
        log_etl_run(table_name, "incremental_load", 0)

    conn.close()

if __name__ == "__main__":
    apply_incremental_load("outputs/fact_finance.csv", "fact_finance", ["EmployeeID", "ExpenseTypeID", "ExpenseAmount", "DateKey"])
    apply_incremental_load("outputs/fact_hr.csv", "fact_hr", ["EmployeeID", "DateKey", "Salary", "Status"])
    apply_incremental_load("outputs/fact_operations.csv", "fact_operations", ["Department", "ProcessID", "DowntimeHours", "DateKey"])
    apply_incremental_load("outputs/dim_employee.csv", "dim_employee", ["EmployeeID", "Name", "Gender", "DateOfJoining", "ManagerID"])
    apply_incremental_load("outputs/dim_expensetype.csv", "dim_expensetype", ["ExpenseTypeID", "ExpenseTypeName"])
    apply_incremental_load("outputs/dim_process.csv", "dim_process", ["ProcessID", "ProcessName"])
    apply_incremental_load("outputs/dim_department.csv", "dim_department", ["DepartmentID", "DepartmentName"])
