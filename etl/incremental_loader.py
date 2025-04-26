import pandas as pd
import mysql.connector
import configparser

# Load DB config
config = configparser.ConfigParser()
config.read("sql/db_config.ini")

DB_CONFIG = {
    "host": config['mysql']['host'],
    "user": config['mysql']['user'],
    "password": config['mysql']['password'],
    "database": config['mysql']['database']
}

def incremental_insert(staging_table, target_table, natural_keys, insert_columns, foreign_key_check=None):
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()

    # Read data
    staging = pd.read_sql(f"SELECT * FROM {staging_table}", conn)
    target = pd.read_sql(f"SELECT * FROM {target_table}", conn)

    staging.columns = staging.columns.str.lower()
    target.columns = target.columns.str.lower()

    print(f"Loading: {staging_table} -> {target_table}")

    # --- NEW foreign key validation here ---
    if foreign_key_check:
        foreign_table, foreign_column = foreign_key_check
        foreign_df = pd.read_sql(f"SELECT {foreign_column} FROM {foreign_table}", conn)
        foreign_list = foreign_df[foreign_column.lower()].dropna().unique().tolist()
        
        # Keep only rows where employeeid exists in dim_employee
        staging = staging[staging[foreign_column.lower()].isin(foreign_list)]

    if target.empty:
        new_data = staging
    else:
        merged = staging.merge(target, on=natural_keys, how='left', indicator=True)
        new_data = merged[merged['_merge'] == 'left_only']

    if not new_data.empty:
        for _, row in new_data.iterrows():
            placeholders = ", ".join(["%s"] * len(insert_columns))
            columns = ", ".join(insert_columns)
            sql = f"INSERT INTO {target_table} ({columns}) VALUES ({placeholders})"

            values = [row.get(col.lower(), None) for col in insert_columns]
            cursor.execute(sql, values)

        conn.commit()
        print(f"Inserted {len(new_data)} new rows into '{target_table}'.")
    else:
        print(f"No new data to insert into '{target_table}'.")

    cursor.close()
    conn.close()

def main():
    incremental_insert(
        staging_table="staging_finance",
        target_table="fact_finance",
        natural_keys=["employeeid", "expensetypeid", "expenseamount", "datekey"],
        insert_columns=["employeeid", "expensetypeid", "expenseamount", "approvedby", "datekey"],
        foreign_key_check=("dim_employee", "employeeid")   # <------ NEW foreign key check
    )

    incremental_insert(
        staging_table="staging_hr",
        target_table="fact_hr",
        natural_keys=["employeeid", "departmentid", "salary", "datekey"],
        insert_columns=["employeeid", "departmentid", "salary", "status", "datekey"],
        foreign_key_check=("dim_employee", "employeeid")    # <------ HR also checks employeeid
    )

    incremental_insert(
        staging_table="staging_operations",
        target_table="fact_operations",
        natural_keys=["processid", "locationid", "departmentid", "downtimehours", "datekey"],
        insert_columns=["processid", "locationid", "departmentid", "downtimehours", "datekey"]
        # No foreign key check needed for operations for employee
    )

if __name__ == "__main__":
    main()
