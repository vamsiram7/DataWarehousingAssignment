import pandas as pd
import sqlite3
from datetime import datetime

DB_PATH = "sql/organizational_insights.db"
INPUT_PATH = "outputs/dim_employee.csv"
TABLE_NAME = "dim_employee_scd2"

def initialize_scd2_table():
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    cursor.execute(f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        EmployeeID INTEGER,
        Name TEXT,
        Gender TEXT,
        ManagerID INTEGER,
        DateOfJoining TEXT,
        StartDate TEXT,
        EndDate TEXT,
        IsCurrent INTEGER
    );
    """)
    conn.commit()
    conn.close()

def apply_scd2():
    conn = sqlite3.connect(DB_PATH)

    new_df = pd.read_csv(INPUT_PATH)
    new_df['StartDate'] = datetime.today().strftime('%Y-%m-%d')
    new_df['EndDate'] = None
    new_df['IsCurrent'] = 1

    existing_df = pd.read_sql(f"SELECT * FROM {TABLE_NAME}", conn)

    inserts = []

    for _, new_row in new_df.iterrows():
        matched = existing_df[
            (existing_df['EmployeeID'] == new_row['EmployeeID']) &
            (existing_df['IsCurrent'] == 1)
        ]

        if matched.empty:
            inserts.append(new_row)
        else:
            for _, current in matched.iterrows():
                # Check for any change
                if (
                    current['Name'] != new_row['Name'] or
                    current['Gender'] != new_row['Gender'] or
                    current['ManagerID'] != new_row['ManagerID'] or
                    current['DateOfJoining'] != new_row['DateOfJoining']
                ):
                    # Expire current
                    conn.execute(f"""
                        UPDATE {TABLE_NAME}
                        SET EndDate = ?, IsCurrent = 0
                        WHERE EmployeeID = ? AND IsCurrent = 1
                    """, (datetime.today().strftime('%Y-%m-%d'), new_row['EmployeeID']))
                    inserts.append(new_row)

    # Insert new versions
    if inserts:
        insert_df = pd.DataFrame(inserts)
        insert_df.to_sql(TABLE_NAME, conn, if_exists='append', index=False)

    conn.commit()
    conn.close()
    print("SCD Type 2 changes applied to dim_employee_scd2.")

if __name__ == "__main__":
    initialize_scd2_table()
    apply_scd2()
