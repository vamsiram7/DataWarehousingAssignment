import pandas as pd
import mysql.connector
import configparser
from datetime import datetime

def initialize_scd2_table(cursor):
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_employee_scd2 (
            employeeid INT,
            name VARCHAR(255),
            gender VARCHAR(255),
            managerid INT,
            startdate DATE,
            enddate DATE,
            iscurrent TINYINT,
            status VARCHAR(255),
            PRIMARY KEY (employeeid, startdate)
        )
    """)

def apply_scd2(cursor, conn):
    print("Applying SCD2 logic...")

    # Load dim_employee and fact_hr tables
    cursor.execute("SELECT employeeid, name, gender, managerid FROM dim_employee")
    dim_employee = cursor.fetchall()
    dim_employee_columns = [desc[0] for desc in cursor.description]
    df_dim_employee = pd.DataFrame(dim_employee, columns=dim_employee_columns)

    cursor.execute("SELECT employeeid, status FROM fact_hr")
    fact_hr = cursor.fetchall()
    fact_hr_columns = [desc[0] for desc in cursor.description]
    df_fact_hr = pd.DataFrame(fact_hr, columns=fact_hr_columns)

    # Merge dim_employee and fact_hr to get full latest info
    merged_df = pd.merge(df_dim_employee, df_fact_hr, on='employeeid', how='left')

    today = datetime.today().date()

    # Create temp table for easier processing
    cursor.execute("DROP TABLE IF EXISTS temp_dim_employee")
    cursor.execute("""
        CREATE TABLE temp_dim_employee (
            employeeid INT,
            name VARCHAR(255),
            gender VARCHAR(255),
            managerid INT,
            status VARCHAR(255)
        )
    """)

    for _, row in merged_df.iterrows():
        cursor.execute("""
            INSERT INTO temp_dim_employee (employeeid, name, gender, managerid, status)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            int(row['employeeid']),
            row['name'],
            row['gender'],
            int(row['managerid']) if pd.notna(row['managerid']) else None,
            row['status']
        ))
    conn.commit()

    # Process each employee for SCD2 handling
    cursor.execute("SELECT DISTINCT employeeid FROM temp_dim_employee")
    employee_ids = cursor.fetchall()

    for (emp_id,) in employee_ids:
        # Get latest record from temp table
        cursor.execute("""
            SELECT name, gender, managerid, status
            FROM temp_dim_employee
            WHERE employeeid = %s
        """, (emp_id,))
        latest = cursor.fetchone()

        # Check if employee already has current record in dim_employee_scd2
        cursor.execute("""
            SELECT name, gender, managerid, status, startdate
            FROM dim_employee_scd2
            WHERE employeeid = %s AND iscurrent = 1
        """, (emp_id,))
        current_record = cursor.fetchone()

        if current_record is None:
            # New employee: Insert directly
            cursor.execute("""
                INSERT INTO dim_employee_scd2 (employeeid, name, gender, managerid, startdate, enddate, iscurrent, status)
                VALUES (%s, %s, %s, %s, %s, NULL, 1, %s)
            """, (
                emp_id,
                latest[0],  # name
                latest[1],  # gender
                latest[2],  # managerid
                today,
                latest[3]   # status
            ))
            conn.commit()

        else:
            # Compare fields
            if (current_record[0] != latest[0]) or (current_record[1] != latest[1]) or (current_record[2] != latest[2]) or (current_record[3] != latest[3]):
                # If any change, close old record and insert new
                old_startdate = current_record[4]

                # Close old record
                cursor.execute("""
                    UPDATE dim_employee_scd2
                    SET enddate = %s, iscurrent = 0
                    WHERE employeeid = %s AND startdate = %s
                """, (
                    today,
                    emp_id,
                    old_startdate
                ))

                # Insert new current record
                cursor.execute("""
                    INSERT INTO dim_employee_scd2 (employeeid, name, gender, managerid, startdate, enddate, iscurrent, status)
                    VALUES (%s, %s, %s, %s, %s, NULL, 1, %s)
                """, (
                    emp_id,
                    latest[0],  # name
                    latest[1],  # gender
                    latest[2],  # managerid
                    today,
                    latest[3]   # status
                ))
                conn.commit()
            else:
                # No change detected: Do nothing
                pass

    print("SCD2 application completed.")

if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

    initialize_scd2_table(cursor)
    apply_scd2(cursor, conn)

    cursor.close()
    conn.close()
