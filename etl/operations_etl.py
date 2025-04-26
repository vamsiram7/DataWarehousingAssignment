import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_audit_event  #  Added import

def operations_etl():
    # Load database config
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

    # Read Operations dirty data
    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')

    # Basic cleaning
    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()
    df['DowntimeHours'] = pd.to_numeric(df['DowntimeHours'], errors='coerce')
    df = df[(df['ProcessName'].str.strip() != '') & (df['Department'].str.strip() != '') & (df['Location'].str.strip() != '')]
    df.dropna(subset=['ProcessName', 'Department', 'Location', 'DowntimeHours'], inplace=True)

    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            return pd.NaT

    df['ProcessDate'] = df['ProcessDate'].apply(parse_dates)
    df = df[~df['ProcessDate'].isnull()]
    df.drop_duplicates(inplace=True)

    # Create staging table
    cursor.execute("DROP TABLE IF EXISTS staging_operations")
    cursor.execute("""
        CREATE TABLE staging_operations (
            ProcessName VARCHAR(255),
            Department VARCHAR(255),
            Location VARCHAR(255),
            DowntimeHours FLOAT,
            ProcessDate DATE
        )
    """)

    for _, row in df.iterrows():
        clean_row = (
            row['ProcessName'],
            row['Department'],
            row['Location'],
            float(row['DowntimeHours']) if not pd.isna(row['DowntimeHours']) else None,
            row['ProcessDate'] if not pd.isna(row['ProcessDate']) else None
        )
        cursor.execute("""
            INSERT INTO staging_operations (ProcessName, Department, Location, DowntimeHours, ProcessDate)
            VALUES (%s, %s, %s, %s, %s)
        """, clean_row)

    conn.commit()

    # Load into dim_process
    cursor.execute("""
        INSERT IGNORE INTO dim_process (processname)
        SELECT DISTINCT ProcessName
        FROM staging_operations
    """)
    conn.commit()
    log_audit_event('dim_process', 'INSERT', cursor.rowcount)  #  Audit log

    # Load into dim_location
    cursor.execute("""
        INSERT IGNORE INTO dim_location (location)
        SELECT DISTINCT Location
        FROM staging_operations
    """)
    conn.commit()
    log_audit_event('dim_location', 'INSERT', cursor.rowcount)  # Audit log

    # Load into fact_operations
    cursor.execute("""
        INSERT INTO fact_operations (processid, locationid, departmentid, downtimehours, datekey)
        SELECT 
            p.processid,
            l.locationid,
            d.departmentid,
            s.DowntimeHours,
            DATE_FORMAT(s.ProcessDate, '%Y%m%d')
        FROM staging_operations s
        JOIN dim_process p ON s.ProcessName = p.processname
        JOIN dim_location l ON s.Location = l.location
        JOIN dim_department d ON s.Department = d.department
    """)
    conn.commit()
    log_audit_event('fact_operations', 'INSERT', cursor.rowcount)  #  Audit log

    print("Operations ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    operations_etl()
