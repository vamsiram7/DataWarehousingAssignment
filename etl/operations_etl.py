import pandas as pd
import mysql.connector
import configparser
from audit_logger import log_audit_event

def operations_etl():
    config = configparser.ConfigParser()
    config.read('sql/db_config.ini')
    conn = mysql.connector.connect(
        host=config['mysql']['host'],
        user=config['mysql']['user'],
        password=config['mysql']['password'],
        database=config['mysql']['database']
    )
    cursor = conn.cursor()

    df = pd.read_excel('./data/Operations_Dataset_Dirty.xlsx')

    df['ProcessName'] = df['ProcessName'].astype(str).str.strip().str.upper()
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Location'] = df['Location'].astype(str).str.strip().str.title()
    df['DowntimeHours'] = pd.to_numeric(df['DowntimeHours'], errors='coerce')

    df = df[(df['ProcessName'].str.strip() != '') & (df['Department'].str.strip() != '') & (df['Location'].str.strip() != '')]
    df.dropna(subset=['ProcessName', 'Department', 'Location', 'DowntimeHours'], inplace=True)

    def parse_dates(x):
        try:
            return pd.to_datetime(x)
        except:
            return pd.NaT

    df['ProcessDate'] = df['ProcessDate'].apply(parse_dates)
    df = df[~df['ProcessDate'].isnull()]
    df.drop_duplicates(inplace=True)

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
        cursor.execute("""
            INSERT INTO staging_operations (ProcessName, Department, Location, DowntimeHours, ProcessDate)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            row['ProcessName'],
            row['Department'],
            row['Location'],
            float(row['DowntimeHours']),
            row['ProcessDate']
        ))
    conn.commit()

    cursor.execute("""
        INSERT IGNORE INTO dim_process (processname)
        SELECT DISTINCT processname FROM staging_operations
        WHERE processname NOT IN (SELECT processname FROM dim_process)
    """)
    conn.commit()
    log_audit_event('dim_process', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT IGNORE INTO dim_location (location)
        SELECT DISTINCT location FROM staging_operations
        WHERE location NOT IN (SELECT location FROM dim_location)
    """)
    conn.commit()
    log_audit_event('dim_location', 'INCREMENTAL_INSERT', cursor.rowcount)

    cursor.execute("""
        INSERT INTO fact_operations (processid, locationid, departmentid, downtimehours, datekey)
        SELECT 
            p.processid,
            l.locationid,
            d.departmentid,
            s.downtimehours,
            DATE_FORMAT(s.processdate, '%Y%m%d')
        FROM staging_operations s
        JOIN dim_process p ON s.processname = p.processname
        JOIN dim_location l ON s.location = l.location
        JOIN dim_department d ON s.department = d.department
        LEFT JOIN fact_operations f ON f.processid = p.processid AND f.locationid = l.locationid AND f.departmentid = d.departmentid AND f.datekey = DATE_FORMAT(s.processdate, '%Y%m%d')
        WHERE f.processid IS NULL
    """)
    conn.commit()
    log_audit_event('fact_operations', 'INCREMENTAL_INSERT', cursor.rowcount)

    print("Operations ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    operations_etl()
