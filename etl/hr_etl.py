import pandas as pd
import mysql.connector
import configparser

def hr_etl():
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

    # Read HR dirty data
    df = pd.read_excel('./data/HR_Dataset_Dirty.xlsx')

    # Cleaning
    df['Department'] = df['Department'].astype(str).str.strip().str.upper()
    df['Gender'] = df['Gender'].fillna('Unknown')
    df['Salary'] = df['Salary'].fillna(df['Salary'].median())
    df['ManagerID'] = pd.to_numeric(df['ManagerID'], errors='coerce')
    df.dropna(subset=['EmployeeID', 'Department'], inplace=True)

    def parse_dates(date_str):
        try:
            return pd.to_datetime(date_str)
        except:
            return pd.NaT

    df['DateOfJoining'] = df['DateOfJoining'].apply(parse_dates)
    df = df[~df['DateOfJoining'].isnull()]
    df.drop_duplicates(inplace=True)

    # Create staging table
    cursor.execute("DROP TABLE IF EXISTS staging_hr")
    cursor.execute("""
        CREATE TABLE staging_hr (
            EmployeeID INT,
            Name VARCHAR(255),
            Gender VARCHAR(255),
            ManagerID INT,
            Department VARCHAR(255),
            Salary FLOAT,
            Status VARCHAR(255),
            DateOfJoining DATE
        )
    """)

    for _, row in df.iterrows():
        clean_row = (
            int(row['EmployeeID']) if not pd.isna(row['EmployeeID']) else None,
            row['Name'],
            row['Gender'],
            int(row['ManagerID']) if not pd.isna(row['ManagerID']) else None,
            row['Department'],
            float(row['Salary']) if not pd.isna(row['Salary']) else None,
            row['Status'],
            row['DateOfJoining'] if not pd.isna(row['DateOfJoining']) else None
        )
        cursor.execute("""
            INSERT INTO staging_hr (EmployeeID, Name, Gender, ManagerID, Department, Salary, Status, DateOfJoining)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, clean_row)

    conn.commit()

    # Load into dimension tables
    cursor.execute("""
        INSERT IGNORE INTO dim_department (department)
        SELECT DISTINCT Department
        FROM staging_hr
    """)

    cursor.execute("""
        INSERT IGNORE INTO dim_employee (employeeid, name, gender, managerid)
        SELECT DISTINCT EmployeeID, Name, Gender, ManagerID
        FROM staging_hr
    """)

    # Load into fact_hr
    cursor.execute("""
        INSERT INTO fact_hr (employeeid, departmentid, salary, status, datekey)
        SELECT 
            s.EmployeeID,
            d.departmentid,
            s.Salary,
            s.Status,
            DATE_FORMAT(s.DateOfJoining, '%Y%m%d')
        FROM staging_hr s
        JOIN dim_department d ON s.Department = d.department
    """)

    conn.commit()
    print("HR ETL completed successfully.")

    cursor.close()
    conn.close()

if __name__ == "__main__":
    hr_etl()
