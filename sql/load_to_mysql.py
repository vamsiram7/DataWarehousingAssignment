import pandas as pd
import os
import mysql.connector
from mysql.connector import Error
import configparser

def load_data_to_mysql():
    try:
        # Load configuration
        config = configparser.ConfigParser()
        config_path = os.path.join("sql", "db_config.ini")
        
        # Check if config file exists, otherwise use default credentials
        if os.path.exists(config_path):
            config.read(config_path)
            host = config['mysql']['host']
            user = config['mysql']['user']
            password = config['mysql']['password']
            database = config['mysql']['database']
        else:
            print("Config file not found. Using default credentials.")
            host = "localhost"
            user = "root"
            password = "your_password_here" 
            database = "organizational_insights"
        
        # Create MySQL connection
        conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        
        if conn.is_connected():
            cursor = conn.cursor()
            print(f"Connected to MySQL database: {database}")
            
            # Mapping of cleaned output CSVs to table names
            files = {
                "dim_employee": "outputs/dim_employee.csv",
                "dim_department": "outputs/dim_department.csv",
                "dim_expensetype": "outputs/dim_expensetype.csv",
                "dim_process": "outputs/dim_process.csv",
                "fact_hr": "outputs/fact_hr.csv",
                "fact_finance": "outputs/fact_finance.csv",
                "fact_operations": "outputs/fact_operations.csv"
            }
            
            # Load each CSV into the MySQL database
            for table_name, csv_path in files.items():
                df = pd.read_csv(csv_path)
                
                # Drop table if exists
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                
                # Create table schema based on DataFrame
                columns = []
                for col, dtype in zip(df.columns, df.dtypes):
                    mysql_type = "VARCHAR(255)"  # Default type
                    if "int" in str(dtype).lower():
                        mysql_type = "INT"
                    elif "float" in str(dtype).lower():
                        mysql_type = "FLOAT"
                    elif "date" in str(dtype).lower() or "time" in str(dtype).lower():
                        mysql_type = "DATETIME"
                    columns.append(f"`{col}` {mysql_type}")
                
                create_query = f"CREATE TABLE {table_name} ({', '.join(columns)})"
                cursor.execute(create_query)
                
                # Insert data
                for _, row in df.iterrows():
                    placeholders = ", ".join(["%s"] * len(row))
                    columns = ", ".join([f"`{col}`" for col in df.columns])
                    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                    cursor.execute(insert_query, tuple(row))
                
                # Commit after each table
                conn.commit()
                print(f"Loaded {table_name} into MySQL database.")
            
            print("All tables loaded into MySQL database.")
    
    except Error as e:
        print(f"Error connecting to MySQL Database: {e}")
    
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            print("MySQL connection closed.")

if __name__ == "__main__":
    load_data_to_mysql()