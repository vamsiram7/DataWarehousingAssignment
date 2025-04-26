-- Create Database
CREATE DATABASE IF NOT EXISTS organizational_insights;
USE organizational_insights;
-- ====================
-- Dimension Tables
-- ====================

CREATE TABLE dim_employee (
    employeeid INT PRIMARY KEY,
    name VARCHAR(255),
    gender VARCHAR(255),
    managerid INT
);

CREATE TABLE dim_department (
    departmentid INT AUTO_INCREMENT PRIMARY KEY,
    department VARCHAR(255)
);

CREATE TABLE dim_expensetype (
    expensetypeid INT AUTO_INCREMENT PRIMARY KEY,
    expensetype VARCHAR(255)
);

CREATE TABLE dim_process (
    processid INT AUTO_INCREMENT PRIMARY KEY,
    processname VARCHAR(255)
);

CREATE TABLE dim_location (
    locationid INT AUTO_INCREMENT PRIMARY KEY,
    location VARCHAR(255)
);

-- ====================
-- Fact Tables
-- ====================

CREATE TABLE fact_hr (
    hrid INT PRIMARY KEY AUTO_INCREMENT,
    employeeid INT,
    departmentid INT,
    salary FLOAT,
    status VARCHAR(255),
    datekey INT,
    FOREIGN KEY (employeeid) REFERENCES dim_employee(employeeid),
    FOREIGN KEY (departmentid) REFERENCES dim_department(departmentid)
);

CREATE TABLE fact_finance (
    financeid INT PRIMARY KEY AUTO_INCREMENT,
    employeeid INT,
    expensetypeid INT,
    expenseamount FLOAT,
    approvedby INT,
    datekey INT,
    FOREIGN KEY (employeeid) REFERENCES dim_employee(employeeid),
    FOREIGN KEY (expensetypeid) REFERENCES dim_expensetype(expensetypeid)
);

CREATE TABLE fact_operations (
    operationsid INT PRIMARY KEY AUTO_INCREMENT,
    processid INT,
    locationid INT,
    departmentid INT,
    downtimehours FLOAT,
    datekey INT,
    FOREIGN KEY (processid) REFERENCES dim_process(processid),
    FOREIGN KEY (locationid) REFERENCES dim_location(locationid),
    FOREIGN KEY (departmentid) REFERENCES dim_department(departmentid)
);
-- ====================
-- SCD Table
-- ====================

CREATE TABLE dim_employee_scd2 (
    employeeid INT,
    name VARCHAR(255),
    gender VARCHAR(255),
    managerid INT,
    startdate DATE,
    enddate DATE,
    iscurrent TINYINT,
    status VARCHAR(255),
    PRIMARY KEY (employeeid, startdate)
);

-- Create audit_log table
CREATE TABLE IF NOT EXISTS audit_log (
    id INT AUTO_INCREMENT PRIMARY KEY,
    operation_type VARCHAR(255),
    table_name VARCHAR(255),
    row_count INT,
    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
);
