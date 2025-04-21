DROP VIEW IF EXISTS view_hr_user;
DROP VIEW IF EXISTS view_finance_user;
DROP VIEW IF EXISTS view_operations_user;
DROP VIEW IF EXISTS view_super_user;

-- HR User View: Access to employee info and HR metrics
CREATE VIEW view_hr_user AS
SELECT 
    f.EmployeeID,
    e.Name,
    e.Gender,
    e.DateOfJoining,
    e.ManagerID,
    f.DepartmentID,
    f.Salary,
    f.Status,
    f.DateKey
FROM fact_hr f
JOIN dim_employee e ON f.EmployeeID = e.EmployeeID;

-- Finance User View: Access to expense details only
CREATE VIEW view_finance_user AS
SELECT 
    f.EmployeeID,
    f.ExpenseTypeID,
    d.ExpenseTypeName,
    f.ExpenseAmount,
    f.DateKey
FROM fact_finance f
JOIN dim_expensetype d ON f.ExpenseTypeID = d.ExpenseTypeID;

-- Operations User View: Downtime by department and process
CREATE VIEW view_operations_user AS
SELECT 
    f.Department,
    f.ProcessID,
    d.ProcessName,
    d.Location,
    f.DowntimeHours,
    f.DateKey
FROM fact_operations f
JOIN dim_process d ON f.ProcessID = d.ProcessID;

DROP VIEW IF EXISTS view_super_user;

-- Super User View: Combine HR and Finance data only
CREATE VIEW view_super_user AS
SELECT 
    hr.EmployeeID,
    hr.Name,
    hr.Gender,
    hr.DateOfJoining,
    hr.ManagerID,
    hr.DepartmentID,
    hr.Salary,
    hr.Status,
    finance.ExpenseTypeID,
    finance.ExpenseTypeName,
    finance.ExpenseAmount,
    COALESCE(hr.DateKey, finance.DateKey) AS DateKey
FROM view_hr_user hr
LEFT JOIN view_finance_user finance
    ON hr.EmployeeID = finance.EmployeeID;
