-- Create Views for Role-Based Access

USE organizational_insights;

-- Drop existing views if any
DROP VIEW IF EXISTS view_hr_user;
DROP VIEW IF EXISTS view_finance_user;
DROP VIEW IF EXISTS view_operations_user;
DROP VIEW IF EXISTS view_super_user;

-- HR View: HR data
CREATE VIEW view_hr_user AS
SELECT 
    hr.hrid,
    hr.employeeid,
    emp.name,
    emp.gender,
    emp.managerid,
    dept.department,
    hr.salary,
    hr.status,
    hr.datekey
FROM fact_hr hr
JOIN dim_employee emp ON hr.employeeid = emp.employeeid
JOIN dim_department dept ON hr.departmentid = dept.departmentid;

-- Finance View: Finance data
CREATE VIEW view_finance_user AS
SELECT 
    fin.financeid,
    fin.employeeid,
    emp.name,
    exp.expensetype,
    fin.expenseamount,
    fin.approvedby,
    fin.datekey
FROM fact_finance fin
JOIN dim_employee emp ON fin.employeeid = emp.employeeid
JOIN dim_expensetype exp ON fin.expensetypeid = exp.expensetypeid;

-- Operations View: Operations data
CREATE VIEW view_operations_user AS
SELECT 
    ops.operationsid,
    proc.processname,
    loc.location,
    dept.department,
    ops.downtimehours,
    ops.datekey
FROM fact_operations ops
JOIN dim_process proc ON ops.processid = proc.processid
JOIN dim_location loc ON ops.locationid = loc.locationid
JOIN dim_department dept ON ops.departmentid = dept.departmentid;

-- Super User View: All Data from HR, Finance, and Operations
CREATE VIEW view_super_user AS
SELECT 
    'HR' AS Source,
    hr.hrid AS ID,
    hr.employeeid,
    emp.name,
    emp.gender,
    emp.managerid,
    dept.department,
    hr.salary AS Amount,
    hr.status,
    hr.datekey
FROM fact_hr hr
JOIN dim_employee emp ON hr.employeeid = emp.employeeid
JOIN dim_department dept ON hr.departmentid = dept.departmentid

UNION ALL

SELECT 
    'FINANCE' AS Source,
    fin.financeid AS ID,
    fin.employeeid,
    emp.name,
    NULL AS gender,
    NULL AS managerid,
    NULL AS department,
    fin.expenseamount AS Amount,
    NULL AS status,
    fin.datekey
FROM fact_finance fin
JOIN dim_employee emp ON fin.employeeid = emp.employeeid
JOIN dim_expensetype exp ON fin.expensetypeid = exp.expensetypeid

UNION ALL

SELECT 
    'OPERATIONS' AS Source,
    ops.operationsid AS ID,
    NULL AS employeeid,
    NULL AS name,
    NULL AS gender,
    NULL AS managerid,
    dept.department,
    ops.downtimehours AS Amount,
    NULL AS status,
    ops.datekey
FROM fact_operations ops
JOIN dim_process proc ON ops.processid = proc.processid
JOIN dim_location loc ON ops.locationid = loc.locationid
JOIN dim_department dept ON ops.departmentid = dept.departmentid;
