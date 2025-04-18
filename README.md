# DataWarehousingAssignment
Organizational Insights with Role-Based Access

This is an end-to-end data warehousing project that consolidates HR, Finance, and Operations datasets. The project includes ETL pipelines, star schema design, KPI reporting, role-based access simulation, and bonus features like incremental loading, SCD Type 2, and audit logging.


## Tools & Technologies Used

- **Python** (ETL development using pandas)
- **SQLite** (data warehouse simulation)
- **Jupyter Notebooks** (KPI analysis and visualizations)
- **SQL** (views, filters, role-based access)
- **GitHub** (version control and project showcase)

---

## Project Folder Structure

organizational-insights-data-warehouse/ ├── data/ # Raw Excel files ├── outputs/ # Cleaned CSV files (facts and dimensions) ├── etl/ # ETL, SCD, and incremental scripts ├── sql/ # SQL view and load scripts ├── notebooks/ # KPI and log analysis notebooks ├── docs/ # Optional: schema diagrams or descriptions └── README.md # Project documentation

## ETL Pipelines Overview

Each business area has its own ETL script that performs data cleaning, transformation, and loading into star schema format.

| Domain     | Script                  | Fact Table        | Dimensions Used                          |
|------------|-------------------------|-------------------|------------------------------------------|
| HR         | `etl/hr_etl.py`         | `fact_hr`         | `dim_employee`, `dim_department`         |
| Finance    | `etl/finance_etl.py`    | `fact_finance`    | `dim_expensetype`                        |
| Operations | `etl/operations_etl.py` | `fact_operations` | `dim_process`                            |

All cleaned CSVs are saved in the `/outputs/` folder and loaded into the SQLite database using `sql/load_to_sqlite.py`.

## KPI Metrics Summary

Business metrics were generated using pandas and Jupyter notebooks under the `/notebooks/` directory.

| Domain     | KPIs Generated                                              |
|------------|-------------------------------------------------------------|
| HR         | Headcount, Attrition Rate, Average Salary by Gender         |
| Finance    | Monthly Expenses by Expense Type                            |
| Operations | Total Downtime by Process and Department                    |

Explore these KPIs inside:
- `notebooks/hr_kpi.ipynb`
- `notebooks/finance_kpi.ipynb`
- `notebooks/operations_kpi.ipynb`


## Role-Based Access (Simulated Using SQL Views)

This project simulates access control by creating SQL views for each user role in the SQLite database.

| Role         | SQL View Name          | Access Permissions                              |
|--------------|------------------------|-------------------------------------------------|
| HR Analyst   | `view_hr_user`         | Employee data, salary, headcount, attrition     |
| Finance User | `view_finance_user`    | Expense types, amounts, dates                   |
| Ops Manager  | `view_operations_user` | Downtime by process and department              |
| Super User   | All views/tables       | Full access to all business domains             |

Views are defined inside: `sql/role_views.sql`

## Bonus Features

| Feature             | Script/Notebook                           | Description                                         |
|---------------------|-------------------------------------------|-----------------------------------------------------|
| SCD Type 2          | `etl/scd2_employee_etl.py`                | Tracks historical changes in employee data          |
| Incremental Loading | `etl/incremental_fact_finance_etl.py`     | Loads only new finance records                      |
| Audit Logging       | `etl/audit_logger.py` + `audit_log` table | Tracks ETL actions with timestamps and record count |
| Audit Log Viewer    | `notebooks/view_audit_log.ipynb`          | Shows audit history inside a Jupyter notebook       |


## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/vamsiram7DataWarehousingAssignment.git
cd DataWarehousingAssignment
```

### 2. Install required Python packages

```bash
pip install pandas openpyxl jupyter
```

### 3. Run ETL scripts.

```bash
python etl/hr_etl.py
python etl/finance_etl.py
python etl/operations_etl.py
```

### 4. Load cleaned data into SQLite

```bash
python sql/load_to_sqlite.py
```

### 5. Create role-based SQL views

```bash
python sql/create_views.py
```

### 6. Run bonus features

```bash
python etl/scd2_employee_etl.py
python etl/incremental_fact_finance_etl.py
```

### 7. Launch KPI and audit log notebooks

```bash
jupyter notebook

Then open:
notebooks/hr_kpi.ipynb
notebooks/finance_kpi.ipynb
notebooks/operations_kpi.ipynb
notebooks/view_audit_log.ipynb
```

