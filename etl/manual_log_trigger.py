from audit_logger import log_etl_run

# Manually create the audit_log table by calling the logger
log_etl_run(table_name="manual_trigger", action="init", records_inserted=0)
print("audit_log table initialized with dummy log.")