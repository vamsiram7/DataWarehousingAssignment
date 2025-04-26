USE organizational_insights;

-- Drop users if already existing (Optional clean step)
DROP USER IF EXISTS 'hr_user'@'localhost';
DROP USER IF EXISTS 'finance_user'@'localhost';
DROP USER IF EXISTS 'operations_user'@'localhost';
DROP USER IF EXISTS 'super_user'@'localhost';

-- Create HR User
CREATE USER 'hr_user'@'localhost' IDENTIFIED BY 'Hr_user@123';
GRANT SELECT ON organizational_insights.view_hr_user TO 'hr_user'@'localhost';

-- Create Finance User
CREATE USER 'finance_user'@'localhost' IDENTIFIED BY 'Finance_user@123';
GRANT SELECT ON organizational_insights.view_finance_user TO 'finance_user'@'localhost';

-- Create Operations User
CREATE USER 'operations_user'@'localhost' IDENTIFIED BY 'Operations_user@123';
GRANT SELECT ON organizational_insights.view_operations_user TO 'operations_user'@'localhost';

-- Create Super User
CREATE USER 'admin_user'@'localhost' IDENTIFIED BY 'Admin_user@123';
GRANT SELECT ON organizational_insights.view_super_user TO 'super_user'@'localhost';

-- (Optional) Apply privilege changes immediately
FLUSH PRIVILEGES;
