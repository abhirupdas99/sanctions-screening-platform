-- Phase 1: schema owner, roles and application users. Safe to re-run.
WHENEVER SQLERROR EXIT FAILURE
SET VERIFY OFF FEEDBACK ON SQLBLANKLINES ON
-- 1. Schema owner: owns Firco's tables; nobody can log in as it
CREATE USER IF NOT EXISTS aml_fff NO AUTHENTICATION;
-- quota 2gb for user
ALTER USER aml_fff QUOTA 2G ON users;
-- 2. Roles (CREATE ROLE has no IF NOT EXISTS, so ignore "already exists", ORA-01921)
BEGIN
  EXECUTE IMMEDIATE 'CREATE ROLE firco_app_role';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1921 THEN RAISE; END IF;
END;
/
BEGIN
  EXECUTE IMMEDIATE 'CREATE ROLE lh_extract_role';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -1921 THEN RAISE; END IF;
END;
/
-- 3. Application users. ALTER keeps the password in sync with .env on re-runs.
CREATE USER IF NOT EXISTS firco_app IDENTIFIED BY "&1";
ALTER USER firco_app IDENTIFIED BY "&1";
GRANT CREATE SESSION, firco_app_role TO firco_app;
CREATE USER IF NOT EXISTS lh_extract IDENTIFIED BY "&2";
ALTER USER lh_extract IDENTIFIED BY "&2";
GRANT CREATE SESSION, lh_extract_role TO lh_extract;
PROMPT 01_roles_users: done
EXIT