-- Phase 1: object privileges to roles. Safe to re-run.
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
-- Engine + analysts' application
GRANT SELECT, INSERT, UPDATE ON aml_fff.alert_snapshot       TO firco_app_role;
GRANT SELECT, INSERT         ON aml_fff.alert_audit_log      TO firco_app_role;
GRANT SELECT                 ON aml_fff.alert_audit_seq      TO firco_app_role;
GRANT SELECT, INSERT, UPDATE ON aml_fff.fff_users            TO firco_app_role;
GRANT SELECT, INSERT, DELETE ON aml_fff.fff_user_profile     TO firco_app_role;
GRANT SELECT, INSERT         ON aml_fff.fof_audit_profile    TO firco_app_role;
GRANT SELECT, INSERT, UPDATE ON aml_fff.screening_input      TO firco_app_role;
GRANT SELECT, INSERT         ON aml_fff.sanctions_list_entry TO firco_app_role;
-- Lakehouse extract (ADF + SHIR in Tier 2): read-only, 5 reporting tables
GRANT SELECT ON aml_fff.alert_snapshot    TO lh_extract_role;
GRANT SELECT ON aml_fff.alert_audit_log   TO lh_extract_role;
GRANT SELECT ON aml_fff.fff_users         TO lh_extract_role;
GRANT SELECT ON aml_fff.fff_user_profile  TO lh_extract_role;
GRANT SELECT ON aml_fff.fof_audit_profile TO lh_extract_role;
PROMPT 03_grants: done
EXIT