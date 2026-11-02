-- Phase 5 reset (run as SYSTEM): empty Firco's working tables so the month can be replayed.
-- Only for this simulation: in production nobody deletes audit history.
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
DELETE FROM aml_fff.alert_audit_log;
DELETE FROM aml_fff.alert_hit;
DELETE FROM aml_fff.alert_snapshot;
DELETE FROM aml_fff.screening_input;
DELETE FROM aml_fff.sanctions_list_entry;
COMMIT;
ALTER SEQUENCE aml_fff.alert_audit_seq RESTART START WITH 100001;
PROMPT 99_reset_phase5: done
EXIT