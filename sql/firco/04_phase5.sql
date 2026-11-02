-- Phase 5: screening_input keyed by person, alert hits, analyst users and profiles, grants. Safe to re-run.
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF SERVEROUTPUT ON
-- 1. screening_input: one row per PERSON per batch (subject_key), plus date of birth for matching.
--    The Phase 1 version is keyed by source_record_id, which isn't unique (Phase 4 found 100888 twice).
--    It holds no data before Phase 5, so it is rebuilt once; later runs see subject_key and leave it alone.
DECLARE
  n NUMBER;
BEGIN
  SELECT COUNT(*) INTO n FROM all_tab_columns
   WHERE owner = 'AML_FFF' AND table_name = 'SCREENING_INPUT' AND column_name = 'SUBJECT_KEY';
  IF n = 0 THEN
    EXECUTE IMMEDIATE 'DROP TABLE aml_fff.screening_input PURGE';
    EXECUTE IMMEDIATE q'[
      CREATE TABLE aml_fff.screening_input (
        batch_id          VARCHAR2(40)  NOT NULL,
        subject_key       VARCHAR2(16)  NOT NULL,
        source_record_id  VARCHAR2(30)  NOT NULL,
        subject_name      VARCHAR2(200) NOT NULL,
        country_code      VARCHAR2(3),
        date_of_birth     VARCHAR2(10),
        change_type       VARCHAR2(15)  NOT NULL,
        loaded_at         TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
        CONSTRAINT pk_screening_input PRIMARY KEY (batch_id, subject_key),
        CONSTRAINT ck_screening_input_change CHECK (change_type IN ('NEW', 'UPDATES')))]';
    DBMS_OUTPUT.PUT_LINE('screening_input rebuilt with subject_key');
  END IF;
END;
/
-- 1b. The real list writes dates of birth as free text ('1958 or 1959', several dates): widen the column.
ALTER TABLE aml_fff.sanctions_list_entry MODIFY (date_of_birth VARCHAR2(200));

-- 2. alert_hit: why each alert was raised (best list entry, score, trigger)
CREATE TABLE IF NOT EXISTS aml_fff.alert_hit (
    alert_id        VARCHAR2(30)   NOT NULL,
    subject_key     VARCHAR2(16)   NOT NULL,
    batch_id        VARCHAR2(40)   NOT NULL,
    trigger_type    VARCHAR2(15)   NOT NULL,
    list_version    VARCHAR2(40)   NOT NULL,
    list_entry_id   VARCHAR2(40)   NOT NULL,
    list_name       VARCHAR2(1000) NOT NULL,
    list_country    VARCHAR2(1000),
    list_dob        VARCHAR2(200),
    match_score     NUMBER(5,1)    NOT NULL,
    hit_count       NUMBER         NOT NULL,
    created_at      TIMESTAMP      NOT NULL,
    CONSTRAINT pk_alert_hit PRIMARY KEY (alert_id),
    CONSTRAINT ck_alert_hit_trigger CHECK (trigger_type IN ('BATCH', 'LIST_RESCREEN'))
);
-- The list's names and regime descriptions are long free text: widen an existing alert_hit too.
ALTER TABLE aml_fff.alert_hit MODIFY (list_name VARCHAR2(1000), list_country VARCHAR2(1000), list_dob VARCHAR2(200));

CREATE INDEX IF NOT EXISTS aml_fff.ix_alert_hit_subject ON aml_fff.alert_hit (subject_key, list_entry_id);
-- 3. Analysts and their access (the Task 5 tables). Planted for the access review:
--    linda.gauthier is inactive but still mapped; samuel.okafor holds Level 2 AND System Administrator
--    (segregation-of-duties conflict); kevin.brooks has no profile; noah.patel's profile 99 has no name;
--    profile 20 was renamed, so it has two names.
MERGE INTO aml_fff.fff_users t
USING (SELECT 1 id, 'olivia.chen' username, 'olivia.chen@bank.test' email, 'Y' active, DATE '2025-03-01' created_at FROM dual
       UNION ALL SELECT 2, 'marc.tremblay', 'marc.tremblay@bank.test', 'Y', DATE '2025-04-15' FROM dual
       UNION ALL SELECT 3, 'priya.nair', 'priya.nair@bank.test', 'Y', DATE '2024-11-20' FROM dual
       UNION ALL SELECT 4, 'samuel.okafor', 'samuel.okafor@bank.test', 'Y', DATE '2024-06-10' FROM dual
       UNION ALL SELECT 5, 'linda.gauthier', 'linda.gauthier@bank.test', 'N', DATE '2023-09-05' FROM dual
       UNION ALL SELECT 6, 'kevin.brooks', 'kevin.brooks@bank.test', 'Y', DATE '2026-09-28' FROM dual
       UNION ALL SELECT 7, 'noah.patel', 'noah.patel@bank.test', 'Y', DATE '2026-01-12' FROM dual) s
ON (t.id = s.id)
WHEN NOT MATCHED THEN INSERT (id, username, email, active, created_at)
VALUES (s.id, s.username, s.email, s.active, s.created_at);
MERGE INTO aml_fff.fof_audit_profile t
USING (SELECT 1001 audit_profile_id, 10 profile_id, 'Level 1 Analyst' name, DATE '2024-01-01' created_at FROM dual
       UNION ALL SELECT 1002, 20, 'Alert Reviewer', DATE '2024-01-01' FROM dual
       UNION ALL SELECT 1003, 30, 'System Administrator', DATE '2024-01-01' FROM dual
       UNION ALL SELECT 1004, 20, 'Level 2 Investigator', DATE '2026-06-01' FROM dual) s
ON (t.audit_profile_id = s.audit_profile_id)
WHEN NOT MATCHED THEN INSERT (audit_profile_id, profile_id, name, created_at)
VALUES (s.audit_profile_id, s.profile_id, s.name, s.created_at);
MERGE INTO aml_fff.fff_user_profile t
USING (SELECT 1 user_id, 10 profile_id FROM dual
       UNION ALL SELECT 2, 10 FROM dual
       UNION ALL SELECT 3, 20 FROM dual
       UNION ALL SELECT 4, 20 FROM dual
       UNION ALL SELECT 4, 30 FROM dual
       UNION ALL SELECT 5, 10 FROM dual
       UNION ALL SELECT 7, 99 FROM dual) s
ON (t.user_id = s.user_id AND t.profile_id = s.profile_id)
WHEN NOT MATCHED THEN INSERT (user_id, profile_id) VALUES (s.user_id, s.profile_id);
COMMIT;
-- 4. Grants for the new and rebuilt tables (a dropped table loses its grants)
GRANT SELECT, INSERT, UPDATE ON aml_fff.screening_input TO firco_app_role;
GRANT SELECT, INSERT         ON aml_fff.alert_hit       TO firco_app_role;
GRANT SELECT                 ON aml_fff.alert_hit       TO lh_extract_role;
PROMPT 04_phase5: done
EXIT