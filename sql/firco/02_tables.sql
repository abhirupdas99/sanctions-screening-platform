-- Phase 1: Firco-side tables in schema AML_FFF. Safe to re-run (IF NOT EXISTS, Oracle 23ai+).
WHENEVER SQLERROR EXIT FAILURE
SET FEEDBACK OFF
-- From the Task 2 schema --------------------------------------------------
CREATE TABLE IF NOT EXISTS aml_fff.alert_snapshot (
    alert_id             VARCHAR2(30)  NOT NULL,
    source_record_id     VARCHAR2(30)  NOT NULL,
    subject_name         VARCHAR2(200) NOT NULL,
    country_code         VARCHAR2(3),
    vendor_alert_ref     VARCHAR2(50),
    alert_status         VARCHAR2(20)  NOT NULL,
    snapshot_updated_at  TIMESTAMP     NOT NULL,
    CONSTRAINT pk_alert_snapshot PRIMARY KEY (alert_id),
    CONSTRAINT ck_alert_snapshot_status CHECK (alert_status IN ('OPEN', 'IN_REVIEW', 'CLOSED'))
);
CREATE TABLE IF NOT EXISTS aml_fff.alert_audit_log (
    audit_event_id  NUMBER         NOT NULL,
    alert_id        VARCHAR2(30)   NOT NULL,
    event_timestamp TIMESTAMP      NOT NULL,
    actor_type      VARCHAR2(10)   NOT NULL,
    actor_id        VARCHAR2(100)  NOT NULL,
    action_code     VARCHAR2(30)   NOT NULL,
    decision_code   VARCHAR2(30),
    comments        VARCHAR2(1000),
    CONSTRAINT pk_alert_audit_log PRIMARY KEY (audit_event_id),
    CONSTRAINT ck_alert_audit_actor_type CHECK (actor_type IN ('USER', 'WORKFLOW'))
);
CREATE INDEX IF NOT EXISTS aml_fff.ix_alert_audit_timestamp ON aml_fff.alert_audit_log (event_timestamp);
CREATE INDEX IF NOT EXISTS aml_fff.ix_alert_audit_alert_timestamp ON aml_fff.alert_audit_log (alert_id, event_timestamp);
CREATE SEQUENCE IF NOT EXISTS aml_fff.alert_audit_seq START WITH 100001 NOCACHE;
-- From the Task 5 schema --------------------------------------------------
CREATE TABLE IF NOT EXISTS aml_fff.fff_users (
    id          NUMBER        NOT NULL,
    username    VARCHAR2(50)  NOT NULL,
    email       VARCHAR2(100),
    active      CHAR(1)       NOT NULL,
    created_at  DATE          NOT NULL,
    CONSTRAINT pk_fff_users PRIMARY KEY (id),
    CONSTRAINT ck_fff_users_active CHECK (active IN ('Y', 'N'))
);
CREATE TABLE IF NOT EXISTS aml_fff.fff_user_profile (
    user_id     NUMBER NOT NULL,
    profile_id  NUMBER NOT NULL,
    CONSTRAINT pk_fff_user_profile PRIMARY KEY (user_id, profile_id),
    CONSTRAINT fk_user_profile_user FOREIGN KEY (user_id) REFERENCES aml_fff.fff_users (id)
);
CREATE TABLE IF NOT EXISTS aml_fff.fof_audit_profile (
    audit_profile_id NUMBER        NOT NULL,
    profile_id       NUMBER        NOT NULL,
    name             VARCHAR2(100) NOT NULL,
    created_at       DATE          NOT NULL,
    CONSTRAINT pk_fof_audit_profile PRIMARY KEY (audit_profile_id)
);
-- New: what Firco is asked to screen, and the list it screens against ----
CREATE TABLE IF NOT EXISTS aml_fff.screening_input (
    batch_id          VARCHAR2(40)  NOT NULL,
    source_record_id  VARCHAR2(30)  NOT NULL,
    subject_name      VARCHAR2(200) NOT NULL,
    country_code      VARCHAR2(3),
    change_type       VARCHAR2(15)  NOT NULL,
    loaded_at         TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    CONSTRAINT pk_screening_input PRIMARY KEY (batch_id, source_record_id),
    CONSTRAINT ck_screening_input_change CHECK (change_type IN ('NEW', 'UPDATES', 'LIST_RESCREEN'))
);
CREATE TABLE IF NOT EXISTS aml_fff.sanctions_list_entry (
    list_version   VARCHAR2(40)   NOT NULL,
    entry_id       VARCHAR2(40)   NOT NULL,
    entity_type    VARCHAR2(20),
    full_name      VARCHAR2(400)  NOT NULL,
    aliases        VARCHAR2(4000),
    regulation     VARCHAR2(400),
    date_of_birth  VARCHAR2(20),
    listed_on      DATE,
    CONSTRAINT pk_sanctions_list_entry PRIMARY KEY (list_version, entry_id)
);
PROMPT 02_tables: done
EXIT