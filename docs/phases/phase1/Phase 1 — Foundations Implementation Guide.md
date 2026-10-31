# Phase 1 — Foundations: Implementation Guide

Oct 31, 2026 · @Abhirup

## Goal and done criteria

Phase 1 builds the empty but correctly secured shell of the screening platform. By the end you have a Git project, a Python + Spark environment, a dedicated Oracle database holding Firco's tables, three Oracle accounts with least privilege, and two control tables. One command, `make check`, proves all of it.

No data moves yet. That starts in Phase 2.

| # | Done when | Proved by |
| --- | --- | --- |
| 1 | Project is in Git, with secrets never committed | `git status` clean; `.env` listed in `.gitignore` |
| 2 | Python, Spark and Delta work together | `make check` creates and reads a Delta table |
| 3 | Oracle data survives deleting the container | Named Docker volume; the tables still exist after `docker rm` |
| 4 | The schema owner can't log in | Login as `AML_FFF` fails |
| 5 | The extract user can read, and only read, 5 tables | `SELECT` works; `INSERT` fails with a privilege error |
| 6 | The audit log is append-only for the app | `FIRCO_APP` can `INSERT` into `alert_audit_log`, but `UPDATE` and `DELETE` fail |
| 7 | Control tables exist | `ctl.file_manifest` and `ctl.run_log` readable as Delta |

Time needed: about 2–3 hours, most of it the first Oracle start and the Python install.

**Why separate from `cibc_tasks`:** the take-home submission stays untouched. This project gets its own folder, its own Oracle container on port 1522, and its own data volume, so the two can run side by side.

## Prerequisites

You already have most of these from the take-home. Run each check in an **Ubuntu (WSL) terminal**, never PowerShell, and install only what fails.

| Tool | Version | Check | Install if missing |
| --- | --- | --- | --- |
| WSL 2 + Ubuntu | Ubuntu 22.04 or 24.04 | `lsb_release -d` | In PowerShell (admin): `wsl --install -d Ubuntu-24.04` |
| Docker, usable from WSL | Any recent | `docker run --rm hello-world` | Docker Desktop with WSL integration on, or Docker Engine inside Ubuntu |
| Disk space | 15 GB free | `df -h ~` | Oracle image about 2 GB, data volume 3–5 GB, Python about 1 GB |
| Git | 2.x | `git --version` | `sudo apt install -y git` |
| make | any | `make --version` | `sudo apt install -y make` |
| Java (for Spark) | **17** | `java -version` | `sudo apt install -y openjdk-17-jdk-headless` |
| uv (Python manager) | latest | `uv --version` | `curl -LsSf https://astral.sh/uv/install.sh \| sh`, then open a new terminal |
| Python | **3.11**, installed by uv in Step 2 | `uv python list` | Nothing to do; uv downloads it |
| VS Code + WSL extension | optional | `code .` opens the folder | Install VS Code on Windows, add the "WSL" extension |
| Internet access | to PyPI, Maven Central, Docker Hub | the installs succeed | Spark downloads the Delta jars from Maven Central on first run |

**Why these exact versions:** PySpark 3.5 runs on Java 8, 11 or 17, so 17 is the newest safe choice. Python 3.11 avoids compatibility issues with newer Python releases. Delta Lake 3.2.x is the release built for Spark 3.5. Using one fixed, known-good set is called **pinning**, and it's what makes "works on my machine" true on every machine.

**Rules for this project, learned the hard way in the take-home:**

- [ ] Work only in Ubuntu, in `~/screening-platform`. Never in `/mnt/c/...` and never in PowerShell.
- [ ] Type or paste SQL\*Plus commands one line at a time; multi-line SQL lives in files.
- [ ] Never delete and re-create the project folder while a container is attached to it.

## Step 1 — Project folder, Git and layout

Create the folder structure once, so every later phase knows where things go. Code and SQL are committed to Git; data, the lakehouse and secrets are not.

```
screening-platform/
├── Makefile                 one-word commands: make db-up, make check …
├── README.md
├── requirements.txt         pinned Python packages
├── .env.example             setting names, no real values (committed)
├── .env                     real passwords (NEVER committed)
├── .gitignore
├── sql/
│   ├── admin/               run as SYSTEM: roles, users, grants
│   └── firco/               Firco's tables in schema AML_FFF
├── src/
│   ├── common/              config.py, spark.py, oracle.py (shared helpers)
│   ├── jobs/                pipeline jobs (Phase 1: create_ctl_tables.py)
│   └── checks/              check_phase1.py
├── data/                    landing/, quarantine/, archive/   (ignored)
└── lakehouse/               Delta tables: ctl/, bronze/, silver/, gold/ (ignored)
```

**Run in Ubuntu:**

```bash
cd ~
mkdir -p screening-platform && cd screening-platform
git init -b main
mkdir -p sql/admin sql/firco src/common src/jobs src/checks \
         data/landing data/quarantine data/archive lakehouse
touch src/__init__.py src/common/__init__.py src/jobs/__init__.py src/checks/__init__.py
touch data/landing/.gitkeep data/quarantine/.gitkeep data/archive/.gitkeep
```

`.gitkeep` is an empty placeholder: Git doesn't track empty folders, so this keeps the folder structure in the repository while its contents stay ignored.

**Set your Git identity** (once per machine; skip if `git config user.name` already prints your name):

```bash
git config --global user.name "Abhirup Das"
git config --global user.email "you@example.com"
```

The `.gitignore` and the first commit come at the end of Step 3, after the secrets file exists, so you can prove it's ignored before anything is committed.

## Step 2 — Python, Spark and Delta environment

A **virtual environment** (`.venv`) is a private Python install for this project only, so its packages never clash with other projects. uv creates it with Python 3.11 and installs the pinned packages in seconds.

**2a. Pin the packages.** Create `requirements.txt`:

```text
pyspark==3.5.3
delta-spark==3.2.1
oracledb==3.4.2
python-dotenv==1.2.4
```

| Package | Why |
| --- | --- |
| `pyspark` | Spark engine, the same API as Databricks |
| `delta-spark` | Delta Lake tables locally, as on Databricks |
| `oracledb` | Oracle driver; "thin" mode needs no Oracle client install |
| `python-dotenv` | Reads `.env` into settings |

**2b. Create the environment and point Spark at Java 17:**

```bash
cd ~/screening-platform
uv venv --python 3.11 .venv
source .venv/bin/activate          # prompt now starts with (.venv)
uv pip install -r requirements.txt
echo 'export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64' >> ~/.bashrc
export JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
```

Run `source .venv/bin/activate` every time you open a new terminal for this project.

**2c. The shared Spark helper.** Create `src/common/spark.py`. Every job gets Spark from here, so the Delta settings live in one place:

```python
"""One place to build the local Spark session with Delta Lake enabled."""
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession


def get_spark(app_name: str = "screening-platform") -> SparkSession:
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.showConsoleProgress", "false")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
```

- `local[*]`: run Spark on your PC using all CPU cores. On Databricks this line goes away and the cluster provides Spark.
- The two `delta` settings switch on Delta Lake.
- `configure_spark_with_delta_pip` downloads the matching Delta jars (a few MB) from Maven Central on the **first** run, then caches them in `~/.ivy2`.
- Time zone UTC: every timestamp is stored in one zone, so nightly runs never shift by an hour at daylight-saving changes.

**2d. Smoke test.** Write a Delta table and read it back:

```bash
python -c "from src.common.spark import get_spark; s = get_spark('smoke'); s.range(3).write.format('delta').mode('overwrite').save('/tmp/delta_smoke'); print('Delta OK, rows:', s.read.format('delta').load('/tmp/delta_smoke').count())"
```

Expected last line: `Delta OK, rows: 3`. The first run takes 1–2 minutes while the jars download; later runs take about 15 seconds.

## Step 3 — Configuration and secrets

Passwords live in exactly one place, `.env`, which Git ignores. Code reads them through `src/common/config.py`. Nothing is hardcoded in Python or SQL. In Azure the same names come from Key Vault instead.

**3a. `.env.example`** (committed; names only):

```bash
# Oracle container (Step 4)
ORACLE_CONTAINER=screening-oracle
ORACLE_PORT=1522
ORACLE_DSN=localhost:1522/FREEPDB1
ORACLE_SYSTEM_PASSWORD=

# Application accounts (Step 5)
ORACLE_FIRCO_APP_PASSWORD=
ORACLE_LH_EXTRACT_PASSWORD=

# Local paths
LAKEHOUSE_DIR=lakehouse
DATA_DIR=data
```

**3b. Create the real `.env` with random passwords**, readable only by you:

```bash
cp .env.example .env
for v in ORACLE_SYSTEM_PASSWORD ORACLE_FIRCO_APP_PASSWORD ORACLE_LH_EXTRACT_PASSWORD; do
  sed -i "s/^$v=.*/$v=Pw_$(openssl rand -hex 10)/" .env
done
chmod 600 .env
grep PASSWORD .env | sed 's/=.*/=<set>/'      # confirm all three are set, without printing them
```

Random 20-character passwords mean nobody can guess them, and you'll never type them: scripts read them from `.env`. They start with a letter (`Pw_`) because Oracle requires that for unquoted passwords.

**3c. `src/common/config.py`**: the single entry point for settings:

```python
"""Central settings: every path and secret comes from here, read from .env."""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing setting {name} - copy .env.example to .env and fill it in")
    return value


LAKEHOUSE = PROJECT_ROOT / os.getenv("LAKEHOUSE_DIR", "lakehouse")
DATA = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
ORACLE_DSN = os.getenv("ORACLE_DSN", "localhost:1522/FREEPDB1")


def oracle_password(user: str) -> str:
    """Password for an Oracle account, e.g. oracle_password('lh_extract')."""
    return _required(f"ORACLE_{user.upper()}_PASSWORD")
```

A missing setting fails immediately with a clear message, instead of a confusing login error later. That's called **failing fast**.

**3d. `.gitignore`**, then prove the secrets are ignored before the first commit:

```text
.env
.venv/
__pycache__/
*.pyc
lakehouse/
data/**
!data/**/
!data/**/.gitkeep
spark-warehouse/
metastore_db/
derby.log
*.log
```

```bash
git check-ignore -v .env              # must print: .gitignore:1:.env  .env
git add -A
git status --short                    # .env, .venv and lakehouse must NOT appear
git commit -m "Phase 1: project skeleton, pinned environment, config"
```

If `.env` ever appears in `git status`, stop. Don't commit; fix `.gitignore` first. A password pushed to GitHub is compromised forever, even after you delete it, because it stays in the history.

## Step 4 — A dedicated Oracle container with a persistent volume

This Oracle plays Firco's back-end database. Two things differ from the take-home container, and both fix problems you hit there:

| Setting | Take-home | This project | Why |
| --- | --- | --- | --- |
| Port | 1521 | **1522** | Both containers can exist side by side |
| Data storage | Inside the container | **Named volume** `screening-oradata` | `docker rm` no longer wipes the database |
| Starting it | Long `docker run` pasted by hand | **`make db-up`** | No more lost `\` line breaks when pasting |
| Password | Typed in the command | Read from `.env` | Never on screen or in shell history |

**4a. The Makefile.** Create `Makefile` in the project root. It turns every long command into one word. The first line, `.RECIPEPREFIX = >`, makes command lines start with `>` instead of a TAB, so copy-pasting can't break it.

```makefile
# Phase 1 Makefile. Recipe lines start with ">" instead of a TAB, so copy-paste can't break them.
.RECIPEPREFIX = >
SHELL := /bin/bash
include .env
export

PY      := .venv/bin/python
SYS_SQL  = docker exec -i -w /work $(ORACLE_CONTAINER) sqlplus -s -l system/$(ORACLE_SYSTEM_PASSWORD)@//localhost:1521/FREEPDB1

.PHONY: help db-up db-wait db-start db-stop db-init ctl check sqlplus

help:
> @echo "make db-up | db-start | db-stop | db-init | ctl | check | sqlplus"

db-up:            ## first time only: create the container + data volume
> @docker run -d --name $(ORACLE_CONTAINER) -p $(ORACLE_PORT):1521 \
>   -e ORACLE_PASSWORD=$(ORACLE_SYSTEM_PASSWORD) \
>   -v screening-oradata:/opt/oracle/oradata -v $(CURDIR):/work \
>   gvenzl/oracle-free:23-slim
> @$(MAKE) --no-print-directory db-wait

db-wait:
> @echo "Waiting for Oracle (first start: 5-10 minutes)..."
> @until docker exec $(ORACLE_CONTAINER) healthcheck.sh >/dev/null 2>&1; do sleep 10; done
> @echo "Oracle is ready."

db-start:         ## every later session
> @docker start $(ORACLE_CONTAINER) >/dev/null
> @$(MAKE) --no-print-directory db-wait

db-stop:
> @docker stop $(ORACLE_CONTAINER)

db-init:          ## roles, users, Firco tables, grants (safe to re-run)
> @$(SYS_SQL) @sql/admin/01_roles_users.sql $(ORACLE_FIRCO_APP_PASSWORD) $(ORACLE_LH_EXTRACT_PASSWORD)
> @$(SYS_SQL) @sql/firco/02_tables.sql
> @$(SYS_SQL) @sql/admin/03_grants.sql

ctl:              ## Delta control tables
> @$(PY) -m src.jobs.create_ctl_tables

check:            ## prove Phase 1 is done
> @$(PY) -m src.checks.check_phase1

sqlplus:          ## interactive SQL*Plus as SYSTEM
> @docker exec -it -w /work $(ORACLE_CONTAINER) sqlplus system/$(ORACLE_SYSTEM_PASSWORD)@//localhost:1521/FREEPDB1
```

The `@` at the start of a command line stops make from printing it, so passwords never appear on screen.

**4b. Start it (first time only):**

```bash
docker stop cibc-oracle 2>/dev/null   # frees about 2 GB of RAM; the take-home DB is kept
make db-up
```

Expected: a long container ID, then `Waiting for Oracle…`, then `Oracle is ready.` The first start builds a fresh database in the empty volume, so it's slower than the take-home container: allow up to 10 minutes. `healthcheck.sh` is a readiness check built into the image, more reliable than searching the logs.

**Every later session:** `make db-start`, not `db-up`. The container and its volume already exist.

**Confirm the volume exists:** `docker volume ls | grep screening-oradata`.

## Step 5 — Oracle users and least-privilege grants

Three accounts, each able to do exactly its job and nothing more. Privileges are granted to **roles**, and roles to users: when someone or something new needs the same access, you grant the role, not 15 separate privileges.

**Who can do what**

| Table (schema `AML_FFF`) | `AML_FFF` (owner) | `FIRCO_APP` (engine + analysts) | `LH_EXTRACT` (lakehouse / ADF) |
| --- | --- | --- | --- |
| `alert_snapshot` | owns, can't log in | SELECT, INSERT, UPDATE | SELECT |
| `alert_audit_log` | owns | SELECT, **INSERT only** | SELECT |
| `fff_users` | owns | SELECT, INSERT, UPDATE | SELECT |
| `fff_user_profile` | owns | SELECT, INSERT, DELETE | SELECT |
| `fof_audit_profile` | owns | SELECT, INSERT | SELECT |
| `screening_input` | owns | SELECT, INSERT, UPDATE | none |
| `sanctions_list_entry` | owns | SELECT, INSERT | none |
| Log in? | **No** (`NO AUTHENTICATION`) | Yes | Yes |

Three design points to understand:

- **The owner can't log in.** Nobody can use the owner account to drop tables by accident; table changes are done by an admin, deliberately.
- **The audit log is append-only by privilege, not by promise.** `FIRCO_APP` has no UPDATE or DELETE on `alert_audit_log`, so even a bug in the engine can't rewrite history.
- **`LH_EXTRACT` reads 5 tables only.** It can't see the screening input or the list, and it can't write anything. This is the account ADF + SHIR will use in Tier 2.

**5a. `sql/admin/01_roles_users.sql`** (run as SYSTEM; the two passwords arrive as script arguments `&1` and `&2` from `.env`, never written in the file):

```sql
-- Phase 1: schema owner, roles and application users. Safe to re-run.
WHENEVER SQLERROR EXIT FAILURE
SET VERIFY OFF FEEDBACK ON SQLBLANKLINES ON
-- 1. Schema owner: owns Firco's tables; nobody can log in as it
CREATE USER IF NOT EXISTS aml_fff NO AUTHENTICATION;
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
```

**5b. `sql/admin/03_grants.sql`** (run after the tables exist, Step 6):

```sql
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
```

SYSTEM can grant on another schema's tables because the DBA role includes `GRANT ANY OBJECT PRIVILEGE`. Oracle 23ai also offers schema-level grants (`GRANT SELECT ANY TABLE ON SCHEMA …`), but those would give `LH_EXTRACT` every table, including future ones. Table-level grants keep it at exactly 5.

**Portability note:** `IF NOT EXISTS` is new in Oracle 23ai. On 19c, the version most banks run, these scripts would use the same PL/SQL "ignore already exists" pattern as the roles.

## Step 6 — Firco's tables in AML\_FFF

Seven tables. Five are copied column for column from the take-home schemas, so your Task 2 and Task 5 queries will run on this database unchanged in Phase 5. Two are new: what Firco is asked to screen, and the list it screens against.

| Table | Origin | Grain |
| --- | --- | --- |
| `alert_snapshot` | Task 2 schema | one row per alert, current state |
| `alert_audit_log` (+ `alert_audit_seq`) | Task 2 schema | one row per event, append-only |
| `fff_users`, `fff_user_profile`, `fof_audit_profile` | Task 5 schema | users, role assignments, role-name history |
| `screening_input` | New | one row per customer per screening batch |
| `sanctions_list_entry` | New | one row per list entry per list version |

**`sql/firco/02_tables.sql`:**

```sql
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
```

Notes:

- **`alert_audit_seq`** gives each new audit event its `audit_event_id`. `NOCACHE` makes the IDs gap-free and in order, which keeps the extract watermark simple. That's fine at this volume; a high-volume system would cache the sequence.
- **`date_of_birth` is text** because sanctions lists often publish partial dates (only a year, for example). Converting those to `DATE` would fail or invent a day.
- **`change_type`** records why each customer was screened (`NEW`, `UPDATES`, or a `LIST_RESCREEN` after a list change), so every alert can be traced back to its trigger.

**Now run Steps 5 and 6 together:**

```bash
make db-init
```

Expected output, three lines: `01_roles_users: done`, `02_tables: done`, `03_grants: done`. Because every statement is "if not exists" or a re-grant, running it again prints the same three lines and changes nothing. That property is **idempotent**: re-running is always safe, which matters when a deploy fails halfway.

## Step 7 — Control tables as Delta

The control tables are the pipeline's own record of what it did. `file_manifest` answers "which files arrived, and did they pass the gate?" (Task 3, from Phase 3). `run_log` answers "what ran, how many rows went in and out, and did it succeed?" Together they're what you show an auditor, and what the reconciliation checks read.

| Table | One row per | Key columns |
| --- | --- | --- |
| `ctl.file_manifest` | inbound file | `file_name`, `checksum_result` (MATCH / MISMATCH / UNDETERMINED), `row_count`, `status` (ACCEPTED / QUARANTINED) |
| `ctl.run_log` | stage of a pipeline run | `run_id`, `stage`, `rows_in`, `rows_out`, `rows_rejected`, `status`, start and end times |

**`src/jobs/create_ctl_tables.py`:**

```python
"""Phase 1: create the two control tables as Delta. Safe to run again (IF NOT EXISTS)."""
from src.common.config import LAKEHOUSE
from src.common.spark import get_spark

DDL = {
    "file_manifest": """
        file_name        STRING    NOT NULL,
        source_system    STRING    NOT NULL,
        business_date    DATE      NOT NULL,
        received_at      TIMESTAMP NOT NULL,
        checksum_algo    STRING,
        checksum_result  STRING    NOT NULL,
        row_count        BIGINT,
        status           STRING    NOT NULL,
        run_id           STRING    NOT NULL
    """,
    "run_log": """
        run_id           STRING    NOT NULL,
        job_name         STRING    NOT NULL,
        business_date    DATE      NOT NULL,
        stage            STRING    NOT NULL,
        status           STRING    NOT NULL,
        rows_in          BIGINT,
        rows_out         BIGINT,
        rows_rejected    BIGINT,
        started_at       TIMESTAMP NOT NULL,
        ended_at         TIMESTAMP,
        message          STRING
    """,
}


def main() -> None:
    spark = get_spark("create-ctl-tables")
    for name, columns in DDL.items():
        path = (LAKEHOUSE / "ctl" / name).as_posix()
        spark.sql(f"CREATE TABLE IF NOT EXISTS delta.`{path}` ({columns}) USING DELTA")
        count = spark.read.format("delta").load(path).count()
        print(f"ctl.{name}: ready at {path} ({count} rows)")
    spark.stop()


if __name__ == "__main__":
    main()
```

- \*\*`delta.\`path\``** creates the table at a folder path, with no metastore needed locally. On Databricks the same columns become Unity Catalog tables (`screening.ctl.run\_log\`) in Phase 6; only that one line changes.
- **`NOT NULL`** is enforced by Delta: a job that forgets `run_id` fails immediately instead of writing an untraceable row.
- **Explicit columns, not inferred from data**: the control tables' shape is a contract that later phases write to.

**Run it:**

```bash
make ctl
```

Expected:

```text
ctl.file_manifest: ready at /home/<you>/screening-platform/lakehouse/ctl/file_manifest (0 rows)
ctl.run_log: ready at /home/<you>/screening-platform/lakehouse/ctl/run_log (0 rows)
```

Run it a second time: same output, nothing recreated or emptied.

## Step 8 — The phase check

Don't take "it seems to work" as done. `make check` tests every done criterion from the top of this guide and fails loudly if any is missing. It's a small automated **control test**: each check tries the forbidden action and passes only if Oracle refuses it.

**`src/checks/check_phase1.py`:**

```python
"""Phase 1 acceptance check. Prints PASS/FAIL per control and exits 1 if anything failed."""
import subprocess
import sys

import oracledb

from src.common.config import LAKEHOUSE, ORACLE_DSN, PROJECT_ROOT, oracle_password

# Oracle error codes that mean "you are not allowed to do that"
DENIED = {942, 1031, 41900}  # table not visible / insufficient privileges / missing privilege (23ai)


def connect(user: str):
    return oracledb.connect(user=user, password=oracle_password(user), dsn=ORACLE_DSN)


def must_be_denied(cur, sql: str) -> None:
    try:
        cur.execute(sql)
    except oracledb.DatabaseError as exc:
        if exc.args[0].code in DENIED:
            return
        raise
    raise AssertionError(f"was ALLOWED but should be denied: {sql}")


def secrets_present():
    for user in ("system", "firco_app", "lh_extract"):
        oracle_password(user)
    return "3 passwords set in .env"


def env_not_in_git():
    ignored = subprocess.run(["git", "check-ignore", "-q", ".env"], cwd=PROJECT_ROOT).returncode == 0
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", ".env"], cwd=PROJECT_ROOT,
                             capture_output=True).returncode == 0
    assert ignored and not tracked, ".env is not ignored, or is already committed"
    return ".env ignored and untracked"


def oracle_volume():
    out = subprocess.run(["docker", "volume", "inspect", "screening-oradata"], capture_output=True)
    assert out.returncode == 0, "volume screening-oradata not found"
    return "named volume exists"


def owner_cannot_login():
    try:
        oracledb.connect(user="aml_fff", password="anything", dsn=ORACLE_DSN)
    except oracledb.DatabaseError as exc:
        if exc.args[0].full_code == "ORA-01017":  # invalid credentials = the account can't log in
            return "login refused (ORA-01017)"
        raise  # any other error (e.g. database down) is NOT a pass
    raise AssertionError("AML_FFF could log in")


def extract_reads_five_tables():
    tables = ["alert_snapshot", "alert_audit_log", "fff_users", "fff_user_profile", "fof_audit_profile"]
    with connect("lh_extract") as con, con.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM aml_fff.{t}")
    return "SELECT works on 5 tables"


def extract_cannot_write_or_see_more():
    with connect("lh_extract") as con, con.cursor() as cur:
        must_be_denied(cur, "INSERT INTO aml_fff.alert_audit_log (audit_event_id) SELECT -1 FROM dual WHERE 1 = 0")
        must_be_denied(cur, "SELECT COUNT(*) FROM aml_fff.screening_input")
    return "INSERT denied; screening_input not visible"


def audit_log_is_append_only():
    with connect("firco_app") as con, con.cursor() as cur:
        cur.execute(
            "INSERT INTO aml_fff.alert_audit_log (audit_event_id, alert_id, event_timestamp, actor_type,"
            " actor_id, action_code) VALUES (-1, 'CHECK', SYSTIMESTAMP, 'WORKFLOW', 'phase1-check', 'TEST')"
        )
        con.rollback()  # leave no trace
        must_be_denied(cur, "UPDATE aml_fff.alert_audit_log SET comments = 'x' WHERE 1 = 0")
        must_be_denied(cur, "DELETE FROM aml_fff.alert_audit_log WHERE 1 = 0")
    return "INSERT allowed (rolled back); UPDATE and DELETE denied"


def ctl_tables_exist():
    for name in ("file_manifest", "run_log"):
        assert (LAKEHOUSE / "ctl" / name / "_delta_log").is_dir(), f"ctl.{name} missing - run: make ctl"
    return "ctl.file_manifest, ctl.run_log"


CHECKS = [
    secrets_present, env_not_in_git, oracle_volume, owner_cannot_login,
    extract_reads_five_tables, extract_cannot_write_or_see_more, audit_log_is_append_only, ctl_tables_exist,
]


def main() -> int:
    failed = 0
    for check in CHECKS:
        try:
            print(f"PASS  {check.__name__:34} {check()}")
        except Exception as exc:  # report every failure, don't stop at the first
            failed += 1
            print(f"FAIL  {check.__name__:34} {str(exc).splitlines()[0]}")
    print(f"\n{len(CHECKS) - failed}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
```

How it reads:

- **Negative tests** (`must_be_denied`) pass only when Oracle refuses. The `WHERE 1 = 0` makes the statement touch no rows, but Oracle still checks privileges before running it, so the test is safe.
- **The insert test rolls back**, so the audit log stays empty.
- **A connection failure is never a pass.** Only `ORA-01017` (invalid credentials) proves the owner can't log in; "database is down" fails the check.
- It runs **every** check and reports them all, like the Task 3 script, and exits 1 for the scheduler, like Task 3's exit codes.

**8a. Run it:**

```bash
make check
```

Expected:

```text
PASS  secrets_present                    3 passwords set in .env
PASS  env_not_in_git                     .env ignored and untracked
PASS  oracle_volume                      named volume exists
PASS  owner_cannot_login                 login refused (ORA-01017)
PASS  extract_reads_five_tables          SELECT works on 5 tables
PASS  extract_cannot_write_or_see_more   INSERT denied; screening_input not visible
PASS  audit_log_is_append_only           INSERT allowed (rolled back); UPDATE and DELETE denied
PASS  ctl_tables_exist                   ctl.file_manifest, ctl.run_log

8/8 checks passed
```

**8b. Prove the data survives losing the container** (done criterion 3), then check again:

```bash
docker rm -f screening-oracle      # deletes the container, NOT the volume
make db-up                         # new container, same volume: starts fast, no rebuild
make check                         # still 8/8, tables and users survived
```

**8c. Commit:**

```bash
git add -A
git status --short                 # .env must not be listed
git commit -m "Phase 1: Oracle roles, users, Firco tables, ctl tables, phase check"
```

Phase 1 is done when `make check` prints 8/8 and the commit is made.

## Troubleshooting

The first five rows are errors you already hit in the take-home; the rest are the likely ones for this phase. Read the error code first, then find it here.

| Symptom | Cause | Fix |
| --- | --- | --- |
| `docker: invalid reference format` | Running in PowerShell | Use the Ubuntu terminal |
| `SP2-0310: unable to open file "…sqlFREE…"` | Paste mixed old text into the line | Type `@file` lines by hand; long SQL lives in files |
| `ls: cannot open directory '.'` inside the container | Project folder deleted and re-created after the container started | `docker rm -f screening-oracle`, then `make db-up` (the volume keeps the data) |
| `ORA-01031: insufficient privileges` while creating users | Connected as an app user, not SYSTEM | Use `make db-init` or `make sqlplus`; both connect as SYSTEM |
| SQL\*Plus shows `2` and waits | Statement not finished (missing `;`) | Type `/` to run it, or `.` to abandon it |
| `Makefile:4: .env: No such file or directory` | `.env` not created yet | Step 3b |
| `make db-wait` never finishes | Database still building, or the start failed | `docker logs --tail 50 screening-oracle`; the first build can take 10 minutes |
| `port is already allocated` on 1522 | Something else uses 1522 | Change `ORACLE_PORT` and the port in `ORACLE_DSN` in `.env` |
| `DPY-6005: cannot connect to database` | Container stopped, or wrong port in `ORACLE_DSN` | `make db-start`; check `docker ps` shows `0.0.0.0:1522->1521` |
| `ORA-01017` for `FIRCO_APP` or `LH_EXTRACT` | `.env` passwords changed after the users were created | `make db-init` again; its `ALTER USER` re-syncs the passwords |
| `ORA-01950: no privileges on tablespace 'USERS'` | Owner has no quota | The `ALTER USER aml_fff QUOTA` line in `01_roles_users.sql` didn't run; re-run `make db-init` |
| `JAVA_GATEWAY_EXITED` | Java missing, or `JAVA_HOME` points at the wrong Java | `java -version` must show 17; re-run the `export JAVA_HOME=…` line from Step 2b |
| `module not found: io.delta#delta-spark_2.12` | Spark couldn't download the Delta jars (no internet or a proxy) | Check internet access to `repo1.maven.org`; retry `make ctl` |
| `ModuleNotFoundError: No module named 'src'` | Command run outside the project root, or not as a module | `cd ~/screening-platform`, then use `make ctl` / `make check` (they run `python -m …`) |
| PC slows down badly, or Oracle crashes | WSL is short of memory with two databases | Keep `cibc-oracle` stopped; optionally cap WSL in `%UserProfile%\.wslconfig` with `memory=8GB` |

**Changing the SYSTEM password later:** the image applies `ORACLE_PASSWORD` only when it first creates the database ([image docs](https://hub.docker.com/r/gvenzl/oracle-free)). After you edit `ORACLE_SYSTEM_PASSWORD` in `.env`, also run `docker exec screening-oracle resetPassword <new password>`, or `make db-init` will fail with `ORA-01017`.

**When a check fails:** read the `FAIL` line's message, fix that one thing, and run `make check` again. The other checks keep reporting, so one failure doesn't hide another.

## Interview talking points and sources

Phase 1 has no data in it, but it's where most of the security and operability decisions are made. Four lines to use:

- **Least privilege:** "Three Oracle accounts: an owner nobody can log into, an application account, and a read-only extract account limited to the five reporting tables. Privileges go through roles."
- **Controls enforced, not promised:** "The audit log is append-only because the application has no UPDATE or DELETE privilege on it, and I prove that with an automated negative test."
- **Secrets:** "Passwords are generated, live only in an ignored `.env` locally and in Key Vault on Azure, and are never printed or committed."
- **Reproducible and idempotent:** "Pinned versions, one-command setup, and scripts that are safe to re-run. A new machine gets the same environment, and a failed deploy can simply be run again."

**What was tested while writing this guide:** the Python 3.11 + PySpark 3.5.3 + Java 17 environment, `config.py`, the `.gitignore` rules, the Makefile (dry run) and the check script's failure reporting. Two things couldn't be run in my test environment: Oracle (its image download was blocked) and the Delta jar download (Maven Central was blocked). So the SQL scripts and the Delta step are written to Oracle 23ai and Delta 3.2 syntax but are untested. Treat the first `make db-init` and `make ctl` as the real test, and paste any error here.

**Sources**

- [gvenzl/oracle-free image](https://hub.docker.com/r/gvenzl/oracle-free), Docker Hub (persistent volume at `/opt/oracle/oradata`, `healthcheck.sh`, first-start-only passwords)
- [Azure Data Factory Oracle connector](https://learn.microsoft.com/azure/data-factory/connector-oracle), Microsoft Learn (why the `LH_EXTRACT` account exists: Tier 2 ADF + SHIR)
- [Sanctions Screening Platform — Mini-Project Architecture](https://claude.ai/artifact/LeijRpiKGYETW8mGx9xiiL), the design this phase implements
