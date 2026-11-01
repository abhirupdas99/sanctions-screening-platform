# Phase 1 Makefile. Recipe lines start with ">" instead of a TAB, so copy-paste can't break them.
.RECIPEPREFIX = >
SHELL := /bin/bash
include .env
export

PY      := .venv/bin/python
SYS_SQL  = docker exec -i -w /work $(ORACLE_CONTAINER) sqlplus -s -l system/$(ORACLE_SYSTEM_PASSWORD)@//localhost:1521/FREEPDB1

.PHONY: help db-up db-wait db-start db-stop db-init ctl check sqlplus list generate check-p2 ingest ingest-all show-ctl check-p3 reset-p3 silver silver-all check-p4 reset-p4

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

LIST_URL := https://www.international.gc.ca/world-monde/assets/office_docs/international_relations-relations_internationales/sanctions/sema-lmes.xml

list:             ## download the real Canadian sanctions list
> @mkdir -p data/reference
> @curl -fsSL -o data/reference/sema-lmes.xml "$(LIST_URL)"
> @echo "Downloaded: $$(grep -o '<record>' data/reference/sema-lmes.xml | wc -l) records"
> @sha256sum data/reference/sema-lmes.xml

generate:         ## Phase 2: synthetic files + answer key
> @$(PY) -m src.generator.generate_data

check-p2:         ## prove Phase 2 is done
> @$(PY) -m src.checks.check_phase2

DATE ?= 2026-10-01

ingest:           ## Phase 3: one business date (make ingest DATE=2026-10-02)
> @$(PY) -m src.jobs.ingest_day --date $(DATE)

ingest-all:       ## Phase 3: every day folder in landing, oldest first
> @$(PY) -m src.jobs.ingest_day --all

show-ctl:         ## print the file manifest and run log
> @$(PY) -m src.checks.show_ctl

check-p3:         ## prove Phase 3 is done
> @$(PY) -m src.checks.check_phase3

reset-p3:         ## start Phase 3 again: regenerate files, empty bronze and ctl
> @rm -rf lakehouse/bronze lakehouse/ctl
> @find data/archive data/quarantine -mindepth 1 -not -name .gitkeep -exec rm -rf {} +
> @$(MAKE) --no-print-directory ctl generate

silver:           ## Phase 4: one business date (make silver DATE=2026-10-02); earlier days first
> @$(PY) -m src.jobs.build_silver --date $(DATE)

silver-all:       ## Phase 4: every business date in bronze, oldest first
> @$(PY) -m src.jobs.build_silver --all

check-p4:         ## prove Phase 4 is done
> @$(PY) -m src.checks.check_phase4

reset-p4:         ## rebuild Phase 4 from bronze: drop silver, gold and the outbound files
> @rm -rf lakehouse/silver lakehouse/gold data/outbound