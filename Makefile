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
