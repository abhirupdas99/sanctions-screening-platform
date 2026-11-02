# Demo script (about 8 minutes)

Before the interview:

- [ ] Docker Desktop running; `make db-start` shows "Oracle is ready."
- [ ] Two terminals open in `~/screening-platform`, font size up; VS Code open on `src/silver/rules.py`
- [ ] `make check-p5 | tee docs/evidence/check-p5.txt` run once, so the proof is saved even if something breaks live
- [ ] README open on GitHub in a browser tab
- [ ] Laptop plugged in, sleep off, notifications off

## 0. The pitch (30 s)

"I built the pipeline a bank runs for sanctions screening, end to end, on my laptop: two customer source systems, a checksum gate, a Delta lakehouse, a screening engine in Oracle, analysts with 4-eyes, and the compliance reports. It replays a month of synthetic data against the real Canadian sanctions list, and every phase has an acceptance check."

## 1. The shape (1 min) — README diagram

Walk the diagram left to right in one breath per box. Land on: **only new or changed people are screened**, and **when the list changes, everyone is re-screened against the new names**.

## 2. Files you can't trust (1 min)

```bash
make show-ctl
```

Point at the QUARANTINED and WAITING rows: "A checksum mismatch never reaches bronze. A file without its checksum waits; it isn't guessed."

## 3. Silver: why so few people get screened (1.5 min)

Show `src/silver/rules.py` briefly (validation reasons, lowest-ID parent, NULL-safe comparison), then the extract on the day a new sanctioned customer joins:

```bash
head -5 data/outbound/firco/2026-10-14/screening_input_20261014.tsv | column -t -s $'\t'
```

"Day 1 screens 428 people. After that it's 1 to 22 a day: only NEW and UPDATES. That's my change-detection logic in production."

## 4. Screening and 4-eyes, in Oracle (2 min)

```bash
make sqlplus-firco
```

```sql
SET LINESIZE 200
COLUMN subject_name FORMAT A28
COLUMN actor_id FORMAT A16
SELECT h.alert_id, h.trigger_type, TO_CHAR(h.created_at, 'YYYY-MM-DD') alerted, s.subject_name, l.actor_id confirmed_by
FROM aml_fff.alert_hit h
JOIN aml_fff.alert_snapshot s ON s.alert_id = h.alert_id
JOIN aml_fff.alert_audit_log l ON l.alert_id = h.alert_id AND l.decision_code = 'CONFIRMED_MATCH'
ORDER BY h.created_at;
```

"Five true matches. Two were caught by a LIST_RESCREEN: customers who'd been with the bank all month and got sanctioned on the 21st."

Then the trail of a LIST_RESCREEN one (put its alert_id from the result in the query):

```sql
SELECT audit_event_id, TO_CHAR(event_timestamp, 'MM-DD HH24:MI') at_time, actor_type, actor_id, action_code, decision_code
FROM aml_fff.alert_audit_log WHERE alert_id = 'ALT-20261021-0001' ORDER BY audit_event_id;
```

"Engine raises it, one analyst escalates, a *different* analyst confirms the next day. And this table is append-only: no account can update it."

## 5. The reports (1 min)

```bash
make reports
```

Task 2 table: "Counted from the audit log, never the snapshot, or last month's numbers would change every time an alert closes." Access review: read out the four flags.

## 6. Proof (30 s)

```bash
cat docs/evidence/check-p5.txt
```

"Ten checks, including re-running the whole month to prove nothing changes, and the Spark report matching the same SQL run directly on Oracle."

## 7. Where it goes next (1 min)

"At a bank, the source would be Oracle itself. Data Factory with a self-hosted runtime would copy it into ADLS in watermarked batches, Databricks with Unity Catalog would run these same silver rules, and analysts would decide alerts in a web workbench that writes back to Oracle. I've designed that version; the rules don't change, the plumbing does."

## If something breaks live

Don't debug in the interview. Say "this is exactly what the saved run is for", and show `docs/evidence/` and the README results.
