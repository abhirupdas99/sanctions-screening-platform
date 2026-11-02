"""Phase 5: copy Firco's tables from Oracle into bronze, as the read-only LH_EXTRACT user.

This is the stand-in for ADF + self-hosted integration runtime (Tier 2). Small tables are copied in full into a
partition for the extract date; the append-only audit log is copied incrementally above a watermark.
"""
import argparse
import sys
from datetime import date

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common import oracle, tables
from src.common.spark import get_spark
from src.firco.engine import OUTBOUND
from src.jobs.ingest_day import RUN_LOG, now

JOB, EXTRACT_USER = "extract_firco", "lh_extract"
S, L, D, TS = T.StringType(), T.LongType(), T.DoubleType(), T.TimestampType()
COLUMNS = {   # Oracle table -> columns and Spark types, in order
    "alert_snapshot": [("alert_id", S), ("source_record_id", S), ("subject_name", S), ("country_code", S),
                       ("vendor_alert_ref", S), ("alert_status", S), ("snapshot_updated_at", TS)],
    "alert_hit": [("alert_id", S), ("subject_key", S), ("batch_id", S), ("trigger_type", S), ("list_version", S),
                  ("list_entry_id", S), ("list_name", S), ("list_country", S), ("list_dob", S),
                  ("match_score", D), ("hit_count", L), ("created_at", TS)],
    "fff_users": [("id", L), ("username", S), ("email", S), ("active", S), ("created_at", TS)],
    "fff_user_profile": [("user_id", L), ("profile_id", L)],
    "fof_audit_profile": [("audit_profile_id", L), ("profile_id", L), ("name", S), ("created_at", TS)],
    "alert_audit_log": [("audit_event_id", L), ("alert_id", S), ("event_timestamp", TS), ("actor_type", S),
                        ("actor_id", S), ("action_code", S), ("decision_code", S), ("comments", S)],
}
CAST = {L: lambda v: None if v is None else int(v), D: lambda v: None if v is None else float(v)}


def schema(table):
    return T.StructType([T.StructField(c, t) for c, t in COLUMNS[table]]
                        + [T.StructField("extract_date", T.DateType(), False), T.StructField("run_id", S, False)])


def fetch(cur, table, where="", binds=None):
    cols = COLUMNS[table]
    cur.execute(f"SELECT {', '.join(c for c, _ in cols)} FROM aml_fff.{table} {where}", binds or {})
    return [tuple(CAST.get(t, lambda v: v)(v) for (_, t), v in zip(cols, row)) for row in cur.fetchall()]


def extract(spark, extract_date):
    run_id = f"X-{extract_date:%Y%m%d}-{now():%H%M%S}"
    started, log = now(), []
    with oracle.connect(EXTRACT_USER) as con:
        cur = con.cursor()
        # 1. Small tables: full copy into today's partition (history kept, re-runs replace)
        for table in ("alert_snapshot", "alert_hit", "fff_users", "fff_user_profile", "fof_audit_profile"):
            rows = [(*r, extract_date, run_id) for r in fetch(cur, table)]
            tables.overwrite_partitions(spark.createDataFrame(rows, schema(table)), "bronze", f"firco_{table}",
                                        ["extract_date"])
            log.append((run_id, JOB, extract_date, f"copy_{table}", "SUCCESS", len(rows), len(rows), 0,
                        started, now(), "full copy"))
        # 2. Audit log: only events above the watermark (it's append-only, so nothing below can change)
        existing = tables.read(spark, "bronze", "firco_alert_audit_log")
        watermark = 0 if existing is None else (existing.agg(F.max("audit_event_id")).first()[0] or 0)
        rows = [(*r, extract_date, run_id) for r in
                fetch(cur, "alert_audit_log", "WHERE audit_event_id > :w ORDER BY audit_event_id", {"w": watermark})]
        if rows:
            tables.append(spark.createDataFrame(rows, schema("alert_audit_log")), "bronze", "firco_alert_audit_log")
        cur.execute("SELECT COUNT(*) FROM aml_fff.alert_audit_log")
        in_oracle = cur.fetchone()[0]
    in_bronze = tables.read(spark, "bronze", "firco_alert_audit_log").count()
    new_mark = max(r[0] for r in rows) if rows else watermark
    status = "SUCCESS" if in_bronze == in_oracle else "FAILED"     # reconciliation: Oracle = bronze
    log.append((run_id, JOB, extract_date, "copy_alert_audit_log", status, len(rows), len(rows), 0, started, now(),
                f"watermark {watermark} -> {new_mark}; Oracle {in_oracle} events, bronze {in_bronze}"))
    tables.append(spark.createDataFrame(log, RUN_LOG), "ctl", "run_log")
    if status == "FAILED":
        raise RuntimeError(f"audit log has {in_oracle} events in Oracle but {in_bronze} in bronze")
    print(f"{extract_date}  {run_id}  audit events: {len(rows)} new (watermark {watermark}), "
          f"Oracle {in_oracle} = bronze {in_bronze}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--date", type=date.fromisoformat,
                        help="extract date (default: the latest business date screened)")
    args = parser.parse_args()
    extract_date = args.date or max(date.fromisoformat(p.name) for p in OUTBOUND.iterdir())
    spark = get_spark(JOB)
    try:
        extract(spark, extract_date)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())