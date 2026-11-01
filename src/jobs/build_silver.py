"""Phase 4: build silver for one business date (or all), then the screening extract for Firco.

bronze.customer_raw --(effective snapshot)--> validate + dedupe (Task 1) --> silver.customer_resolved
                                           --> change detection (Task 4) --> silver.customer_change
                                           --> parents of changed people --> gold.screening_extract + file
"""
import argparse
import csv
import hashlib
import io
import sys
from functools import reduce

from pyspark.sql import functions as F

from src.common import tables
from src.common.config import DATA
from src.common.spark import get_spark
from src.jobs.ingest_day import RUN_LOG, now
from src.silver.rules import dedupe, detect_changes, validate

JOB = "build_silver"
SOURCES = ("core", "cards")
OUTBOUND = DATA / "outbound" / "firco"
EXTRACT_COLUMNS = ["batch_id", "subject_key", "source_record_id", "subject_name", "country_code", "date_of_birth", "change_type"]


def usable_extracts(spark):
    """For each source, the business dates with a non-empty accepted extract in bronze."""
    rows = tables.read(spark, "bronze", "customer_raw").groupBy("source_system", "business_date").count().collect()
    return {s: sorted(r.business_date for r in rows if r.source_system == s and r["count"] > 0) for s in SOURCES}


def effective_snapshot(spark, day, extracts):
    """Today's view of every source. A source with no usable file today (quarantined, still waiting, or
    zero rows) is carried forward from its last good extract, so its customers aren't reported as DELETED."""
    bronze = tables.read(spark, "bronze", "customer_raw")
    parts, notes = [], []
    for source in SOURCES:
        usable = [d for d in extracts[source] if d <= day]
        if not usable:
            notes.append(f"{source}: no extract yet")
            continue
        used = usable[-1]
        if used != day:
            notes.append(f"{source}: no usable extract for {day}, carried forward {used}")
        parts.append(bronze.filter((F.col("source_system") == source) & (F.col("business_date") == used))
                     .withColumn("extract_date", F.col("business_date"))
                     .withColumn("business_date", F.lit(day)))
    return reduce(lambda a, b: a.unionByName(b), parts), notes


def counts_by(df, col):
    return {r[col]: r["count"] for r in df.groupBy(col).count().collect()}


def write_extract_file(day, rows):
    """The hand-off to Firco: a TSV plus a SHA3-256 checksum, written checksum-last as the completion signal."""
    folder = OUTBOUND / day.isoformat()
    folder.mkdir(parents=True, exist_ok=True)
    buf = io.StringIO()
    writer = csv.writer(buf, delimiter="\t", lineterminator="\n")
    writer.writerow(EXTRACT_COLUMNS)
    writer.writerows([[r[c] or "" for c in EXTRACT_COLUMNS] for r in rows])
    data = buf.getvalue().encode("utf-8")
    name = f"screening_input_{day:%Y%m%d}.tsv"
    (folder / name).write_bytes(data)
    (folder / f"{name}.checksum").write_text(hashlib.sha3_256(data).hexdigest() + "\n")
    return folder / name


def build_day(spark, day, previous_day, extracts):
    run_id = f"S-{day:%Y%m%d}-{now():%H%M%S}"
    started, log = now(), []

    def stage(name, status, rows_in, rows_out, rejected, message):
        log.append((run_id, JOB, day, name, status, rows_in, rows_out, rejected, started, now(), message))

    try:
        # 1. Effective snapshot (with carry-forward for missing sources)
        snapshot, notes = effective_snapshot(spark, day, extracts)
        rows_in = snapshot.count()
        if notes:
            stage("source_freshness", "WARNING", None, None, None, "; ".join(notes))

        # 2. Task 1: validate + dedupe -> silver.customer_resolved
        tables.overwrite_partitions(dedupe(validate(snapshot)), "silver", "customer_resolved", ["business_date"])
        resolved = tables.read(spark, "silver", "customer_resolved").filter(F.col("business_date") == day)
        st = counts_by(resolved, "status")
        parents, dups, invalid = st.get("NOT_DUPLICATE", 0), st.get("DUPLICATE", 0), st.get("INVALID", 0)
        if parents + dups + invalid != rows_in:                     # reconciliation: nothing lost or added
            raise RuntimeError(f"resolved {parents + dups + invalid} rows from {rows_in} snapshot rows")
        stage("validate", "SUCCESS", rows_in, parents + dups, invalid, f"{invalid} INVALID")
        stage("dedupe", "SUCCESS", parents + dups, parents, 0, f"{dups} duplicates linked to {parents} parents")

        # 3. Task 4: change detection -> silver.customer_change
        previous = None if previous_day is None else (
            tables.read(spark, "silver", "customer_resolved").filter(F.col("business_date") == previous_day))
        changes = detect_changes(resolved, previous).withColumn("business_date", F.lit(day))
        tables.overwrite_partitions(changes, "silver", "customer_change", ["business_date"])
        change = tables.read(spark, "silver", "customer_change").filter(F.col("business_date") == day)
        ct = counts_by(change, "change_type")
        new, upd, same, gone = (ct.get(k, 0) for k in ("NEW", "UPDATES", "NO_UPDATES", "DELETED"))
        if new + upd + same != parents + dups:          # reconciliation: one outcome per valid row today
            raise RuntimeError(f"change counts {ct} don't add up to {parents + dups} valid rows")
        ambiguous = change.filter(F.array_contains("changed_columns", "ambiguous_record_id")).count()
        stage("change_detect", "WARNING" if ambiguous else "SUCCESS", parents + dups, new + upd, 0,
              f"NEW={new} UPDATES={upd} NO_UPDATES={same} DELETED={gone} (vs {previous_day})"
              + (f"; {ambiguous} rows with a non-unique record_id re-screened" if ambiguous else ""))

        # 4. Screening extract: the parent of every person with a NEW or UPDATED record today
        changed_groups = (resolved.join(change.filter(F.col("change_type").isin("NEW", "UPDATES")),
                                        ["source_system", "record_id"])
                          .select("group_key").distinct())
        own = (change.groupBy("source_system", "record_id")      # one row per ID, even if the ID repeats
               .agg(F.max(F.col("change_type") == "NEW").alias("own_new")))
        extract = (resolved.filter(F.col("status") == "NOT_DUPLICATE")
                   .join(changed_groups, "group_key")
                   .join(own, ["source_system", "record_id"], "left")
                   .select(F.lit(f"SCR-{day:%Y%m%d}").alias("batch_id"),
                           # one stable key per person: record IDs can repeat, a person's group key can't
                           F.upper(F.substring(F.sha2("group_key", 256), 1, 16)).alias("subject_key"),
                           F.trim("record_id").alias("source_record_id"),
                           F.regexp_replace(F.trim("name"), r"\s+", " ").alias("subject_name"),
                           F.col("cc_key").alias("country_code"),
                           F.col("date_of_birth"),
                           F.when(F.col("own_new"), "NEW").otherwise("UPDATES").alias("change_type"),
                           F.col("source_system"))
                   .withColumn("business_date", F.lit(day)))
        tables.overwrite_partitions(extract, "gold", "screening_extract", ["business_date"])
        rows = (tables.read(spark, "gold", "screening_extract").filter(F.col("business_date") == day)
                .orderBy("subject_key").collect())
        path = write_extract_file(day, rows)
        stage("screening_extract", "SUCCESS", new + upd, len(rows), 0, f"{len(rows)} people -> {path.name}")
    except Exception as exc:                                         # record the failure, then stop
        stage("build_silver", "FAILED", None, None, None, str(exc)[:500])
        tables.append(spark.createDataFrame(log, RUN_LOG), "ctl", "run_log")
        raise
    tables.append(spark.createDataFrame(log, RUN_LOG), "ctl", "run_log")
    print(f"{day}  {run_id}  rows={rows_in} invalid={invalid} parents={parents} dups={dups}  "
          f"NEW={new} UPD={upd} DEL={gone}  to_screen={len(rows)}" + ("  [carried forward]" if notes else ""))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", help="business date, e.g. 2026-10-01 (earlier days must be built first)")
    group.add_argument("--all", action="store_true", help="every business date in bronze, oldest first")
    args = parser.parse_args()
    spark = get_spark(JOB)
    try:
        days = sorted(r.business_date for r in
                      tables.read(spark, "bronze", "customer_raw").select("business_date").distinct().collect())
        extracts = usable_extracts(spark)
        todo = days if args.all else [d for d in days if d.isoformat() == args.date]
        if not todo:
            sys.exit(f"No bronze data for {args.date} - run Phase 3 ingest first")
        built = set() if tables.read(spark, "silver", "customer_resolved") is None else {
            r.business_date for r in tables.read(spark, "silver", "customer_resolved")
            .select("business_date").distinct().collect()}
        for day in todo:
            i = days.index(day)
            previous_day = days[i - 1] if i else None
            if previous_day and previous_day not in built and not args.all:
                sys.exit(f"Build {previous_day} first: change detection compares with the previous business date")
            build_day(spark, day, previous_day, extracts)
            built.add(day)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())