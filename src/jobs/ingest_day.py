"""Phase 3: nightly ingest for one business date, or all of them.

For each file in data/landing/<date>/:
  checksum gate -> MATCH: load to bronze, then archive | MISMATCH/UNDETERMINED: quarantine | MISSING: wait
Every file gets a row in ctl.file_manifest; every stage gets a row in ctl.run_log.
Safe to re-run: accepted files are skipped, and a bronze load replaces its own partition instead of appending.
"""
import argparse
import csv
import shutil
import sys
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone

from pyspark.sql import functions as F
from pyspark.sql import types as T

from src.common import tables
from src.common.config import DATA
from src.common.spark import get_spark
from src.ingest.checksum_gate import check_file

LANDING, ARCHIVE, QUARANTINE = DATA / "landing", DATA / "archive", DATA / "quarantine"
HEADER = ["Record_ID", "Name", "Country_Code", "Position_Held", "Company_Name", "Date_Of_Birth"]

S, D, TS, L = T.StringType(), T.DateType(), T.TimestampType(), T.LongType()
#the schema for the customer_raw table in the bronze layer. It has fields for each column in the input file, plus additional metadata fields for source_system, business_date, file_name, load_seq, run_id, and ingested_at.
CUSTOMER_RAW = T.StructType(
    [T.StructField(c.lower(), S) for c in HEADER]          # as received: text, untrimmed, nothing dropped
    + [T.StructField("source_system", S, False), T.StructField("business_date", D, False),
       T.StructField("file_name", S, False), T.StructField("load_seq", L, False),
       T.StructField("run_id", S, False), T.StructField("ingested_at", TS, False)])
#list of fields in the sanctions_list_raw table in the bronze layer. It has fields for list_version, entry_seq, country, last_name, given_name, entity_name, aliases, date_of_birth, schedule, item, date_of_listing, business_date, file_name, run_id, and ingested_at.
LIST_FIELDS = ["country", "last_name", "given_name", "entity_name", "aliases", "date_of_birth",
               "schedule", "item", "date_of_listing"]
#list of tags in the sanctions_list_raw table in the bronze layer. It maps the field names to their corresponding XML tags in the input file.
LIST_TAGS = {"country": "Country", "last_name": "LastName", "given_name": "GivenName",
             "entity_name": "EntityOrShip", "aliases": "Aliases", "date_of_birth": "DateOfBirth",
             "schedule": "Schedule", "item": "Item", "date_of_listing": "DateOfListing"}
#list_raw schema for the sanctions_list_raw table in the bronze layer. It has fields for list_version, entry_seq, country, last_name, given_name, entity_name, aliases, date_of_birth, schedule, item, date_of_listing, business_date, file_name, run_id, and ingested_at.
LIST_RAW = T.StructType(
    [T.StructField("list_version", S, False), T.StructField("entry_seq", L, False)]
    + [T.StructField(f, S) for f in LIST_FIELDS]
    + [T.StructField("business_date", D, False), T.StructField("file_name", S, False),
       T.StructField("run_id", S, False), T.StructField("ingested_at", TS, False)])
#manifest schema for the ctl.file_manifest table. It has fields for file_name, source_system, business_date, received_at, checksum_algo, checksum_result, row_count, status, and run_id.
MANIFEST = T.StructType([  # must match the Phase 1 DDL of ctl.file_manifest
    T.StructField("file_name", S, False), T.StructField("source_system", S, False),
    T.StructField("business_date", D, False), T.StructField("received_at", TS, False),
    T.StructField("checksum_algo", S), T.StructField("checksum_result", S, False),
    T.StructField("row_count", L), T.StructField("status", S, False), T.StructField("run_id", S, False)])
#run log schema for the ctl.run_log table. It has fields for run_id, job_name, business_date, stage, status, rows_in, rows_out, rows_rejected, started_at, ended_at, and message.
RUN_LOG = T.StructType([  # must match the Phase 1 DDL of ctl.run_log
    T.StructField("run_id", S, False), T.StructField("job_name", S, False),
    T.StructField("business_date", D, False), T.StructField("stage", S, False),
    T.StructField("status", S, False), T.StructField("rows_in", L), T.StructField("rows_out", L),
    T.StructField("rows_rejected", L), T.StructField("started_at", TS, False),
    T.StructField("ended_at", TS), T.StructField("message", S)])

JOB = "ingest_day"


def now():
    return datetime.now(timezone.utc).replace(tzinfo=None)   # stored as UTC, like the Spark session


class BadFile(Exception):
    """The file passed the checksum but its content is structurally wrong (e.g. unexpected columns)."""


def read_customers(path):
    """Rows exactly as received. newline='' lets the csv module handle Windows CRLF endings."""
    with path.open(encoding="utf-8", newline="") as fh:
        reader = csv.reader(fh, delimiter="\t")
        header = next(reader, None)
        if header != HEADER:
            raise BadFile(f"unexpected header {header}")
        rows = list(reader)
    bad = [i for i, r in enumerate(rows, 2) if len(r) != len(HEADER)]
    if bad:
        raise BadFile(f"{len(bad)} rows with the wrong number of columns, first at line {bad[0]}")
    return rows


def read_list(path):
    def text(rec, prefix):
        for child in rec:
            if child.tag.lower().startswith(prefix.lower()):
                return (child.text or "").strip()
        return None
    return [[text(rec, LIST_TAGS[f]) for f in LIST_FIELDS] for rec in ET.parse(path).getroot().iter("record")]


def move(path, target_root, business_date):
    """Move a data file and its checksum out of landing, so landing only holds unprocessed files."""
    target = target_root / business_date.isoformat()
    target.mkdir(parents=True, exist_ok=True)
    for p in (path, path.with_name(path.name + ".checksum")):
        if p.exists():
            shutil.move(str(p), target / p.name)


def load_bronze(spark, path, source, business_date, run_id):
    """Write one accepted file to bronze and prove the row count. Returns rows written."""
    loaded_at = now()
    if source == "sanctions":
        version = path.name.split("_")[2]                         # sanctions_list_v1_20261001.xml -> v1
        records = read_list(path)
        rows = [(version, i, *r, business_date, path.name, run_id, loaded_at) for i, r in enumerate(records, 1)]
        df = spark.createDataFrame(rows, LIST_RAW)
        tables.overwrite_partitions(df, "bronze", "sanctions_list_raw", ["list_version"])
        written = tables.read(spark, "bronze", "sanctions_list_raw").filter(F.col("list_version") == version).count()
    else:
        records = read_customers(path)
        rows = [(*r, source, business_date, path.name, i, run_id, loaded_at) for i, r in enumerate(records, 1)]
        df = spark.createDataFrame(rows, CUSTOMER_RAW)
        tables.overwrite_partitions(df, "bronze", "customer_raw", ["business_date", "source_system"])
        written = (tables.read(spark, "bronze", "customer_raw")
                   .filter((F.col("business_date") == business_date) & (F.col("source_system") == source))
                   .count())
    if written != len(rows):                                      # reconciliation: file rows = bronze rows
        raise RuntimeError(f"{path.name}: read {len(rows)} rows but bronze holds {written}")
    return len(rows)


def process_file(spark, path, business_date, run_id, last_status, log, counts):
    """Gate one file, then load, quarantine or leave it waiting. Appends to log and counts."""
    previous = last_status.get(path.name)
    if previous == "ACCEPTED":                    # already loaded on an earlier run: never load twice
        counts["SKIPPED"] += 1
        return
    source = path.name.split("_")[0]      # core / cards / sanctions
    algo, result = check_file(path)
    row_count, status, message = None, None, None
    if result == "MISSING":
        counts["WAITING"] += 1                # leave it in landing; the checksum may still arrive
        if previous != "WAITING":             # record it once, not on every re-run
            tables.append(spark.createDataFrame(
                [(path.name, source, business_date, now(), None, result, None, "WAITING", run_id)], MANIFEST),
                "ctl", "file_manifest")
        return
    elif result != "MATCH":
        status = "QUARANTINED"                # MISMATCH or UNDETERMINED: never load it
    else:
        stage_start = now()
        try:
            row_count = load_bronze(spark, path, source, business_date, run_id)
            status = "ACCEPTED"
        except BadFile as exc:                # intact but malformed: quarantine, don't load
            status, message = "QUARANTINED", str(exc)
        log.append((run_id, JOB, business_date, f"bronze_{source}",
                    "FAILED" if status != "ACCEPTED" else ("WARNING" if row_count == 0 else "SUCCESS"),
                    row_count, row_count, 0, stage_start, now(),
                    message or (f"zero rows received in {path.name}" if row_count == 0 else path.name)))
    tables.append(spark.createDataFrame(
        [(path.name, source, business_date, now(), algo, result, row_count, status, run_id)], MANIFEST),
        "ctl", "file_manifest")
    if status == "ACCEPTED":                  # move only after bronze and the manifest are written
        move(path, ARCHIVE, business_date)
    else:
        move(path, QUARANTINE, business_date)
    counts[status] += 1


def ingest_day(spark, business_date):
    run_id = f"R-{business_date:%Y%m%d}-{now():%H%M%S}"
    started = now()
    folder = LANDING / business_date.isoformat()
    manifest = tables.read(spark, "ctl", "file_manifest")
    last_status = {} if manifest is None else {   # latest status of every file seen before
        r.file_name: r.status for r in manifest.orderBy("received_at").select("file_name", "status").collect()}
    files = sorted(p for p in folder.glob("*") if p.suffix in (".tsv", ".xml")) if folder.exists() else []
    log, counts = [], {"ACCEPTED": 0, "QUARANTINED": 0, "WAITING": 0, "SKIPPED": 0}
    try:
        for path in files:
            process_file(spark, path, business_date, run_id, last_status, log, counts)
    except Exception as exc:                      # record the failure before stopping the run
        log.append((run_id, JOB, business_date, "ingest", "FAILED", None, None, None, started, now(), str(exc)[:500]))
        tables.append(spark.createDataFrame(log, RUN_LOG), "ctl", "run_log")
        raise

    gate_status = "WARNING" if counts["QUARANTINED"] or counts["WAITING"] else "SUCCESS"
    log.insert(0, (run_id, JOB, business_date, "checksum_gate", gate_status, len(files),
                   counts["ACCEPTED"], counts["QUARANTINED"], started, now(),
                   ", ".join(f"{k.lower()}={v}" for k, v in counts.items())))
    tables.append(spark.createDataFrame(log, RUN_LOG), "ctl", "run_log")
    print(f"{business_date}  {run_id}  " + "  ".join(f"{k.lower()}={v}" for k, v in counts.items()))
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--date", type=date.fromisoformat, help="business date, e.g. 2026-10-01")
    group.add_argument("--all", action="store_true", help="every day folder in landing, oldest first")
    args = parser.parse_args()
    days = [args.date] if args.date else sorted(date.fromisoformat(p.name) for p in LANDING.iterdir() if p.is_dir())
    spark = get_spark(JOB)
    try:
        for d in days:
            ingest_day(spark, d)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())