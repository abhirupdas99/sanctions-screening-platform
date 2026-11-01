"""Phase 3 acceptance check: the gate's decisions and bronze must match the answer key exactly."""
import csv
import sys
from datetime import date

from pyspark.sql import functions as F

from src.common import tables
from src.common.config import DATA
from src.common.spark import get_spark
from src.jobs.ingest_day import ingest_day

LANDING, QUARANTINE = DATA / "landing", DATA / "quarantine"
STATUS_FOR = {"MATCH": "ACCEPTED", "MISMATCH": "QUARANTINED", "UNDETERMINED": "QUARANTINED", "MISSING": "WAITING"}

with open(DATA / "answer_key" / "files.csv", encoding="utf-8") as fh:
    EXPECTED = list(csv.DictReader(fh))


def checks(spark):
    manifest = tables.read(spark, "ctl", "file_manifest")
    bronze = tables.read(spark, "bronze", "customer_raw")
    run_log = tables.read(spark, "ctl", "run_log")
    lists = tables.read(spark, "bronze", "sanctions_list_raw")
    got = {r.file_name: r for r in manifest.collect()}

    def gate_matches_answer_key():
        wrong = [f"{e['file_name']}: expected {e['expected_result']}, got "
                 f"{got[e['file_name']].checksum_result if e['file_name'] in got else 'nothing'}"
                 for e in EXPECTED
                 if e["file_name"] not in got or got[e["file_name"]].checksum_result != e["expected_result"]
                 or got[e["file_name"]].status != STATUS_FOR[e["expected_result"]]]
        assert not wrong, wrong[0]
        by = {s: sum(1 for r in got.values() if r.status == s) for s in ("ACCEPTED", "QUARANTINED", "WAITING")}
        return f"{len(EXPECTED)} files: " + ", ".join(f"{v} {k.lower()}" for k, v in by.items())

    def one_manifest_row_per_file():
        assert manifest.count() == len(EXPECTED), f"{manifest.count()} manifest rows for {len(EXPECTED)} files"
        return "no duplicates after re-runs"

    def bronze_rows_reconcile():
        counts = {r.file_name: r["count"] for r in bronze.groupBy("file_name").count().collect()}
        for e in EXPECTED:
            if e["source"] != "sanctions_list" and STATUS_FOR[e["expected_result"]] == "ACCEPTED":
                assert counts.get(e["file_name"], 0) == int(e["data_rows"]), \
                    f"{e['file_name']}: bronze {counts.get(e['file_name'], 0)}, file {e['data_rows']}"
        return f"{bronze.count()} customer rows, every accepted file reconciles"

    def bronze_keeps_raw_values():
        padded = bronze.filter(F.col("name") != F.trim(F.col("name"))).count()
        bad_id = bronze.filter(F.col("record_id") == "ABC123").count()
        assert padded > 0 and bad_id > 0, "bronze was cleaned - it must hold values exactly as received"
        return f"{padded} untrimmed names and the ABC123 rows kept as received"

    def no_carriage_returns():
        cols = ["record_id", "name", "country_code", "position_held", "company_name", "date_of_birth"]
        bad = bronze.filter(F.concat_ws("", *cols).contains("\r")).count()
        assert bad == 0, f"{bad} rows still contain \\r"
        return "CRLF file loaded without \\r"

    def files_in_the_right_place():
        quarantined = [p.name for p in QUARANTINE.rglob("*") if p.suffix in (".tsv", ".xml")]
        waiting = [p.name for p in LANDING.rglob("*") if p.is_file()]
        assert len(quarantined) == 3, f"quarantine holds {quarantined}"
        assert len(waiting) == 1, f"landing still holds {waiting}"
        return f"3 files quarantined; landing holds only {waiting[0]}"

    def zero_row_file_flagged():
        warn = run_log.filter((F.col("status") == "WARNING") & F.col("message").contains("zero rows")).count()
        assert warn >= 1, "the empty cards file was not flagged"
        return "empty file loaded and flagged WARNING in ctl.run_log"

    def list_versions_loaded():
        n = {r.list_version: r["count"] for r in lists.groupBy("list_version").count().collect()}
        assert set(n) == {"v1", "v2"} and n["v2"] - n["v1"] == 2, f"list counts {n}"
        return f"v1 {n['v1']} entries, v2 {n['v2']} (+2 newly listed)"

    return [gate_matches_answer_key, one_manifest_row_per_file, bronze_rows_reconcile, bronze_keeps_raw_values,
            no_carriage_returns, files_in_the_right_place, zero_row_file_flagged, list_versions_loaded]


def main() -> int:
    spark = get_spark("check-phase3")
    print("Re-running all 30 days first: an idempotent job must change nothing...")
    for day in sorted(date.fromisoformat(p.name) for p in LANDING.iterdir() if p.is_dir()):
        ingest_day(spark, day)
    all_checks = checks(spark)
    failed = 0
    for check in all_checks:
        try:
            print(f"PASS  {check.__name__:28} {check()}")
        except Exception as exc:  # report every failure, don't stop at the first
            failed += 1
            print(f"FAIL  {check.__name__:28} {str(exc).splitlines()[0]}")
    spark.stop()
    print(f"\n{len(all_checks) - failed}/{len(all_checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())