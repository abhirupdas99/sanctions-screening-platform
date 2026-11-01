"""Phase 4 acceptance check: silver and the screening extract against the Phase 2 answer key."""
import csv
import sys
from collections import defaultdict
from datetime import date

from pyspark.sql import functions as F

from src.common import tables
from src.common.config import DATA
from src.common.spark import get_spark
from src.ingest.checksum_gate import check_file
from src.jobs.build_silver import OUTBOUND, build_day, usable_extracts
from src.silver.rules import COMPARED

ANSWERS = DATA / "answer_key"
EXPECTED_STALE = {("2026-10-07", "core"), ("2026-10-13", "cards"), ("2026-10-28", "cards"),
                  ("2026-11-02", "core"), ("2026-11-06", "cards")}


def answer(name):
    with open(ANSWERS / name, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def fingerprint(spark):
    """Per-day counts of every outcome: identical before and after a re-run means the job is idempotent."""
    res = tables.read(spark, "silver", "customer_resolved").groupBy("business_date", "status").count()
    chg = tables.read(spark, "silver", "customer_change").groupBy("business_date", "change_type").count()
    ext = tables.read(spark, "gold", "screening_extract").groupBy("business_date").count()
    return sorted(map(tuple, res.collect() + chg.collect() + ext.collect()), key=str)


def checks(spark, days, before, after):
    resolved = tables.read(spark, "silver", "customer_resolved")
    change = tables.read(spark, "silver", "customer_change")
    extract = tables.read(spark, "gold", "screening_extract")
    run_log = tables.read(spark, "ctl", "run_log").filter(F.col("job_name") == "build_silver")
    extracts = usable_extracts(spark)

    def every_day_built_and_handed_off():
        built = {r.business_date for r in resolved.select("business_date").distinct().collect()}
        assert built == set(days), f"{len(built)} days built, expected {len(days)}"
        for d in days:
            f = OUTBOUND / d.isoformat() / f"screening_input_{d:%Y%m%d}.tsv"
            assert f.exists() and check_file(f)[1] == "MATCH", f"{f.name} missing or its checksum fails"
        latest = run_log.groupBy("business_date").agg(F.max("run_id").alias("run_id"))   # ignore older attempts
        failed = run_log.join(latest, ["business_date", "run_id"]).filter("status = 'FAILED'").count()
        assert failed == 0, "the latest build_silver run FAILED on some day (see ctl.run_log)"
        return f"{len(days)} days in silver; {len(days)} extract files with valid SHA3-256 checksums"

    def idempotent_rerun():
        assert before == after, "re-running all days changed the results"
        return "re-running all 30 days changed nothing"

    def invalid_rows_match_answer_key():
        key = {r["record_id"]: r for r in answer("invalid_rows.csv")}
        first = resolved.filter((F.col("business_date") == days[0]) & (F.col("status") == "INVALID")).collect()
        got = {r.record_id: r.validation_error for r in first}
        assert set(got) == set(key), f"day 1 INVALID ids {sorted(got)} vs answer key {sorted(key)}"
        for rid, why in got.items():
            assert key[rid]["reason"] in why, f"{rid or '(blank)'}: '{why}' should include '{key[rid]['reason']}'"
        fixed = [rid for rid, r in key.items() if r["fixed_on"]]
        last = {r.record_id for r in resolved.filter((F.col("business_date") == days[-1])
                                                      & (F.col("status") == "INVALID")).collect()}
        assert not set(fixed) & last, f"{fixed} should be valid after being fixed"
        return f"6 INVALID on day 1 with the right reasons; {', '.join(fixed)} valid after its fix"

    def one_parent_with_lowest_id():
        valid = resolved.filter("status != 'INVALID'")
        bad = (valid.groupBy("business_date", "group_key")
               .agg(F.sum(F.when(F.col("status") == "NOT_DUPLICATE", 1).otherwise(0)).alias("parents"),
                    F.min("sid_num").alias("lowest"),
                    F.max(F.when(F.col("status") == "NOT_DUPLICATE", F.col("sid_num"))).alias("parent_id"))
               .filter("parents != 1 OR parent_id != lowest").count())
        assert bad == 0, f"{bad} groups without exactly one parent holding the lowest ID"
        dangling = (valid.filter("status = 'DUPLICATE'").alias("d")
                    .join(valid.filter("status = 'NOT_DUPLICATE'")
                          .select("business_date", F.concat_ws(":", "source_system", "record_id").alias("duplicate_of")),
                          ["business_date", "duplicate_of"], "left_anti").count())
        assert dangling == 0, f"{dangling} duplicates point to a row that isn't a parent"
        return "every group has one parent with its lowest ID; every duplicate points to it"

    def cross_source_duplicate_grouped():
        a = next(r for r in answer("seeded_matches.csv") if r["label"] == "A_exact")
        ids = a["record_ids"].split()
        rows = resolved.filter((F.col("business_date") == days[0]) & F.col("record_id").isin(ids)).collect()
        assert {r.source_system for r in rows} == {"core", "cards"} and len({r.group_key for r in rows}) == 1, \
            f"seeded customer A's core and cards records are not one person: {[(r.source_system, r.status) for r in rows]}"
        parent = [r for r in rows if r.status == "NOT_DUPLICATE"]
        assert len(parent) == 1 and parent[0].source_system == "core", "core record should be the parent"
        variants = (resolved.filter((F.col("business_date") == days[0]) & F.col("group_key").isNotNull())
                    .groupBy("group_key").agg(F.countDistinct("name").alias("spellings"))
                    .filter("spellings > 1").count())
        assert variants > 0, "no case/spacing variants were grouped"
        return f"customer A's core + cards records grouped, core is parent; {variants} groups merge spelling variants"

    def changes_match_answer_key():
        fresh = {(d, s) for s, ds in extracts.items() for d in ds}
        events = defaultdict(lambda: defaultdict(set))
        for e in answer("events.csv"):
            kind = {"FIXED": "NEW", "UPDATE": "UPDATES", "DELETE": "DELETED"}.get(e["event"], e["event"])
            events[(date.fromisoformat(e["business_date"]), e["source"])][kind].add(e["record_id"])
        compared, missed_updates = 0, 0
        got, ambiguous = defaultdict(lambda: defaultdict(set)), defaultdict(set)
        for r in change.collect():
            if "ambiguous_record_id" in r.changed_columns:      # non-unique IDs: re-screened, not compared
                ambiguous[(r.business_date, r.source_system)].add(r.record_id)
            else:
                got[(r.business_date, r.source_system)][r.change_type].add(r.record_id)
        for i, d in enumerate(days[1:], 1):
            for s in ("core", "cards"):
                if (d, s) not in fresh or (days[i - 1], s) not in fresh:
                    continue                       # stale on either day: changes are deferred, not comparable
                skip = ambiguous[(d, s)] | ambiguous[(days[i - 1], s)]
                exp = {k: v - skip for k, v in events[(d, s)].items()}
                exp = defaultdict(set, exp)
                act = got[(d, s)]
                for kind in ("NEW", "DELETED"):
                    assert act[kind] == exp[kind], f"{d} {s} {kind}: got {sorted(act[kind])}, expected {sorted(exp[kind])}"
                assert act["UPDATES"] <= exp["UPDATES"], f"{d} {s}: UPDATES not in answer key {act['UPDATES'] - exp['UPDATES']}"
                for rid in exp["UPDATES"] - act["UPDATES"]:     # an update we didn't report must be a no-op
                    vals = (resolved.filter((F.col("source_system") == s) & (F.col("record_id") == rid)
                                            & F.col("business_date").isin(d, days[i - 1]))
                            .select(*COMPARED).distinct().count())
                    assert vals == 1, f"{d} {s} {rid}: values changed but no UPDATES was reported"
                    missed_updates += 1
                compared += 1
        return (f"NEW and DELETED exact on {compared} day/source pairs; no false UPDATES "
                f"({missed_updates} answer-key updates verified as no-ops: the value was re-set to itself)")

    def missing_sources_carried_forward():
        warn = {(r.business_date.isoformat(), s.split(":")[0].strip())
                for r in run_log.filter("stage = 'source_freshness'").collect()
                for s in r.message.split(";")}
        assert warn == EXPECTED_STALE, f"carried forward {sorted(warn)}, expected {sorted(EXPECTED_STALE)}"
        worst = change.filter("change_type = 'DELETED'").groupBy("business_date").count().agg(F.max("count")).first()[0]
        assert worst < 20, f"{worst} DELETED in one day: a missing file was read as customers leaving"
        return f"5 stale source-days carried forward; at most {worst} DELETED on any day"

    def extract_screens_people_once():
        seeds = {r["label"]: r for r in answer("seeded_matches.csv")}
        c = seeds["C_new_customer"]
        hit = extract.filter((F.col("business_date") == date.fromisoformat(c["first_business_date"]))
                             & F.col("source_record_id").isin(c["record_ids"].split())).collect()
        assert len(hit) == 1 and hit[0].change_type == "NEW", f"customer C not sent as NEW on {c['first_business_date']}"
        twice = extract.groupBy("business_date", "subject_key").count().filter("count > 1").count()
        assert twice == 0, f"{twice} people sent twice on the same day"
        not_parent = (extract.join(resolved.filter("status = 'NOT_DUPLICATE'")
                                   .select("business_date", "source_system", F.trim("record_id").alias("source_record_id")),
                                   ["business_date", "source_system", "source_record_id"], "left_anti").count())
        assert not_parent == 0, f"{not_parent} extract rows are not parents"
        first = extract.filter(F.col("business_date") == days[0]).count()
        later = extract.filter(F.col("business_date") > days[0]).groupBy("business_date").count().agg(F.max("count")).first()[0]
        return f"day 1 screens all {first} people, then at most {later} a day; customer C sent as NEW on arrival"

    return [every_day_built_and_handed_off, idempotent_rerun, invalid_rows_match_answer_key, one_parent_with_lowest_id,
            cross_source_duplicate_grouped, changes_match_answer_key, missing_sources_carried_forward,
            extract_screens_people_once]


def main() -> int:
    spark = get_spark("check-phase4")
    days = sorted(r.business_date for r in
                  tables.read(spark, "silver", "customer_resolved").select("business_date").distinct().collect())
    before = fingerprint(spark)
    print("Re-running all 30 days first: an idempotent job must change nothing...")
    extracts = usable_extracts(spark)
    for i, d in enumerate(days):
        build_day(spark, d, days[i - 1] if i else None, extracts)
    after = fingerprint(spark)
    all_checks = checks(spark, days, before, after)
    failed = 0
    for check in all_checks:
        try:
            print(f"PASS  {check.__name__:33} {check()}")
        except Exception as exc:  # report every failure, don't stop at the first
            failed += 1
            print(f"FAIL  {check.__name__:33} {str(exc).splitlines()[0]}")
    spark.stop()
    print(f"\n{len(all_checks) - failed}/{len(all_checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())