"""Phase 5 acceptance check: screening outcomes, 4-eyes, audit trail, extract-back and both reports."""
import csv
import sys
from datetime import date

from pyspark.sql import functions as F

from src.common import oracle, tables
from src.common.config import DATA
from src.common.spark import get_spark
from src.firco import engine
from src.jobs import run_firco
from src.jobs.extract_firco import extract

ANSWERS = DATA / "answer_key"
TASK2_SQL = """
SELECT TRUNC(event_timestamp, 'MM'), actor_type, action_code, decision_code,
       COUNT(*), COUNT(DISTINCT alert_id), COUNT(decision_code)
FROM aml_fff.alert_audit_log
GROUP BY TRUNC(event_timestamp, 'MM'), actor_type, action_code, decision_code"""   # the take-home Task 2 query
TRUE_MATCHES = {"A_exact": "BATCH", "B_typo": "BATCH", "C_new_customer": "BATCH",
                "D_listed_in_v2": "LIST_RESCREEN", "E_listed_in_v2": "LIST_RESCREEN"}


def seeds():
    with open(ANSWERS / "seeded_matches.csv", encoding="utf-8") as fh:
        return {r["label"]: r for r in csv.DictReader(fh)}


def q(cur, sql, binds=None):
    cur.execute(sql, binds or {})
    return cur.fetchall()


def checks(spark, cur, before, after, extract_counts):
    days = sorted(date.fromisoformat(p.name) for p in engine.OUTBOUND.iterdir())
    outcome = {r[0]: r for r in q(cur, """
        SELECT h.alert_id, h.subject_key, h.trigger_type, h.created_at, s.alert_status, s.source_record_id,
               (SELECT MAX(l.decision_code) FROM aml_fff.alert_audit_log l
                 WHERE l.alert_id = h.alert_id AND l.decision_code IN ('CONFIRMED_MATCH', 'FALSE_POSITIVE'))
        FROM aml_fff.alert_hit h JOIN aml_fff.alert_snapshot s ON s.alert_id = h.alert_id""")}

    def every_batch_loaded_once():
        loaded = dict(q(cur, "SELECT batch_id, COUNT(*) FROM aml_fff.screening_input GROUP BY batch_id"))
        for d in days:
            f = engine.OUTBOUND / d.isoformat() / f"screening_input_{d:%Y%m%d}.tsv"
            expected = sum(1 for _ in f.open(encoding="utf-8")) - 1
            assert loaded.get(f"SCR-{d:%Y%m%d}") == expected, f"{f.name}: {loaded.get(f'SCR-{d:%Y%m%d}')} loaded, file has {expected}"
        return f"{len(days)} batches loaded through the checksum gate, row counts match the files"

    def firco_rerun_changes_nothing():
        assert before == after, f"re-running all days changed Firco: {before} -> {after}"
        return f"re-running all {len(days)} days added no alerts or audit events ({after[1]} events)"

    def list_versions():
        n = dict(q(cur, "SELECT list_version, COUNT(*) FROM aml_fff.sanctions_list_entry GROUP BY list_version"))
        assert set(n) == {"v1", "v2"} and n["v2"] - n["v1"] == 2, f"list versions {n}"
        return f"v1 {n['v1']} entries, v2 {n['v2']}: 2 newly listed"

    def true_matches_confirmed():
        found = []
        for label, trigger in TRUE_MATCHES.items():
            s = seeds()[label]
            ids = s["record_ids"].split()
            hits = [o for o in outcome.values() if o[5] in ids and o[6] == "CONFIRMED_MATCH"]
            assert hits, f"{label} ({s['customer_name']}) was not confirmed"
            assert hits[0][2] == trigger, f"{label} caught by {hits[0][2]}, expected {trigger}"
            first = date.fromisoformat(s["first_business_date"]) if trigger == "BATCH" else days[14]
            assert str(hits[0][3])[:10] == first.isoformat(), f"{label} alerted {str(hits[0][3])[:10]}, expected {first}"
            found.append(label.split("_")[0])
        return f"all 5 true matches confirmed ({', '.join(found)}); D and E by the list-change rescreen on {days[14]}"

    def near_misses_never_confirmed():
        alerted = 0
        for label, s in seeds().items():
            if "near_miss" in label:
                hits = [o for o in outcome.values() if o[5] in s["record_ids"].split()]
                assert all(o[6] == "FALSE_POSITIVE" for o in hits), f"{label} was confirmed"
                alerted += bool(hits)
        cleared = sum(1 for o in outcome.values() if o[6] == "FALSE_POSITIVE")
        return f"{alerted} of 3 near misses alerted and cleared; {cleared} false positives cleared in total"

    def four_eyes_on_every_match():
        rows = q(cur, """SELECT alert_id, actor_id, decision_code FROM aml_fff.alert_audit_log
                         WHERE action_code = 'REVIEW_DECISION' ORDER BY audit_event_id""")
        by_alert = {}
        for alert_id, actor, decision in rows:
            by_alert.setdefault(alert_id, []).append((actor, decision))
        confirmed = [a for a, ds in by_alert.items() if ds[-1][1] == "CONFIRMED_MATCH"]
        for a in confirmed:
            assert len({actor for actor, _ in by_alert[a]}) == 2, f"{a}: confirmed without two different people"
        inactive = {r[0] for r in q(cur, "SELECT username FROM aml_fff.fff_users WHERE active = 'N'")}
        acted = {actor for actor, _, _ in rows}
        assert not acted & inactive, f"inactive users acted: {acted & inactive}"
        return f"{len(confirmed)} confirmations, each by a different person from Level 1; inactive users never acted"

    def audit_trail_consistent():
        ids = [r[0] for r in q(cur, "SELECT audit_event_id FROM aml_fff.alert_audit_log ORDER BY event_timestamp, audit_event_id")]
        assert len(ids) == len(set(ids)), "duplicate audit_event_id"
        bad = [a for a, o in outcome.items() if (o[4] == "CLOSED") != (o[6] is not None)]
        assert not bad, f"status and final decision disagree for {bad[:3]}"
        open_now = sum(1 for o in outcome.values() if o[4] != "CLOSED")
        return f"{len(ids)} events, unique IDs; every CLOSED alert has a final decision ({open_now} still in review)"

    def extract_back_reconciles():
        in_oracle = q(cur, "SELECT COUNT(*) FROM aml_fff.alert_audit_log")[0][0]
        in_bronze = tables.read(spark, "bronze", "firco_alert_audit_log").count()
        assert in_oracle == in_bronze == extract_counts[1], f"Oracle {in_oracle}, bronze {in_bronze}"
        assert extract_counts[0] == extract_counts[1], "a second extract duplicated audit events"
        return f"Oracle {in_oracle} = bronze {in_bronze}; a repeat extract added nothing (watermark)"

    def task2_report_matches_oracle():
        gold = {(r.activity_month.isoformat()[:7], r.actor_type, r.action_code, r.decision_code,
                 r.event_count, r.distinct_alert_count, r.decision_event_count)
                for r in tables.read(spark, "gold", "alert_activity_monthly").collect()}
        ora = {(str(r[0])[:7], *r[1:4], *map(int, r[4:])) for r in q(cur, TASK2_SQL)}
        assert gold == ora, f"gold and Oracle differ: {sorted(gold ^ ora)[:3]}"
        return f"gold.alert_activity_monthly = the Task 2 query on Oracle ({len(gold)} rows)"

    def task5_flags_planted_findings():
        a = tables.read(spark, "gold", "user_access_review")
        a = {(r.username, r.profile_id): r for r in
             a.filter(F.col("extract_date") == a.agg(F.max("extract_date")).first()[0]).collect()}
        assert "inactive" in a[("linda.gauthier", 10)].review_flags, "inactive user with access not flagged"
        assert "administrator" in a[("samuel.okafor", 30)].review_flags, "segregation-of-duties conflict not flagged"
        assert a[("kevin.brooks", None)].mapping_status == "NO_PROFILE_MAPPING"
        assert a[("noah.patel", 99)].mapping_status == "PROFILE_NOT_IN_AUDIT_TABLE"
        renamed = a[("priya.nair", 20)]
        assert renamed.profile_name == "Level 2 Investigator" and renamed.profile_name_versions == 2, "renamed profile"
        return "inactive access, admin+analyst conflict, no mapping, unnamed profile, and the renamed profile all reported"

    return [every_batch_loaded_once, firco_rerun_changes_nothing, list_versions, true_matches_confirmed,
            near_misses_never_confirmed, four_eyes_on_every_match, audit_trail_consistent, extract_back_reconciles,
            task2_report_matches_oracle, task5_flags_planted_findings]


def firco_counts(cur):
    return tuple(q(cur, f"SELECT COUNT(*) FROM aml_fff.{t}")[0][0] for t in ("alert_snapshot", "alert_audit_log"))


def main() -> int:
    spark = get_spark("check-phase5")
    days = sorted(date.fromisoformat(p.name) for p in engine.OUTBOUND.iterdir())
    with oracle.connect("firco_app") as con:
        before = firco_counts(con.cursor())
        print("Re-running Firco for every day first: an idempotent engine must change nothing...")
        for d in days:
            run_firco.run(d, con)
        after = firco_counts(con.cursor())
    extract(spark, days[-1])
    first = tables.read(spark, "bronze", "firco_alert_audit_log").count()
    extract(spark, days[-1])
    second = tables.read(spark, "bronze", "firco_alert_audit_log").count()
    with oracle.connect("firco_app") as con:      # the check reads engine tables LH_EXTRACT can't see
        all_checks = checks(spark, con.cursor(), before, after, (first, second))
        failed = 0
        for check in all_checks:
            try:
                print(f"PASS  {check.__name__:30} {check()}")
            except Exception as exc:  # report every failure, don't stop at the first
                failed += 1
                print(f"FAIL  {check.__name__:30} {str(exc).splitlines()[0]}")
    spark.stop()
    print(f"\n{len(all_checks) - failed}/{len(all_checks)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
    