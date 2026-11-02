"""The mock Firco engine for one business date: load list versions, load the day's batch through the checksum
gate, screen it, re-screen everyone when a new list version arrives, and raise alerts with a full audit trail."""
import csv
import hashlib
from datetime import datetime, time, timedelta

from src.common import oracle
from src.common.config import DATA
from src.firco.matching import ListIndex
from src.ingest.checksum_gate import check_file
from src.jobs.ingest_day import LIST_FIELDS, read_list

ARCHIVE, OUTBOUND = DATA / "archive", DATA / "outbound" / "firco"
ENGINE = "FIRCO_ENGINE"


def at(day, hour, minute=0):
    return datetime.combine(day, time(hour)) + timedelta(minutes=minute)


# ---------------------------------------------------------------------------- list versions
def list_files(up_to):
    """Archived list files (accepted by the Phase 3 gate) published on or before up_to: [(date, version, path)]."""
    found = []
    for p in ARCHIVE.glob("*/sanctions_list_v*_*.xml"):
        _, _, version, stamp = p.stem.split("_")
        published = datetime.strptime(stamp, "%Y%m%d").date()
        if published <= up_to:
            found.append((published, version, p))
    return sorted(found)


def entries_from_xml(path):
    entries = {}
    for values in read_list(path):
        r = dict(zip(LIST_FIELDS, values))
        name = " ".join(x for x in (r["given_name"], r["last_name"]) if x) or r["entity_name"] or ""
        if not name:
            continue
        ident = "|".join(r[k] or "" for k in ("country", "schedule", "item", "last_name", "given_name", "entity_name"))
        entry_id = hashlib.sha1(ident.encode()).hexdigest()[:16].upper()   # stable across list versions
        entries[entry_id] = {"entry_id": entry_id, "full_name": name, "aliases": r["aliases"],
                             "regulation": r["country"], "date_of_birth": r["date_of_birth"],
                             "listed_on": r["date_of_listing"]}
    return list(entries.values())


def load_list_versions(con, day):
    """Load any list version not yet in Oracle. Returns (current_version, previous_version_if_new_today)."""
    cur = con.cursor()
    cur.execute("SELECT DISTINCT list_version FROM aml_fff.sanctions_list_entry")
    loaded = {r[0] for r in cur.fetchall()}
    files = list_files(day)
    for _, version, path in files:
        if version not in loaded:
            rows = entries_from_xml(path)
            cur.executemany(
                "INSERT INTO aml_fff.sanctions_list_entry (list_version, entry_id, entity_type, full_name, aliases,"
                " regulation, date_of_birth, listed_on) VALUES (:list_version, :entry_id, 'PERSON', :full_name,"
                " :aliases, :regulation, :date_of_birth, NULL)",
                [{**{k: r[k] for k in ("entry_id", "full_name", "aliases", "regulation", "date_of_birth")},
                  "list_version": version} for r in rows])
            con.commit()
    if not files:
        raise RuntimeError(f"no sanctions list available on {day}")
    current = files[-1]
    previous = files[-2][1] if len(files) > 1 and current[0] == day else None
    return current[1], previous


def list_entries(con, version, only_new_since=None):
    cur = con.cursor()
    cur.execute("SELECT entry_id, full_name, aliases, regulation, date_of_birth FROM aml_fff.sanctions_list_entry"
                " WHERE list_version = :v", {"v": version})
    rows = [{"entry_id": r[0], "full_name": r[1], "aliases": r[2], "regulation": r[3], "date_of_birth": r[4]}
            for r in cur.fetchall()]
    if only_new_since:
        cur.execute("SELECT entry_id FROM aml_fff.sanctions_list_entry WHERE list_version = :v", {"v": only_new_since})
        old = {r[0] for r in cur.fetchall()}
        rows = [r for r in rows if r["entry_id"] not in old]
    for r in rows:
        r["names"] = [r["full_name"]] + [a.strip() for a in (r["aliases"] or "").split(";") if a.strip()]
    return rows


# ---------------------------------------------------------------------------- the day's batch
def load_batch(con, day):
    """Load data/outbound/firco/<day>/screening_input_<day>.tsv once, only if its checksum matches."""
    batch_id = f"SCR-{day:%Y%m%d}"
    cur = con.cursor()
    cur.execute("SELECT COUNT(*) FROM aml_fff.screening_input WHERE batch_id = :b", {"b": batch_id})
    if cur.fetchone()[0] == 0:
        path = OUTBOUND / day.isoformat() / f"screening_input_{day:%Y%m%d}.tsv"
        algo, result = check_file(path)
        if result != "MATCH":
            raise RuntimeError(f"{path.name}: checksum {result} - batch not loaded")
        with path.open(encoding="utf-8", newline="") as fh:
            rows = list(csv.DictReader(fh, delimiter="\t"))
        cur.executemany(
            "INSERT INTO aml_fff.screening_input (batch_id, subject_key, source_record_id, subject_name,"
            " country_code, date_of_birth, change_type) VALUES (:batch_id, :subject_key, :source_record_id,"
            " :subject_name, :country_code, :date_of_birth, :change_type)",
            [{k: (r[k] or None) for k in ("batch_id", "subject_key", "source_record_id", "subject_name",
                                           "country_code", "date_of_birth", "change_type")} for r in rows])
        con.commit()
    return batch_id


def subjects(con, batch_id=None, up_to_batch=None):
    """People to screen: one batch, or (for a list-change rescreen) the latest version of everyone seen so far."""
    cur = con.cursor()
    if batch_id:
        cur.execute("SELECT subject_key, source_record_id, subject_name, country_code, date_of_birth, batch_id"
                    " FROM aml_fff.screening_input WHERE batch_id = :b ORDER BY subject_key", {"b": batch_id})
    else:
        cur.execute("SELECT subject_key, source_record_id, subject_name, country_code, date_of_birth, batch_id"
                    " FROM aml_fff.screening_input WHERE batch_id <= :b ORDER BY batch_id", {"b": up_to_batch})
    latest = {}
    for r in cur.fetchall():
        latest[r[0]] = {"subject_key": r[0], "source_record_id": r[1], "subject_name": r[2],
                        "country_code": r[3], "date_of_birth": r[4], "batch_id": r[5]}
    return [latest[k] for k in sorted(latest)]


# ---------------------------------------------------------------------------- screening and alerts
def screen(con, day, people, entries, trigger, version):
    """Raise one alert per person whose best hit hasn't been alerted before. Returns alerts raised."""
    cur = con.cursor()
    cur.execute("SELECT subject_key, list_entry_id FROM aml_fff.alert_hit")
    already = {(r[0], r[1]) for r in cur.fetchall()}
    cur.execute("SELECT COUNT(*) FROM aml_fff.alert_hit WHERE alert_id LIKE :p", {"p": f"ALT-{day:%Y%m%d}-%"})
    n = cur.fetchone()[0]
    index = ListIndex(entries)
    raised = 0
    for person in people:
        hit = index.best_hit(person["subject_name"])
        if hit is None or (person["subject_key"], hit[0]["entry_id"]) in already:
            continue                                  # no hit, or this exact hit was already alerted
        entry, match_score, hit_count = hit
        n += 1
        raised += 1
        alert_id = f"ALT-{day:%Y%m%d}-{n:04d}"
        created = at(day, 6 if trigger == "BATCH" else 7, n)
        cur.execute("INSERT INTO aml_fff.alert_snapshot (alert_id, source_record_id, subject_name, country_code,"
                    " vendor_alert_ref, alert_status, snapshot_updated_at) VALUES (:a, :r, :n, :c, :v, 'OPEN', :t)",
                    {"a": alert_id, "r": person["source_record_id"], "n": person["subject_name"],
                     "c": person["country_code"], "v": f"FIRCO-{day:%Y%m%d}{n:04d}", "t": created})
        cur.execute("INSERT INTO aml_fff.alert_hit (alert_id, subject_key, batch_id, trigger_type, list_version,"
                    " list_entry_id, list_name, list_country, list_dob, match_score, hit_count, created_at)"
                    " VALUES (:a, :s, :b, :tr, :v, :e, :ln, :lc, :ld, :ms, :hc, :t)",
                    {"a": alert_id, "s": person["subject_key"], "b": person["batch_id"], "tr": trigger,
                     "v": version, "e": entry["entry_id"], "ln": entry["full_name"], "lc": entry["regulation"],
                     "ld": entry["date_of_birth"], "ms": match_score, "hc": hit_count, "t": created})
        (event_id,) = oracle.next_audit_ids(cur, 1)
        cur.execute("INSERT INTO aml_fff.alert_audit_log (audit_event_id, alert_id, event_timestamp, actor_type,"
                    " actor_id, action_code, decision_code, comments) VALUES (:i, :a, :t, 'WORKFLOW', :u,"
                    " 'INITIAL_REVIEW', 'POTENTIAL_MATCH', :c)",
                    {"i": event_id, "a": alert_id, "t": created, "u": ENGINE,
                     "c": f"{trigger}: best hit {entry['full_name']} ({match_score}), {hit_count} entries hit"})
        already.add((person["subject_key"], entry["entry_id"]))
    con.commit()
    return raised


def run_day(con, day):
    """Everything Firco does for one business date, before the analysts start work."""
    version, replaced = load_list_versions(con, day)
    batch_id = load_batch(con, day)
    raised = screen(con, day, subjects(con, batch_id=batch_id), list_entries(con, version), "BATCH", version)
    rescreened = 0
    if replaced:                                      # new list version today: everyone vs the new names only
        new_entries = list_entries(con, version, only_new_since=replaced)
        rescreened = screen(con, day, subjects(con, up_to_batch=batch_id), new_entries, "LIST_RESCREEN", version)
    return {"batch": batch_id, "list": version, "alerts": raised, "rescreen_alerts": rescreened,
            "new_list": bool(replaced)}