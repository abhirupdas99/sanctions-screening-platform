"""Simulated analysts for one business date. Level 1 reviews every new alert; clear false positives are closed,
likely matches are escalated. Level 2 (a different person: 4-eyes) decides escalations the next business day.
Every action is an append-only row in alert_audit_log; alert_snapshot only holds the current status."""
from datetime import timedelta

from src.common import oracle
from src.firco.engine import at
from src.firco.matching import STRONG, dob_compatible

LEVEL_1, LEVEL_2 = 10, 20        # profile_ids from fof_audit_profile


def analysts(cur, profile_id):
    """Active users holding a profile. Inactive users never act, even if still mapped (Task 5 finds those)."""
    cur.execute("SELECT u.username FROM aml_fff.fff_users u JOIN aml_fff.fff_user_profile p ON p.user_id = u.id"
                " WHERE p.profile_id = :p AND u.active = 'Y' ORDER BY u.username", {"p": profile_id})
    return [r[0] for r in cur.fetchall()]


def log(cur, alert_id, ts, actor_type, actor, action, decision, comment):
    (event_id,) = oracle.next_audit_ids(cur, 1)
    cur.execute("INSERT INTO aml_fff.alert_audit_log (audit_event_id, alert_id, event_timestamp, actor_type,"
                " actor_id, action_code, decision_code, comments) VALUES (:i, :a, :t, :at, :u, :ac, :d, :c)",
                {"i": event_id, "a": alert_id, "t": ts, "at": actor_type, "u": actor, "ac": action,
                 "d": decision, "c": comment})


def set_status(cur, alert_id, status, ts):
    cur.execute("UPDATE aml_fff.alert_snapshot SET alert_status = :s, snapshot_updated_at = :t WHERE alert_id = :a",
                {"s": status, "t": ts, "a": alert_id})


def review(con, day):
    cur = con.cursor()
    level_1, level_2 = analysts(cur, LEVEL_1), analysts(cur, LEVEL_2)
    cleared = escalated = confirmed = 0

    # Level 1: every OPEN alert raised up to today
    cur.execute("SELECT s.alert_id, h.match_score, h.list_dob, i.date_of_birth FROM aml_fff.alert_snapshot s"
                " JOIN aml_fff.alert_hit h ON h.alert_id = s.alert_id"
                " JOIN aml_fff.screening_input i ON i.batch_id = h.batch_id AND i.subject_key = h.subject_key"
                " WHERE s.alert_status = 'OPEN' AND h.created_at < :day_end ORDER BY s.alert_id",
                {"day_end": at(day, 23, 59)})
    for n, (alert_id, match_score, list_dob, customer_dob) in enumerate(cur.fetchall()):
        user, ts = level_1[n % len(level_1)], at(day, 9, 7 * n)
        if float(match_score) >= STRONG and dob_compatible(customer_dob, list_dob):
            log(cur, alert_id, ts, "USER", user, "REVIEW_DECISION", "POTENTIAL_MATCH",
                "Name and date of birth consistent with the list entry; escalating")
            set_status(cur, alert_id, "IN_REVIEW", ts)
            log(cur, alert_id, ts + timedelta(seconds=30), "WORKFLOW", "FIRCO_ENGINE", "ESCALATION", "ESCALATED",
                "Routed to Level 2")
            escalated += 1
        else:
            log(cur, alert_id, ts, "USER", user, "REVIEW_DECISION", "FALSE_POSITIVE",
                "Different person: name only loosely similar or date of birth differs")
            set_status(cur, alert_id, "CLOSED", ts)
            cleared += 1

    # Level 2: escalations from earlier days, decided by someone other than the Level 1 reviewer
    cur.execute("SELECT s.alert_id, MAX(CASE WHEN l.action_code = 'REVIEW_DECISION' THEN l.actor_id END)"
                " FROM aml_fff.alert_snapshot s JOIN aml_fff.alert_audit_log l ON l.alert_id = s.alert_id"
                " WHERE s.alert_status = 'IN_REVIEW' GROUP BY s.alert_id"
                " HAVING MAX(CASE WHEN l.action_code = 'ESCALATION' THEN l.event_timestamp END) < :day_start"
                " ORDER BY s.alert_id", {"day_start": at(day, 0)})
    for n, (alert_id, level_1_user) in enumerate(cur.fetchall()):
        pool = [u for u in level_2 if u != level_1_user]          # 4-eyes: never the Level 1 reviewer
        user = pool[n % len(pool)]
        ts = at(day, 10, 11 * n)
        log(cur, alert_id, ts, "USER", user, "COMMENT", None, "Checked date of birth and country against the list entry")
        log(cur, alert_id, ts + timedelta(minutes=4), "USER", user, "REVIEW_DECISION", "CONFIRMED_MATCH",
            "Confirmed: report to FINTRAC and RCMP/CSIS per procedure")
        set_status(cur, alert_id, "CLOSED", ts)
        confirmed += 1
    con.commit()
    return {"cleared": cleared, "escalated": escalated, "confirmed": confirmed}