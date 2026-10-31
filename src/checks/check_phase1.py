"""Phase 1 acceptance check. Prints PASS/FAIL per control and exits 1 if anything failed."""
import subprocess
import sys

import oracledb

from src.common.config import LAKEHOUSE, ORACLE_DSN, PROJECT_ROOT, oracle_password

# Oracle error codes that mean "you are not allowed to do that"
DENIED = {942, 1031, 41900}  # table not visible / insufficient privileges / missing privilege (23ai)


def connect(user: str):
    return oracledb.connect(user=user, password=oracle_password(user), dsn=ORACLE_DSN)


def must_be_denied(cur, sql: str) -> None:
    try:
        cur.execute(sql)
    except oracledb.DatabaseError as exc:
        if exc.args[0].code in DENIED:
            return
        raise
    raise AssertionError(f"was ALLOWED but should be denied: {sql}")


def secrets_present():
    for user in ("system", "firco_app", "lh_extract"):
        oracle_password(user)
    return "3 passwords set in .env"


def env_not_in_git():
    ignored = subprocess.run(["git", "check-ignore", "-q", ".env"], cwd=PROJECT_ROOT).returncode == 0
    tracked = subprocess.run(["git", "ls-files", "--error-unmatch", ".env"], cwd=PROJECT_ROOT,
                             capture_output=True).returncode == 0
    assert ignored and not tracked, ".env is not ignored, or is already committed"
    return ".env ignored and untracked"


def oracle_volume():
    out = subprocess.run(["docker", "volume", "inspect", "screening-oradata"], capture_output=True)
    assert out.returncode == 0, "volume screening-oradata not found"
    return "named volume exists"


def owner_cannot_login():
    try:
        oracledb.connect(user="aml_fff", password="anything", dsn=ORACLE_DSN)
    except oracledb.DatabaseError as exc:
        if exc.args[0].full_code == "ORA-01017":  # invalid credentials = the account can't log in
            return "login refused (ORA-01017)"
        raise  # any other error (e.g. database down) is NOT a pass
    raise AssertionError("AML_FFF could log in")


def extract_reads_five_tables():
    tables = ["alert_snapshot", "alert_audit_log", "fff_users", "fff_user_profile", "fof_audit_profile"]
    with connect("lh_extract") as con, con.cursor() as cur:
        for t in tables:
            cur.execute(f"SELECT COUNT(*) FROM aml_fff.{t}")
    return "SELECT works on 5 tables"


def extract_cannot_write_or_see_more():
    with connect("lh_extract") as con, con.cursor() as cur:
        must_be_denied(cur, "INSERT INTO aml_fff.alert_audit_log (audit_event_id) SELECT -1 FROM dual WHERE 1 = 0")
        must_be_denied(cur, "SELECT COUNT(*) FROM aml_fff.screening_input")
    return "INSERT denied; screening_input not visible"


def audit_log_is_append_only():
    with connect("firco_app") as con, con.cursor() as cur:
        cur.execute(
            "INSERT INTO aml_fff.alert_audit_log (audit_event_id, alert_id, event_timestamp, actor_type,"
            " actor_id, action_code) VALUES (-1, 'CHECK', SYSTIMESTAMP, 'WORKFLOW', 'phase1-check', 'TEST')"
        )
        con.rollback()  # leave no trace
        must_be_denied(cur, "UPDATE aml_fff.alert_audit_log SET comments = 'x' WHERE 1 = 0")
        must_be_denied(cur, "DELETE FROM aml_fff.alert_audit_log WHERE 1 = 0")
    return "INSERT allowed (rolled back); UPDATE and DELETE denied"


def ctl_tables_exist():
    for name in ("file_manifest", "run_log"):
        assert (LAKEHOUSE / "ctl" / name / "_delta_log").is_dir(), f"ctl.{name} missing - run: make ctl"
    return "ctl.file_manifest, ctl.run_log"


CHECKS = [
    secrets_present, env_not_in_git, oracle_volume, owner_cannot_login,
    extract_reads_five_tables, extract_cannot_write_or_see_more, audit_log_is_append_only, ctl_tables_exist,
]


def main() -> int:
    failed = 0
    for check in CHECKS:
        try:
            print(f"PASS  {check.__name__:34} {check()}")
        except Exception as exc:  # report every failure, don't stop at the first
            failed += 1
            print(f"FAIL  {check.__name__:34} {str(exc).splitlines()[0]}")
    print(f"\n{len(CHECKS) - failed}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())