"""Oracle connections for the application accounts (passwords from .env), plus the audit-ID sequence."""
import oracledb

from src.common.config import ORACLE_DSN, oracle_password


def connect(user: str):
    """A connection as FIRCO_APP or LH_EXTRACT. Never as SYSTEM or the schema owner."""
    return oracledb.connect(user=user, password=oracle_password(user), dsn=ORACLE_DSN)


def next_audit_ids(cur, n: int) -> list[int]:
    """n new audit_event_ids from the Oracle sequence, in order."""
    if n == 0:
        return []
    cur.execute("SELECT aml_fff.alert_audit_seq.NEXTVAL FROM dual CONNECT BY LEVEL <= :n", {"n": n})
    return sorted(int(r[0]) for r in cur.fetchall())