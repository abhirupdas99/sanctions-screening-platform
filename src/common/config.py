"""Central settings: every path and secret comes from here, read from .env."""
import os
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
load_dotenv(PROJECT_ROOT / ".env")


def _required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing setting {name} - copy .env.example to .env and fill it in")
    return value


LAKEHOUSE = PROJECT_ROOT / os.getenv("LAKEHOUSE_DIR", "lakehouse")
DATA = PROJECT_ROOT / os.getenv("DATA_DIR", "data")
ORACLE_DSN = os.getenv("ORACLE_DSN", "localhost:1522/FREEPDB1")


def oracle_password(user: str) -> str:
    """Password for an Oracle account, e.g. oracle_password('lh_extract')."""
    return _required(f"ORACLE_{user.upper()}_PASSWORD")
