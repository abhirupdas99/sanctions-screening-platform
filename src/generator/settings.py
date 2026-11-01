"""Phase 2 generator settings. Change a number here, re-run, and you get a different but repeatable dataset."""
from datetime import date

SEED = 20261031                  # same seed -> byte-identical files every run
START_DATE = date(2026, 10, 1)   # first business date
BUSINESS_DAYS = 30               # weekdays only

N_PERSONS = 450                  # real people behind the records
CORE_SHARE = 0.90                # share of persons held by core banking
CARDS_SHARE = 0.30               # share of persons held by the cards system (overlaps core)
EXTRA_ROLE_RATE = 0.10           # persons with a second core row (another position) = duplicate
VARIANT_RATE = 0.30              # of duplicate rows, how many differ only in case/spacing of the name

DAILY_NEW = (2, 5)               # new persons per day (min, max)
DAILY_UPDATES = (3, 6)           # changed records per day
DAILY_REMOVALS = (0, 2)          # persons who leave per day
BLANK_POSITION_RATE = 0.05       # NULL-like blanks in a compared column

LIST_V2_DAY = 15                 # business day the second list version is published
HELD_BACK = 2                    # list entries missing from v1, added in v2

# Day number -> planted file defect. Expected checksum result is what Phase 3 must report.
FILE_DEFECTS = {
    5:  ("core",  "checksum_corrupted",  "MISMATCH"),
    9:  ("cards", "file_truncated",      "MISMATCH"),
    12: ("core",  "checksum_uppercase",  "MATCH"),
    15: ("cards", "checksum_crlf_space", "MATCH"),
    18: ("core",  "file_crlf",           "MATCH"),
    20: ("cards", "checksum_short",      "UNDETERMINED"),
    23: ("core",  "checksum_missing",    "MISSING"),
    27: ("cards", "file_empty",          "MATCH"),
}