"""Phase 2 acceptance check. Prints PASS/FAIL per check and exits 1 if anything failed."""
import csv
import hashlib
import json
import subprocess
import sys

from src.common.config import DATA, PROJECT_ROOT
from src.generator import settings as S

LANDING, ANSWERS = DATA / "landing", DATA / "answer_key"
REFERENCE_LIST = DATA / "reference" / "sema-lmes.xml"


def read_csv(name):
    with open(ANSWERS / name, encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def classify(data_path, algo):
    """What a correct Task 3 gate should report for this file."""
    cs = data_path.with_name(data_path.name + ".checksum")
    if not cs.exists():
        return "MISSING"
    expected = "".join(cs.read_text().split()).lower()
    if len(expected) not in (32, 64):
        return "UNDETERMINED"
    actual = hashlib.new(algo, data_path.read_bytes()).hexdigest()
    return "MATCH" if actual == expected else "MISMATCH"


def reference_list_recorded():
    summary = json.loads((ANSWERS / "summary.json").read_text())
    actual = hashlib.sha256(REFERENCE_LIST.read_bytes()).hexdigest()
    assert actual == summary["reference_list_sha256"], "sema-lmes.xml changed since generation - re-run make generate"
    return f"list sha256 {actual[:12]}..."


def thirty_day_folders():
    days = sorted(p.name for p in LANDING.iterdir() if p.is_dir())
    assert len(days) == S.BUSINESS_DAYS, f"{len(days)} day folders, expected {S.BUSINESS_DAYS}"
    return f"{days[0]} .. {days[-1]}"


def defects_classify_as_expected():
    files = read_csv("files.csv")
    wrong = [f["file_name"] for f in files
             if classify(LANDING / f["business_date"] / f["file_name"], f["algo"]) != f["expected_result"]]
    assert not wrong, f"unexpected checksum result for {wrong}"
    planted = sum(1 for f in files if f["defect"])
    return f"{len(files)} files, {planted} planted defects behave as the answer key says"


def row_counts_match():
    for f in read_csv("files.csv"):
        if f["source"] == "sanctions_list" or f["defect"] == "file_truncated":
            continue
        lines = (LANDING / f["business_date"] / f["file_name"]).read_bytes().count(b"\n") - 1
        assert lines == int(f["data_rows"]), f"{f['file_name']}: {lines} rows, answer key says {f['data_rows']}"
    return "every intact file has the row count in the answer key"


def seeded_customers_present():
    for s in read_csv("seeded_matches.csv"):
        path = LANDING / s["first_business_date"] / f"core_customers_{s['first_business_date'].replace('-', '')}.tsv"
        assert s["customer_name"] in path.read_text(encoding="utf-8"), f"{s['label']} not in {path.name}"
    return "5 true matches + 3 near misses present from their first day"


def invalid_rows_present():
    first = sorted(p for p in LANDING.iterdir())[0]
    text = next(first.glob("core_*.tsv")).read_text(encoding="utf-8")
    missing = [r["record_id"] or "(blank id)" for r in read_csv("invalid_rows.csv")
               if f"{r['record_id']}\t{r['name']}\t" not in text]
    assert not missing, f"invalid rows missing on day 1: {missing}"
    return "6 invalid rows on day 1"


def answer_key_outside_landing():
    assert not list(LANDING.rglob("*.csv")) and not list(LANDING.rglob("*.json")), "answer key leaked into landing"
    return "answer key kept separate from landing"


def generator_is_deterministic():
    def fingerprint():
        return {p.relative_to(DATA).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                for folder in (LANDING, ANSWERS) for p in sorted(folder.rglob("*")) if p.is_file()}
    before = fingerprint()
    subprocess.run([sys.executable, "-m", "src.generator.generate_data"], cwd=PROJECT_ROOT, check=True,
                   capture_output=True)
    assert fingerprint() == before, "a second run produced different files"
    return f"re-run produced {len(before)} byte-identical files"


CHECKS = [reference_list_recorded, thirty_day_folders, defects_classify_as_expected, row_counts_match,
          seeded_customers_present, invalid_rows_present, answer_key_outside_landing, generator_is_deterministic]


def main() -> int:
    failed = 0
    for check in CHECKS:
        try:
            print(f"PASS  {check.__name__:30} {check()}")
        except Exception as exc:  # report every failure, don't stop at the first
            failed += 1
            print(f"FAIL  {check.__name__:30} {str(exc).splitlines()[0]}")
    print(f"\n{len(CHECKS) - failed}/{len(CHECKS)} checks passed")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())