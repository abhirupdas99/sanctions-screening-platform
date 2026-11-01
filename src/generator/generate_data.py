"""Phase 2: generate 30 business days of synthetic customer files, two sanctions-list versions,
checksum files with planted defects, and an answer key.

Every customer is invented by Faker. Only the seeded matches reuse names from the public sanctions list.
Same SEED -> byte-identical output, so every later phase can be tested against a fixed answer key.
"""
import csv
import hashlib
import io
import json
import random
import shutil
import sys
import xml.etree.ElementTree as ET
from collections import Counter
from datetime import timedelta

from faker import Faker

from src.common.config import DATA
from src.generator import settings as S

LANDING = DATA / "landing"
ANSWERS = DATA / "answer_key"
REFERENCE_LIST = DATA / "reference" / "sema-lmes.xml"
COLUMNS = ["Record_ID", "Name", "Country_Code", "Position_Held", "Company_Name", "Date_Of_Birth"]
ALGO = {"core": "md5", "cards": "sha3_256"}

CUSTOMER_COUNTRIES = ["CA"] * 12 + ["US"] * 4 + ["GB", "FR", "IN", "CN", "DE", "MX", "BR", "PH"]
POSITIONS = ["Director", "Chief Executive Officer", "Chief Financial Officer", "Board Member", "President",
             "Vice President", "Managing Director", "Partner", "Owner", "Analyst", "Engineer", "Consultant"]
# English country names used on the Canadian list -> ISO alpha-2 (only for seeded matches)
LIST_COUNTRY_ISO = {"Russia": "RU", "Belarus": "BY", "Iran": "IR", "Syria": "SY", "Venezuela": "VE",
                    "Myanmar": "MM", "Moldova": "MD", "Haiti": "HT", "Nicaragua": "NI", "Ukraine": "UA",
                    "Zimbabwe": "ZW", "Libya": "LY", "South Sudan": "SS", "Sudan": "SD", "China": "CN",
                    "Sri Lanka": "LK", "Guatemala": "GT", "North Korea": "KP", "Mali": "ML", "Iraq": "IQ"}


# ---------------------------------------------------------------------------- sanctions list
def field(rec, prefix):
    """Child text whose tag starts with prefix (tags are bilingual, e.g. 'LastName-NomDeFamille')."""
    for child in rec:
        if child.tag.lower().startswith(prefix.lower()):
            return (child.text or "").strip()
    return ""


def load_list():
    if not REFERENCE_LIST.exists():
        sys.exit(f"Missing {REFERENCE_LIST} - do Step 1 (download the sanctions list) first.")
    tree = ET.parse(REFERENCE_LIST)
    people = []
    for rec in tree.getroot().iter("record"):
        given, last = field(rec, "GivenName"), field(rec, "LastName")
        country = field(rec, "Country").split(" / ")[0].strip()
        if given and last and country in LIST_COUNTRY_ISO:
            people.append({"rec": rec, "given": given, "last": last, "iso": LIST_COUNTRY_ISO[country],
                           "dob": field(rec, "DateOfBirth"), "item": field(rec, "Item"),
                           "schedule": field(rec, "Schedule"), "country": country})
    if len(people) < 8 + S.HELD_BACK:
        sys.exit("Sanctions list has too few usable person records - is the XML complete?")
    return tree, people


def write_xml(tree, drop):
    """Serialise the list without the records in `drop` (used to build version 1)."""
    parents = {child: parent for parent in tree.getroot().iter() for child in parent}
    removed = [(parents[r], list(parents[r]).index(r), r) for r in drop]
    for parent, _, r in removed:
        parent.remove(r)
    buf = io.BytesIO()
    tree.write(buf, encoding="utf-8", xml_declaration=True)
    for parent, index, r in reversed(removed):   # restore, in place, so the full list stays intact
        parent.insert(index, r)
    return buf.getvalue()


# ---------------------------------------------------------------------------- people and rows
class World:
    def __init__(self):
        self.rng = random.Random(S.SEED)
        self.fake = Faker(["en_CA", "fr_CA", "en_US", "en_GB"])
        self.fake.seed_instance(S.SEED)
        self.core, self.cards = {}, {}            # record_id -> row (dict, with hidden 'pid')
        self.next_id = {"core": 100000, "cards": 500000}
        self.pid = 0
        self.events = []

    def new_id(self, source):
        self.next_id[source] += self.rng.randint(1, 3)
        return str(self.next_id[source])

    def position(self):
        return "" if self.rng.random() < S.BLANK_POSITION_RATE else self.rng.choice(POSITIONS)

    def person(self, name=None, country=None, dob=None):
        self.pid += 1
        return {"pid": self.pid,
                "name": name or f"{self.fake.first_name()} {self.fake.last_name()}",
                "country": country or self.rng.choice(CUSTOMER_COUNTRIES),
                "dob": dob or self.fake.date_of_birth(minimum_age=21, maximum_age=85).isoformat()}

    def row(self, p, source, name=None):
        return {"Record_ID": self.new_id(source), "Name": name or p["name"], "Country_Code": p["country"],
                "Position_Held": self.position(), "Company_Name": self.fake.company(),
                "Date_Of_Birth": p["dob"], "pid": p["pid"]}

    def variant(self, name):
        """Same person, written differently: the case/spacing duplicates Task 1 must catch."""
        kind = self.rng.choice(["upper", "lower", "spaces", "padded"])
        return {"upper": name.upper(), "lower": name.lower(), "spaces": name.replace(" ", "  "),
                "padded": f" {name} "}[kind]

    def add_person(self, p, day, in_core=True, in_cards=False):
        if in_core:
            r = self.row(p, "core")
            self.core[r["Record_ID"]] = r
            self.log(day, "core", r, "NEW")
            if self.rng.random() < S.EXTRA_ROLE_RATE:      # second role = a duplicate row
                name = self.variant(p["name"]) if self.rng.random() < S.VARIANT_RATE else p["name"]
                r2 = self.row(p, "core", name)
                self.core[r2["Record_ID"]] = r2
                self.log(day, "core", r2, "NEW")
        if in_cards:
            name = self.variant(p["name"]) if self.rng.random() < S.VARIANT_RATE else p["name"]
            r = self.row(p, "cards", name)
            self.cards[r["Record_ID"]] = r
            self.log(day, "cards", r, "NEW")

    def log(self, day, source, r, event):
        if day > 1:                                         # day 1 is the initial full load
            self.events.append({"business_day": day, "source": source,
                                "record_id": r["Record_ID"], "event": event})


INVALID_ROWS = [  # (Record_ID, Name, Country_Code, reason) - kept in every core file until fixed
    ("ABC123", "Marie Tremblay", "CA", "source_record_id not numeric"),
    ("100777", "", "CA", "name missing"),
    ("100888", "Jordan Blake", "", "country_code missing"),
    ("", "Priya Raman", "IN", "source_record_id missing"),
    ("12A45", "Lucas Martin", "FR", "source_record_id not numeric"),
    ("100999", "   ", "US", "name missing"),
]
FIXED_ON = {"100888": 8}   # day the source system fixes that row (country filled in)


# ---------------------------------------------------------------------------- files
def to_tsv(rows, crlf=False):
    buf = io.StringIO()
    w = csv.writer(buf, delimiter="\t", lineterminator="\r\n" if crlf else "\n")
    w.writerow(COLUMNS)
    for r in rows:
        w.writerow([r[c] for c in COLUMNS])
    return buf.getvalue().encode("utf-8")


def digest(algo, data):
    return hashlib.new(algo, data).hexdigest()


def write_with_checksum(folder, name, data, algo, defect=None):
    """Write a data file and its .checksum; apply the planted defect, if any."""
    good = digest(algo, data)
    checksum = good + "\n"
    if defect == "checksum_corrupted":
        checksum = good[:-1] + ("0" if good[-1] != "0" else "1") + "\n"
    elif defect == "file_truncated":
        data = data[: len(data) * 2 // 3]                   # transfer cut off mid-file
    elif defect == "checksum_uppercase":
        checksum = good.upper() + "\n"
    elif defect == "checksum_crlf_space":
        checksum = good + " \r\n"
    elif defect == "checksum_short":
        checksum = good[:-1] + "\n"
    (folder / name).write_bytes(data)
    if defect != "checksum_missing":
        (folder / f"{name}.checksum").write_bytes(checksum.encode("ascii"))


def core_rows(world, day):
    rows = sorted(world.core.values(), key=lambda r: int(r["Record_ID"]))
    for pos, (rid, name, cc, _) in zip([10, 60, 120, 200, 300, 380], INVALID_ROWS):
        if FIXED_ON.get(rid, 99) <= day:
            cc = "CA"
        rows.insert(min(pos, len(rows)), {"Record_ID": rid, "Name": name, "Country_Code": cc,
                                          "Position_Held": "Analyst", "Company_Name": "Unknown Ltd",
                                          "Date_Of_Birth": "1980-01-01"})
    return rows


# ---------------------------------------------------------------------------- daily changes
def evolve(world, day, protected):
    rng = world.rng
    # removals: a person leaves the bank -> all their rows disappear
    candidates = sorted({r["pid"] for r in world.core.values()} - protected)
    for pid in rng.sample(candidates, rng.randint(*S.DAILY_REMOVALS)):
        for store, source in ((world.core, "core"), (world.cards, "cards")):
            for rid in [k for k, r in store.items() if r["pid"] == pid]:
                world.log(day, source, store.pop(rid), "DELETE")
    # updates: a field changes on an existing record
    ids = sorted(k for k, r in world.core.items() if r["pid"] not in protected)
    for rid in rng.sample(ids, rng.randint(*S.DAILY_UPDATES)):
        r = world.core[rid]
        what = rng.choice(["Position_Held", "Company_Name", "Position_Held", "Name"])
        if what == "Name":                                  # e.g. middle initial added
            first, _, rest = r["Name"].partition(" ")
            r["Name"] = f"{first} {world.fake.random_uppercase_letter()}. {rest}"
        elif what == "Company_Name":
            r["Company_Name"] = world.fake.company()
        else:
            r["Position_Held"] = world.position()
        world.log(day, "core", r, "UPDATE")
    # new customers
    for _ in range(rng.randint(*S.DAILY_NEW)):
        world.add_person(world.person(), day, in_core=rng.random() < 0.85, in_cards=rng.random() < 0.3)
    if day in FIXED_ON.values():
        for rid, fix_day in FIXED_ON.items():
            if fix_day == day:
                world.events.append({"business_day": day, "source": "core", "record_id": rid,
                                     "event": "FIXED"})


# ---------------------------------------------------------------------------- main
def business_days():
    d, days = S.START_DATE, []
    while len(days) < S.BUSINESS_DAYS:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


def seed_matches(world, people):
    """Pick list persons and create customers that resemble them (true matches + near misses)."""
    rng = world.rng
    # only people listed once, so a held-back name can't still appear in v1 under another regulation
    listed = Counter((p["given"], p["last"]) for p in people)
    unique = [p for p in people if listed[(p["given"], p["last"])] == 1]
    picks = rng.sample(unique, 8)
    a, b, c, d_, e = picks[:5]
    near = picks[5:]

    def full(p):
        return f"{p['given']} {p['last']}"

    def typo(s):
        if len(s) < 4:
            return s + s[-1]                                  # short name: double the last letter
        i = rng.randrange(1, len(s) - 2)
        return s[:i] + s[i + 1] + s[i] + s[i + 2:]          # swap two letters

    def dob(p):  # list dates may be full, year-only or missing
        if len(p["dob"]) == 10:
            return p["dob"]
        if len(p["dob"]) == 4:
            return f"{p['dob']}-01-01"
        return world.fake.date_of_birth(minimum_age=30, maximum_age=80).isoformat()

    plan = [  # label, list person, name as the bank holds it, arrives on day, visible in list version
        ("A_exact", a, full(a), 1, "v1"),
        ("B_typo", b, f"{typo(b['given'])} {b['last']}", 1, "v1"),
        ("C_new_customer", c, f"{c['given'].split()[0]} {c['last']}", 10, "v1"),
        ("D_listed_in_v2", d_, f"{d_['last'].upper()}, {d_['given']}", 1, "v2"),
        ("E_listed_in_v2", e, full(e), 1, "v2"),
    ]
    seeds = []
    for label, p, name, day, version in plan:
        seeds.append({"label": label, "person": world.person(name, p["iso"], dob(p)), "list": p,
                      "day": day, "version": version, "name": name})
    for i, p in enumerate(near, 1):   # same surname, different person: should NOT be confirmed
        name = f"{world.fake.first_name()} {p['last']}"
        seeds.append({"label": f"N{i}_near_miss", "person": world.person(name), "list": p,
                      "day": 1, "version": "none", "name": name})
    return seeds, [d_["rec"], e["rec"]]


def main():
    tree, people = load_list()
    world = World()
    days = business_days()
    seeds, held_back = seed_matches(world, people)
    protected = {s["person"]["pid"] for s in seeds}

    for folder in (LANDING, ANSWERS):
        if folder.exists():
            shutil.rmtree(folder)
        folder.mkdir(parents=True)

    # day-1 population
    for _ in range(S.N_PERSONS):
        world.add_person(world.person(), 1, in_core=world.rng.random() < S.CORE_SHARE,
                         in_cards=world.rng.random() < S.CARDS_SHARE)
    for s in seeds:
        if s["day"] == 1:
            world.add_person(s["person"], 1, in_core=True, in_cards=s["label"] == "A_exact")

    files, counts = [], []
    for n, day in enumerate(days, 1):
        if n > 1:
            evolve(world, n, protected)
            for s in seeds:
                if s["day"] == n:
                    world.add_person(s["person"], n, in_core=True)
        folder = LANDING / day.isoformat()
        folder.mkdir()
        stamp = day.strftime("%Y%m%d")
        for source in ("core", "cards"):
            src, defect, expected = S.FILE_DEFECTS.get(n, (None, None, "MATCH"))
            defect = defect if src == source else None
            expected = expected if src == source else "MATCH"
            rows = core_rows(world, n) if source == "core" else list(world.cards.values())
            if defect == "file_empty":
                rows = []
            name = f"{source}_customers_{stamp}.tsv"
            write_with_checksum(folder, name, to_tsv(rows, crlf=defect == "file_crlf"), ALGO[source], defect)
            files.append({"business_date": day.isoformat(), "file_name": name, "source": source,
                          "algo": ALGO[source], "defect": defect or "", "expected_result": expected,
                          "data_rows": len(rows)})
        if n in (1, S.LIST_V2_DAY):                       # sanctions list versions
            version = "v1" if n == 1 else "v2"
            xml = write_xml(tree, held_back if version == "v1" else [])
            name = f"sanctions_list_{version}_{stamp}.xml"
            write_with_checksum(folder, name, xml, "sha3_256")
            files.append({"business_date": day.isoformat(), "file_name": name, "source": "sanctions_list",
                          "algo": "sha3_256", "defect": "", "expected_result": "MATCH", "data_rows": ""})
        counts.append({"business_date": day.isoformat(), "core_rows": len(core_rows(world, n)),
                       "cards_rows": len(world.cards), "persons": len({r["pid"] for r in
                                                                       [*world.core.values(), *world.cards.values()]})})

    # ---- answer key: what every later phase should find
    def write_csv(name, rows):
        with open(ANSWERS / name, "w", newline="", encoding="utf-8") as fh:
            w = csv.DictWriter(fh, fieldnames=list(rows[0]))
            w.writeheader()
            w.writerows(rows)

    by_day = {n: d.isoformat() for n, d in enumerate(days, 1)}
    write_csv("files.csv", files)
    write_csv("events.csv", [{**e, "business_date": by_day[e["business_day"]]} for e in world.events])
    write_csv("invalid_rows.csv", [{"record_id": rid, "name": name, "country_code": cc, "reason": why,
                                    "fixed_on": by_day.get(FIXED_ON.get(rid, 0), "")}
                                   for rid, name, cc, why in INVALID_ROWS])
    write_csv("seeded_matches.csv", [{
        "label": s["label"], "customer_name": s["name"], "country_code": s["person"]["country"],
        "date_of_birth": s["person"]["dob"], "first_business_date": by_day[s["day"]],
        "list_name": f"{s['list']['given']} {s['list']['last']}", "list_country": s["list"]["country"],
        "list_item": f"{s['list']['schedule']} / {s['list']['item']}", "should_alert_from_list": s["version"],
        "record_ids": " ".join(k for k, r in [*world.core.items(), *world.cards.items()]
                               if r["pid"] == s["person"]["pid"]),
    } for s in seeds])
    write_csv("daily_counts.csv", counts)
    summary = {"seed": S.SEED, "business_days": len(days), "first_day": by_day[1], "last_day": by_day[len(days)],
               "reference_list_sha256": digest("sha256", REFERENCE_LIST.read_bytes()),
               "files": len(files), "events": len(world.events)}
    (ANSWERS / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(f"Generated {len(files)} files over {len(days)} business days "
          f"({by_day[1]} .. {by_day[len(days)]}) into {LANDING}")
    print(f"Answer key: {ANSWERS}")


if __name__ == "__main__":
    main()