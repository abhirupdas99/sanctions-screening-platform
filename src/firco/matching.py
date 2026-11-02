"""The mock Firco's name matching: normalise names, score customers against list entries, decide what alerts.

Real Firco uses 40+ tunable algorithms; this keeps the two ideas that matter for the pipeline:
a strong rule (the whole name is very similar) and a weak rule (same surname, loosely similar name),
which deliberately produces false positives for analysts to clear.
"""
import re
import unicodedata

from rapidfuzz import fuzz, process

STRONG = 88          # whole-name similarity that raises an alert on its own
WEAK = 55            # lower similarity that still alerts when the surname matches exactly
MIN_SURNAME = 4      # very short surnames (e.g. "Li") are too common to alert on alone


def normalise(name: str) -> str:
    """Uppercase, strip accents and punctuation, collapse spaces: 'SMIRNOVA, Mohammad' -> 'SMIRNOVA MOHAMMAD'."""
    text = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode()
    return " ".join(re.sub(r"[^A-Za-z0-9 ]", " ", text).upper().split())


def score(customer: str, listed: str) -> float:
    """0-100. token_sort ignores word order (LAST, Given); token_set lets a missing middle name still match."""
    a, b = normalise(customer), normalise(listed)
    if not a or not b:
        return 0.0
    best = fuzz.token_sort_ratio(a, b)
    if len(a.split()) >= 2 and len(b.split()) >= 2:     # a single word must never match as a 'subset'
        best = max(best, fuzz.token_set_ratio(a, b))
    return round(best, 1)


def surname_hit(customer: str, listed: str) -> bool:
    """True if the customer's surname (last word, or first word before a comma) is a word of the listed name."""
    raw = (customer or "").strip()
    surname = raw.split(",")[0] if "," in raw else (raw.split() or [""])[-1]
    surname = normalise(surname)
    return len(surname) >= MIN_SURNAME and surname in normalise(listed).split()


class ListIndex:
    """All names of one list version, normalised once, so each customer is scored in a single fast pass."""

    def __init__(self, entries: list[dict]):
        self.entries = entries
        self.names, self.owner = [], []
        for e in entries:
            for n in e["names"]:
                if normalise(n):
                    self.names.append(normalise(n))
                    self.owner.append(e)

    def best_hit(self, customer: str):
        """The highest-scoring entry that triggers the strong or weak rule, or None -> (entry, score, n_entries_hit)."""
        query = normalise(customer)
        if not query or not self.names:
            return None
        candidates = set()
        for scorer in (fuzz.token_sort_ratio, fuzz.token_set_ratio):   # fast C pre-filter over every name
            for _, _, i in process.extract(query, self.names, scorer=scorer, score_cutoff=WEAK, limit=None):
                candidates.add(i)
        hits = {}
        for i in candidates:
            s = score(customer, self.names[i])
            if s >= STRONG or (s >= WEAK and surname_hit(customer, self.names[i])):
                e = self.owner[i]
                hits[e["entry_id"]] = max(s, hits.get(e["entry_id"], (0, None))[0]), e
        if not hits:
            return None
        s, e = max(hits.values(), key=lambda h: (h[0], h[1]["entry_id"]))   # deterministic tie-break
        return e, s, len(hits)


def dob_compatible(customer_dob: str | None, listed_dob: str | None) -> bool:
    """Can the dates of birth belong to the same person? The list writes them many ways: '1965-04-29', '1965',
    '1958 or 1959', '1965-04-29 or 1966-01-01'. Unknown on either side can't rule a match out."""
    if not customer_dob or not listed_dob:
        return True
    dates = re.findall(r"\d{4}-\d{2}-\d{2}", listed_dob)
    if dates:                                             # any full date listed: the customer's must be one of them
        return customer_dob[:10] in dates
    years = re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", listed_dob)
    return not years or customer_dob[:4] in years         # otherwise compare on the year(s)