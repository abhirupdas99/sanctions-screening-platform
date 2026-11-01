"""Task 3's rules in Python: is this file complete (checksum present) and intact (digest matches)?"""
import hashlib #library for hashing algorithms
from pathlib import Path #library for filesystem paths

ALGO_BY_LENGTH = {32: "md5", 64: "sha3_256"}   # 32 hex chars = MD5, 64 = SHA3-256

#function that goes to the path of the file and checks if the checksum is present and if it matches the expected value. It returns the algorithm used and the result of the check.
def check_file(data_path: Path) -> tuple[str | None, str]:
    """Returns (algorithm, result). Result is MATCH, MISMATCH, UNDETERMINED or MISSING."""
    checksum_path = data_path.with_name(data_path.name + ".checksum")
    if not checksum_path.exists():
        return None, "MISSING"                       # the "file complete" signal hasn't arrived
    expected = "".join(checksum_path.read_text(encoding="ascii", errors="replace").split()).lower()
    algo = ALGO_BY_LENGTH.get(len(expected))
    if algo is None:
        return None, "UNDETERMINED"                   # report it, never guess
    digest = hashlib.new(algo)
    with data_path.open("rb") as fh:                  # stream in 1 MB chunks: works for any file size
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return algo, "MATCH" if digest.hexdigest() == expected else "MISMATCH"