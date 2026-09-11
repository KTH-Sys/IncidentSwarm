"""Named live SQL queries shared by the dashboard and offline verification."""

from pathlib import Path
import re

QUERIES_PATH = Path(__file__).with_name("queries.sql")


def load_queries(path: Path = QUERIES_PATH) -> dict[str, str]:
    parts = re.split(r"^--\s*name:\s*(\S+)\s*$", path.read_text(), flags=re.MULTILINE)
    return {parts[i].strip(): parts[i + 1].strip() for i in range(1, len(parts) - 1, 2)}
