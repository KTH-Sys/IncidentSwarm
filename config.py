"""Loads .env for every entry point.

Imported for its side effect — `import config` near the top of a CLI or the
Streamlit app is enough. Values already in the real environment win, so a shell
export still overrides the file.
"""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent

load_dotenv(ROOT / ".env", override=False)
