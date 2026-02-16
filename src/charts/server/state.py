"""Server-side UI state persistence.

Stores UI state (watchlists, layouts, drawings, display settings) as flat
JSON files — one per key — in a configurable directory so that any device
connecting to the server sees the same state.

Default directory: ``~/.orb/chart_state/``
"""

from __future__ import annotations

import json
import re
import threading
from pathlib import Path
from typing import Any

_VALID_KEY = re.compile(r"^[a-zA-Z0-9_]+$")
_write_lock = threading.Lock()


def _default_state_dir() -> Path:
    return Path.home() / ".orb" / "chart_state"


def _sanitize_key(key: str) -> str | None:
    """Return the key if safe for use as a filename, else ``None``."""
    if _VALID_KEY.match(key):
        return key
    return None


def _state_file(state_dir: Path, key: str) -> Path:
    return state_dir / f"{key}.json"


def load_state(state_dir: Path | None, key: str) -> Any:
    """Load a single state key.  Returns ``None`` if missing or invalid."""
    d = state_dir or _default_state_dir()
    safe = _sanitize_key(key)
    if not safe:
        return None
    path = _state_file(d, safe)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def save_state(state_dir: Path | None, key: str, data: Any) -> bool:
    """Persist a single state key.  Returns ``True`` on success."""
    d = state_dir or _default_state_dir()
    safe = _sanitize_key(key)
    if not safe:
        return False
    d.mkdir(parents=True, exist_ok=True)
    path = _state_file(d, safe)
    tmp = path.with_suffix(".tmp")
    with _write_lock:
        try:
            tmp.write_text(json.dumps(data), encoding="utf-8")
            tmp.replace(path)
            return True
        except OSError:
            return False


def load_all_state(state_dir: Path | None) -> dict[str, Any]:
    """Load every state key into a single dict."""
    d = state_dir or _default_state_dir()
    result: dict[str, Any] = {}
    if not d.exists():
        return result
    for path in sorted(d.glob("*.json")):
        key = path.stem
        try:
            result[key] = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            continue
    return result


def delete_state(state_dir: Path | None, key: str) -> bool:
    """Delete a single state key.  Returns ``True`` if deleted."""
    d = state_dir or _default_state_dir()
    safe = _sanitize_key(key)
    if not safe:
        return False
    path = _state_file(d, safe)
    try:
        path.unlink(missing_ok=True)
        return True
    except OSError:
        return False
