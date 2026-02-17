"""Firebase Realtime Database sync proxy.

Proxies read/write operations to Firebase REST API so that the
database secret never reaches the browser.
"""

from __future__ import annotations

import json
import urllib.request
import urllib.error
from typing import Any


def _firebase_url(project_id: str, path: str, secret: str) -> str:
    base = f"https://{project_id}-default-rtdb.firebaseio.com"
    return f"{base}/{path}.json?auth={secret}"


def firebase_read(project_id: str, path: str, secret: str) -> Any:
    """GET data from Firebase.  Returns parsed JSON or ``None`` on error."""
    url = _firebase_url(project_id, path, secret)
    try:
        req = urllib.request.Request(url, method="GET")
        with urllib.request.urlopen(req, timeout=15) as resp:
            return json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, json.JSONDecodeError, OSError):
        return None


def firebase_write(project_id: str, path: str, secret: str, data: Any) -> bool:
    """PUT data to Firebase.  Returns ``True`` on success."""
    url = _firebase_url(project_id, path, secret)
    body = json.dumps(data).encode("utf-8")
    try:
        req = urllib.request.Request(
            url,
            data=body,
            method="PUT",
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(req, timeout=15) as resp:
            return resp.status == 200
    except (urllib.error.URLError, OSError):
        return False
