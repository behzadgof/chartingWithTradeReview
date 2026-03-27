"""Template loading utilities shared across page routes."""

from __future__ import annotations

from pathlib import Path

_TEMPLATES_DIR = Path(__file__).resolve().parent.parent.parent / "templates"


def _load_template(name: str) -> str:
    """Read an HTML template and perform shared-code substitution."""
    template_path = _TEMPLATES_DIR / name
    html = template_path.read_text(encoding="utf-8")

    _subs = {
        "{{LIB_JS}}": "lightweight-charts.js",
        "{{SHARED_CSS}}": "_shared.css",
        "{{SHARED_INDICATORS_JS}}": "_indicators.js",
        "{{DRAWING_CSS}}": "_drawing.css",
        "{{DRAWING_JS}}": "_drawing.js",
    }

    for placeholder, filename in _subs.items():
        path = _TEMPLATES_DIR / filename
        html = html.replace(
            placeholder,
            path.read_text(encoding="utf-8") if path.exists() else "",
        )

    return html
