from __future__ import annotations

import os
from pathlib import Path
from typing import Dict, Tuple, Any

ROOT_DIR = Path(__file__).resolve().parent
# Serve frontend build from frontend/dist after separating frontend folder
DIST_DIR = ROOT_DIR.parent / "frontend" / "dist"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if value and len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1]
        os.environ.setdefault(key, value)


for env_path in (ROOT_DIR.parent / ".env", ROOT_DIR.parent / ".env.local", ROOT_DIR / ".env"):
    load_env_file(env_path)


def env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in {"1", "true", "yes", "on"}


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return int(float(raw))
    except (TypeError, ValueError):
        return default


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return default
    try:
        return float(raw)
    except (TypeError, ValueError):
        return default


OPENROUTER_BASE_URL = os.getenv("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1").rstrip("/")
OPENROUTER_API_KEY = os.getenv("OPENROUTER_API_KEY", "").strip()
OPENROUTER_MODEL = os.getenv("OPENROUTER_MODEL", "openrouter/free").strip() or "openrouter/free"
OPENROUTER_HTTP_REFERER = os.getenv("OPENROUTER_HTTP_REFERER", "http://localhost:5000").strip()
OPENROUTER_TITLE = os.getenv("OPENROUTER_TITLE", "ReviewIQ").strip()
OPENROUTER_TIMEOUT_SECONDS = env_float("OPENROUTER_TIMEOUT_SECONDS", 75.0)
REVIEWIQ_BATCH_SIZE = env_int("REVIEWIQ_BATCH_SIZE", 14)
REVIEWIQ_MAX_WORKERS = env_int("REVIEWIQ_MAX_WORKERS", 4)
REVIEWIQ_ALLOW_LOCAL_FALLBACK = env_bool("REVIEWIQ_ALLOW_LOCAL_FALLBACK", True)

# Simple in-memory cache for Play Store analyses: app_id -> (timestamp, payload)
PLAY_STORE_CACHE: Dict[str, Tuple[float, Dict[str, Any]]] = {}
PLAY_CACHE_TTL = env_int("PLAY_CACHE_TTL", 600)
