from __future__ import annotations

import csv
import io
import html as html_lib
import json
import math
import random
import os
import time
import re
import unicodedata
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from statistics import mean
from urllib.parse import urlparse
from typing import Any, Dict, Iterable, List, Optional, Tuple

from flask import Flask, abort, jsonify, request, send_from_directory
import requests
from google_play_scraper import Sort, reviews, search, app as app_details


ROOT_DIR = Path(__file__).resolve().parent.parent
# Serve frontend build from frontend/dist after separating frontend folder
DIST_DIR = ROOT_DIR / "frontend" / "dist"


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


for env_path in (ROOT_DIR / ".env", ROOT_DIR / ".env.local", ROOT_DIR / "backend" / ".env"):
    load_env_file(env_path)


app = Flask(
    __name__,
    static_folder=str(DIST_DIR) if DIST_DIR.exists() else None,
    static_url_path="",
)


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


@app.after_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


def clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def safe_float(value: Any, default: float | None = None) -> Optional[float]:
    if value is None:
        return default
    try:
        if isinstance(value, str):
            cleaned = value.strip().replace("%", "")
            if not cleaned:
                return default
            return float(cleaned)
        return float(value)
    except (TypeError, ValueError):
        return default


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def strip_noise(text: str) -> str:
    if not text:
        return ""
    cleaned = re.sub(r"https?://\S+|www\.\S+", " ", text, flags=re.I)
    cleaned = re.sub(r"@\w+", " ", cleaned)
    cleaned = re.sub(r"#\w+", " ", cleaned)
    cleaned = re.sub(r"(.)\1{4,}", r"\1\1\1", cleaned)
    cleaned = re.sub(r"[\u0000-\u001f]+", " ", cleaned)
    cleaned = normalize_whitespace(cleaned)
    spam_tokens = [
        "buy now",
        "use code",
        "free gift",
        "limited offer",
        "subscribe",
        "promo",
        "discount",
    ]
    lowered = cleaned.lower()
    if any(token in lowered for token in spam_tokens) and len(lowered.split()) < 8:
        return ""
    return cleaned


def normalize_text(text: str) -> str:
    text = unicodedata.normalize("NFKC", text or "")
    text = text.lower()
    text = re.sub(r"[^0-9a-z\u0900-\u097f\u0980-\u09ff\u0a00-\u0aff\u0b80-\u0bff\u0c00-\u0dff\s]", " ", text)
    text = normalize_whitespace(text)
    return text


def canonical_header(key: str) -> str:
    key = normalize_text(str(key))
    aliases = {
        "review": "text",
        "review text": "text",
        "comment": "text",
        "content": "text",
        "body": "text",
        "message": "text",
        "title": "title",
        "summary": "summary",
        "rating": "rating",
        "stars": "rating",
        "score": "rating",
        "date": "date",
        "review date": "date",
        "created at": "date",
        "timestamp": "date",
        "platform": "platform",
        "source": "platform",
        "channel": "platform",
        "language": "language",
        "lang": "language",
        "region": "region",
        "country": "region",
        "market": "region",
        "device": "device",
        "device model": "device",
        "model": "device",
        "os": "os",
        "os version": "os",
        "version": "version",
        "app version": "version",
        "build": "version",
        "release": "version",
        "release date": "release_date",
        "author": "author",
        "user": "author",
        "reviewer": "author",
        "product": "product",
        "app": "product",
        "sku": "product",
        "id": "id",
    }
    return aliases.get(key, key.replace(" ", "_"))


def parse_date(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    raw = str(value).strip()
    if not raw:
        return None

    raw = raw.replace("Z", "+00:00")
    for fmt in (
        "%Y-%m-%d",
        "%Y/%m/%d",
        "%d/%m/%Y",
        "%m/%d/%Y",
        "%d-%m-%Y",
        "%Y-%m-%d %H:%M:%S",
        "%Y/%m/%d %H:%M:%S",
    ):
        try:
            return datetime.strptime(raw, fmt)
        except ValueError:
            continue

    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def to_iso_date(value: Optional[datetime]) -> str:
    if not value:
        return ""
    return value.date().isoformat()


def start_of_week(value: datetime) -> datetime:
    monday = value - timedelta(days=value.weekday())
    return datetime(monday.year, monday.month, monday.day)


def week_label(value: datetime) -> str:
    return start_of_week(value).date().isoformat()


def chunked(iterable: Iterable[Any], size: int) -> List[List[Any]]:
    batch: List[List[Any]] = []
    bucket: List[Any] = []
    for item in iterable:
        bucket.append(item)
        if len(bucket) >= size:
            batch.append(bucket)
            bucket = []
    if bucket:
        batch.append(bucket)
    return batch


POSITIVE_WORDS = {
    "good": 1.0,
    "great": 1.3,
    "excellent": 1.5,
    "amazing": 1.5,
    "awesome": 1.4,
    "fast": 0.9,
    "smooth": 0.9,
    "stable": 0.8,
    "stunning": 1.2,
    "clear": 0.8,
    "helpful": 0.8,
    "love": 1.1,
    "best": 1.3,
    "solid": 0.8,
    "clean": 0.7,
    "beautiful": 1.0,
    "cool": 0.7,
    "reliable": 1.0,
    "efficient": 0.9,
    "improved": 0.7,
    "quick": 0.7,
}


NEGATIVE_WORDS = {
    "bad": 1.0,
    "terrible": 1.5,
    "awful": 1.4,
    "slow": 0.9,
    "laggy": 1.0,
    "lag": 0.9,
    "crash": 1.5,
    "freez": 1.2,
    "freeze": 1.2,
    "bug": 1.0,
    "drain": 1.3,
    "hot": 1.0,
    "overheat": 1.5,
    "broken": 1.4,
    "issue": 1.0,
    "problem": 1.0,
    "fail": 1.3,
    "error": 1.2,
    "cluttered": 0.9,
    "confusing": 0.8,
    "worse": 1.0,
    "stuck": 1.0,
    "late": 0.7,
    "unresolved": 1.2,
    "refund": 1.0,
    "painful": 0.8,
    "annoying": 0.8,
    "disconnect": 1.0,
}


ASPECTS: Dict[str, Dict[str, Any]] = {
    "battery": {
        "label": "Battery overheating",
        "keywords": [
            "battery",
            "battery life",
            "charging",
            "drain",
            "drains",
            "power",
            "thermal",
            "overheat",
            "hot",
            "heat",
        ],
        "importance": 1.95,
        "action": "Release a thermal rollback hotfix and optimize background power usage.",
        "root_cause": "Thermal governor regression",
    },
    "camera": {
        "label": "Camera quality",
        "keywords": [
            "camera",
            "zoom",
            "focus",
            "blur",
            "stunning",
            "photo",
            "image",
            "night mode",
        ],
        "importance": 1.25,
        "action": "Keep the camera pipeline stable and address zoom blur in the next sprint.",
        "root_cause": "Image pipeline instability",
    },
    "ui": {
        "label": "UI and navigation",
        "keywords": [
            "ui",
            "interface",
            "layout",
            "design",
            "navigation",
            "dark mode",
            "theme",
            "cluttered",
        ],
        "importance": 1.05,
        "action": "Simplify the navigation and ship the missing polish in the next sprint.",
        "root_cause": "Layout friction",
    },
    "crash": {
        "label": "App crashes",
        "keywords": [
            "crash",
            "freez",
            "freeze",
            "hang",
            "stuck",
            "error",
            "launch",
            "bug",
        ],
        "importance": 2.0,
        "action": "Patch the crash path first and protect the login launch flow.",
        "root_cause": "Regression in launch or login code path",
    },
    "payment": {
        "label": "Payment flow",
        "keywords": [
            "payment",
            "checkout",
            "billing",
            "transaction",
            "card",
            "refund",
            "payment failed",
        ],
        "importance": 1.85,
        "action": "Inspect the payment gateway and add safer fallback handling.",
        "root_cause": "Checkout gateway instability",
    },
    "performance": {
        "label": "Speed and responsiveness",
        "keywords": [
            "performance",
            "slow",
            "laggy",
            "lag",
            "stutter",
            "stutters",
            "loading",
            "scrolling",
            "responsive",
        ],
        "importance": 1.55,
        "action": "Profile the slowest screens and reduce rendering overhead.",
        "root_cause": "Rendering bottleneck",
    },
    "support": {
        "label": "Support responsiveness",
        "keywords": [
            "support",
            "help",
            "ticket",
            "response",
            "replied",
            "resolved",
            "unresolved",
            "customer care",
        ],
        "importance": 1.1,
        "action": "Tighten response SLAs and surface unresolved tickets to the support queue.",
        "root_cause": "Support backlog or SLA drift",
    },
    "search": {
        "label": "Search and discovery",
        "keywords": [
            "search",
            "discover",
            "discoverability",
            "find",
            "results",
            "filter",
        ],
        "importance": 1.2,
        "action": "Improve ranking and test search relevance on the top queries.",
        "root_cause": "Ranking or indexing mismatch",
    },
    "notifications": {
        "label": "Notifications",
        "keywords": [
            "notification",
            "notifications",
            "alert",
            "push",
            "reminder",
        ],
        "importance": 0.95,
        "action": "Tune notification timing and reduce duplicate alerts.",
        "root_cause": "Notification cadence mismatch",
    },
    "storage": {
        "label": "Storage and install",
        "keywords": [
            "storage",
            "install",
            "installation",
            "size",
            "download",
            "space",
            "memory",
        ],
        "importance": 1.25,
        "action": "Reduce package size and fix install-time blockers.",
        "root_cause": "Packaging or dependency overhead",
    },
    "ads": {
        "label": "Ads and monetization",
        "keywords": [
            "ads",
            "advertising",
            "monetization",
            "subscription",
            "paywall",
        ],
        "importance": 0.8,
        "action": "Balance monetization with product trust and ad frequency.",
        "root_cause": "Ad frequency or paywall friction",
    },
    "delivery": {
        "label": "Delivery and fulfillment",
        "keywords": [
            "delivery",
            "delivered",
            "shipment",
            "shipping",
            "late",
            "order",
            "courier",
        ],
        "importance": 1.4,
        "action": "Review the delivery SLA and route delays by region.",
        "root_cause": "Fulfillment or courier delay",
    },
    "overall": {
        "label": "Overall experience",
        "keywords": ["experience", "overall", "service", "product"],
        "importance": 1.0,
        "action": "Review the strongest negative themes and cut the top irritants first.",
        "root_cause": "Cross-cutting experience issue",
    },
}


LANGUAGE_DISPLAY = {
    "Hindi": "Hindi",
    "Tamil": "Tamil",
    "Telugu": "Telugu",
    "Kannada": "Kannada",
    "Bengali": "Bengali",
    "English": "English",
}


DEFAULT_IMPORTANCE_OVERRIDES = {
    "battery": 1.95,
    "camera": 1.25,
    "ui": 1.05,
    "crash": 2.0,
    "payment": 1.85,
    "performance": 1.55,
    "support": 1.1,
    "search": 1.2,
    "notifications": 0.95,
    "storage": 1.25,
    "ads": 0.8,
    "delivery": 1.4,
    "overall": 1.0,
}


LANGUAGE_WORD_BANK = {
    "English": {
        "battery": "battery",
        "camera": "camera",
        "ui": "UI",
        "performance": "performance",
        "support": "support",
        "payment": "payment",
        "crash": "crash",
        "good": "good",
        "bad": "bad",
        "hot": "hot",
        "slow": "slow",
        "fast": "fast",
        "freeze": "freeze",
        "drain": "drains",
        "helpful": "helpful",
        "stunning": "stunning",
        "cluttered": "cluttered",
        "laggy": "laggy",
        "clear": "clear",
        "very": "very",
        "issue": "issue",
        "login": "login",
        "phone": "phone",
        "checkout": "checkout",
        "payment_failed": "payment failed",
        "smooth": "smooth",
        "stuck": "stuck",
        "unresolved": "unresolved",
    },
    "Hindi": {
        "battery": "बैटरी",
        "camera": "कैमरा",
        "ui": "इंटरफेस",
        "performance": "परफॉर्मेंस",
        "support": "सहायता",
        "payment": "भुगतान",
        "crash": "क्रैश",
        "good": "अच्छा",
        "bad": "खराब",
        "hot": "गर्म",
        "slow": "धीमा",
        "fast": "तेज",
        "freeze": "फ्रीज",
        "drain": "खत्म",
        "helpful": "मददगार",
        "stunning": "शानदार",
        "cluttered": "उलझा हुआ",
        "laggy": "लैगी",
        "clear": "साफ़",
        "very": "बहुत",
        "issue": "समस्या",
        "login": "लॉगिन",
        "phone": "फोन",
        "checkout": "चेकआउट",
        "payment_failed": "भुगतान विफल",
        "smooth": "स्मूद",
        "stuck": "अटक गया",
        "unresolved": "अनसुलझा",
    },
    "Tamil": {
        "battery": "பேட்டரி",
        "camera": "கேமரா",
        "ui": "இடைமுகம்",
        "performance": "செயல்திறன்",
        "support": "உதவி",
        "payment": "பணம்",
        "crash": "முடக்கம்",
        "good": "நல்ல",
        "bad": "மோசம்",
        "hot": "சூடு",
        "slow": "மெதுவாக",
        "fast": "வேகம்",
        "freeze": "உறை",
        "drain": "குறை",
        "helpful": "மிகவும் உதவிகரமான",
        "stunning": "அருமை",
        "cluttered": "குழப்பமான",
        "laggy": "தடை",
        "clear": "தெளிவு",
        "very": "மிகவும்",
        "issue": "பிரச்சனை",
        "login": "உள்நுழைவு",
        "phone": "போன்",
        "checkout": "செக்அவுட்",
        "payment_failed": "பணம் தோல்வி",
        "smooth": "சீரான",
        "stuck": "சிக்கி",
        "unresolved": "தீராத",
    },
    "Telugu": {
        "battery": "బ్యాటరీ",
        "camera": "కెమెరా",
        "ui": "ఇంటర్ఫేస్",
        "performance": "పర్ఫార్మెన్స్",
        "support": "సహాయం",
        "payment": "చెల్లింపు",
        "crash": "క్రాష్",
        "good": "మంచి",
        "bad": "చెడు",
        "hot": "వేడి",
        "slow": "నెమ్మది",
        "fast": "వేగం",
        "freeze": "ఫ్రీజ్",
        "drain": "డ్రైన్",
        "helpful": "సహాయక",
        "stunning": "అద్భుతం",
        "cluttered": "గందరగోళం",
        "laggy": "ల్యాగ్",
        "clear": "స్పష్ట",
        "very": "చాలా",
        "issue": "సమస్య",
        "login": "లాగిన్",
        "phone": "ఫోన్",
        "checkout": "చెకౌట్",
        "payment_failed": "చెల్లింపు విఫలం",
        "smooth": "సాఫీ",
        "stuck": "ఇరుక్కుపోయింది",
        "unresolved": "అపరిష్కృత",
    },
    "Kannada": {
        "battery": "ಬ್ಯಾಟರಿ",
        "camera": "ಕ್ಯಾಮೆರಾ",
        "ui": "ಇಂಟರ್ಫೇಸ್",
        "performance": "ಕಾರ್ಯಕ್ಷಮತೆ",
        "support": "ಸಹಾಯ",
        "payment": "ಪಾವತಿ",
        "crash": "ಕ್ರ್ಯಾಶ್",
        "good": "ಒಳ್ಳೆಯ",
        "bad": "ಕೆಟ್ಟ",
        "hot": "ಬಿಸಿ",
        "slow": "ನಿಧಾನ",
        "fast": "ವೇಗ",
        "freeze": "ಫ್ರೀಜ್",
        "drain": "ಡ್ರೈನ್",
        "helpful": "ಸಹಾಯಕ",
        "stunning": "ಅದ್ಭುತ",
        "cluttered": "ಗೊಂದಲ",
        "laggy": "ಲ್ಯಾಗ್",
        "clear": "ಸ್ಪಷ್ಟ",
        "very": "ತುಂಬಾ",
        "issue": "ಸಮಸ್ಯೆ",
        "login": "ಲಾಗಿನ್",
        "phone": "ಫೋನ್",
        "checkout": "ಚೆಕ್ಔಟ್",
        "payment_failed": "ಪಾವತಿ ವಿಫಲ",
        "smooth": "ಮೃದುವಾದ",
        "stuck": "ಸಿಲುಕಿತು",
        "unresolved": "ಅನಿರ್ಧಾರಿತ",
    },
    "Bengali": {
        "battery": "ব্যাটারি",
        "camera": "ক্যামেরা",
        "ui": "ইন্টারফেস",
        "performance": "পারফরম্যান্স",
        "support": "সাহায্য",
        "payment": "পেমেন্ট",
        "crash": "ক্র্যাশ",
        "good": "ভালো",
        "bad": "খারাপ",
        "hot": "গরম",
        "slow": "ধীর",
        "fast": "দ্রুত",
        "freeze": "ফ্রিজ",
        "drain": "ড্রেন",
        "helpful": "সহায়ক",
        "stunning": "দারুণ",
        "cluttered": "গোলমাল",
        "laggy": "ল্যাগ",
        "clear": "পরিষ্কার",
        "very": "খুব",
        "issue": "সমস্যা",
        "login": "লগইন",
        "phone": "ফোন",
        "checkout": "চেকআউট",
        "payment_failed": "পেমেন্ট ব্যর্থ",
        "smooth": "মসৃণ",
        "stuck": "আটকে",
        "unresolved": "অমীমাংসিত",
    },
}


def detect_language(text: str, explicit: Optional[str] = None) -> str:
    explicit = normalize_whitespace(str(explicit or ""))
    if explicit:
        normalized = explicit.lower()
        if normalized in {"en", "eng", "english"}:
            return "English"
        return explicit.title()
    return "English"


def translate_to_english(text: str, language: str) -> str:
    """
    Heuristic fallback is removed. Translation is now primarily handled 
    by the LLM in the enrich_rows_with_openrouter batch process.
    """
    return normalize_whitespace(text)


def sentence_split(text: str) -> List[str]:
    if not text:
        return []
    parts = re.split(r"(?<=[\.\!\?\u0964\u0965\u3002])\s+|[\n\r]+", text)
    return [part.strip() for part in parts if part and part.strip()]


def rating_to_sentiment_weight(rating: Optional[float]) -> float:
    if rating is None:
        return 0.0
    return clamp((float(rating) - 3.0) * 0.95, -2.0, 2.0)


def sentiment_from_text(text: str, rating: Optional[float] = None) -> Tuple[float, float, str]:
    normalized = normalize_text(text)
    score = 0.0
    confidence_bonus = 0.0

    for word, weight in POSITIVE_WORDS.items():
        if word in normalized:
            score += weight
            confidence_bonus += 0.03

    for word, weight in NEGATIVE_WORDS.items():
        if word in normalized:
            score -= weight
            confidence_bonus += 0.03

    if " not " in f" {normalized} " or " no " in f" {normalized} ":
        score -= 0.15

    score += rating_to_sentiment_weight(rating)

    exclamation_count = text.count("!")
    if exclamation_count:
        score += 0.12 if score > 0 else -0.12
        confidence_bonus += min(0.08, exclamation_count * 0.02)

    sentiment = math.tanh(score / 3.0)
    confidence = clamp(0.58 + abs(score) * 0.09 + confidence_bonus, 0.58, 0.98)
    label = "positive" if sentiment > 0.18 else "negative" if sentiment < -0.18 else "neutral"
    return sentiment, confidence, label


def top_counter_items(counter: Counter, limit: int = 3) -> List[Dict[str, Any]]:
    return [{"value": key, "count": count} for key, count in counter.most_common(limit)]


def canonical_feature_from_keyword(text: str) -> List[str]:
    normalized = normalize_text(text)
    found: List[str] = []
    for feature_key, info in ASPECTS.items():
        for keyword in info["keywords"]:
            if normalize_text(keyword) in normalized:
                found.append(feature_key)
                break
    return found


def extract_mentions(row: Dict[str, Any], review_id: str) -> List[Dict[str, Any]]:
    llm_mentions = analysis_mentions_from_row(row, review_id)
    if llm_mentions:
        return llm_mentions

    raw_text = strip_noise(row.get("text", ""))
    if not raw_text:
        return []

    language = detect_language(raw_text, row.get("language"))
    translated_text = translate_to_english(raw_text, language)
    rating = safe_float(row.get("rating"))
    sentences = sentence_split(translated_text) or [translated_text]

    mentions: List[Dict[str, Any]] = []
    for sentence in sentences:
        features = canonical_feature_from_keyword(sentence)
        if not features:
            continue
        sentiment, confidence, label = sentiment_from_text(sentence, rating)
        for feature_key in features:
            info = ASPECTS[feature_key]
            mentions.append(
                {
                    "review_id": review_id,
                    "feature_key": feature_key,
                    "feature_label": info["label"],
                    "sentiment": round(sentiment, 4),
                    "sentiment_label": label,
                    "confidence": round(confidence, 4),
                    "evidence": sentence.strip(),
                    "translated_evidence": sentence.strip(),
                    "rating": rating,
                }
            )

    if not mentions and rating is not None:
        sentiment, confidence, label = sentiment_from_text(translated_text, rating)
        if abs(sentiment) >= 0.2:
            mentions.append(
                {
                    "review_id": review_id,
                    "feature_key": "overall",
                    "feature_label": ASPECTS["overall"]["label"],
                    "sentiment": round(sentiment, 4),
                    "sentiment_label": label,
                    "confidence": round(confidence, 4),
                    "evidence": translated_text.strip(),
                    "translated_evidence": translated_text.strip(),
                    "rating": rating,
                }
            )

    return mentions


def canonical_review_row(raw: Dict[str, Any], index: int) -> Dict[str, Any]:
    normalized: Dict[str, Any] = {}
    for key, value in raw.items():
        key_name = canonical_header(key)
        if key_name in {"analysis", "analysis_engine"} and isinstance(value, (dict, list)):
            normalized[key_name] = value
            continue
        value_str = "" if value is None else str(value).strip()
        if key_name == "text":
            normalized[key_name] = normalize_whitespace(f"{normalized.get(key_name, '')} {value_str}")
        elif key_name in {"title", "summary"} and value_str:
            if normalized.get("text"):
                normalized["text"] = normalize_whitespace(f"{normalized['text']} {value_str}")
            else:
                normalized["text"] = value_str
        else:
            normalized[key_name] = value_str

    if not normalized.get("text"):
        text_like = [normalized.get("title", ""), normalized.get("summary", "")]
        normalized["text"] = normalize_whitespace(" ".join(part for part in text_like if part))

    normalized["id"] = normalized.get("id") or f"review-{index + 1}"
    normalized["rating"] = safe_float(normalized.get("rating"))
    normalized["date"] = to_iso_date(parse_date(normalized.get("date")))
    normalized["release_date"] = to_iso_date(parse_date(normalized.get("release_date")))
    normalized["platform"] = normalized.get("platform") or "Unknown"
    normalized["language"] = detect_language(normalized.get("text", ""), normalized.get("language"))
    normalized["region"] = normalized.get("region") or "Unknown"
    normalized["device"] = normalized.get("device") or "Unknown"
    normalized["os"] = normalized.get("os") or "Unknown"
    normalized["version"] = normalized.get("version") or "Unknown"
    normalized["product"] = normalized.get("product") or "ReviewIQ Demo"
    normalized["author"] = normalized.get("author") or f"user-{index + 1}"
    normalized["text"] = strip_noise(normalized.get("text", ""))
    return normalized


def parse_csv_text(csv_text: str) -> List[Dict[str, Any]]:
    if not csv_text or not csv_text.strip():
        return []

    text = csv_text.strip()
    try:
        sniffed = csv.Sniffer().sniff(text[:4096], delimiters=",;\t|")
        reader = csv.DictReader(io.StringIO(text), dialect=sniffed)
        fieldnames = reader.fieldnames or []
        if len(fieldnames) <= 1:
            raise ValueError("single column csv")
        rows = [canonical_review_row(row, idx) for idx, row in enumerate(reader)]
    except Exception:
        rows = []

    rows = [row for row in rows if row.get("text")]
    if rows:
        return dedupe_rows(rows)

    fallback_rows: List[Dict[str, Any]] = []
    for idx, line in enumerate(line for line in text.splitlines() if line.strip()):
        fallback_rows.append(
            canonical_review_row(
                {
                    "id": f"line-{idx + 1}",
                    "text": line.strip(),
                    "date": (datetime.utcnow() - timedelta(days=idx % 28)).date().isoformat(),
                    "platform": "Raw Text",
                    "language": detect_language(line),
                },
                idx,
            )
        )
    return dedupe_rows(fallback_rows)


def strip_html_to_text(raw_html: str) -> str:
    if not raw_html:
        return ""
    cleaned = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", " ", raw_html)
    cleaned = re.sub(r"(?is)<!--.*?-->", " ", cleaned)
    cleaned = re.sub(r"(?s)<[^>]+>", " ", cleaned)
    cleaned = html_lib.unescape(cleaned)
    return normalize_whitespace(cleaned)


def fetch_review_page_text(review_url: str) -> str:
    parsed = urlparse(review_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Please enter a valid http or https URL.")

    # More robust browser-like headers to avoid being blocked
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.google.com/",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1"
    }

    session = requests.Session()
    response = session.get(
        review_url,
        timeout=OPENROUTER_TIMEOUT_SECONDS,
        headers=headers,
    )
    response.raise_for_status()
    raw_text = response.text[:300000]
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "html" in content_type or "<html" in raw_text.lower():
        raw_text = strip_html_to_text(raw_text)
    return normalize_whitespace(raw_text)


def dedupe_rows(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    unique: List[Dict[str, Any]] = []
    for row in rows:
        key = (
            normalize_text(row.get("text", "")),
            str(row.get("rating", "")),
            row.get("date", ""),
            row.get("platform", ""),
        )
        if key in seen:
            continue
        seen.add(key)
        unique.append(row)
    return unique


# Build a lookup from normalized keywords -> canonical feature_key
# If REVIEWIQ_LLM_ONLY is enabled, don't build a hard-coded lookup so the
# LLM can supply feature keys/labels without being forced into the static
# taxonomy.
FEATURE_KEY_LOOKUP: Dict[str, str] = {}
if not env_bool("REVIEWIQ_LLM_ONLY", False):
    for feature_key, info in ASPECTS.items():
        FEATURE_KEY_LOOKUP[normalize_text(feature_key)] = feature_key
        FEATURE_KEY_LOOKUP[normalize_text(info["label"])] = feature_key
        for keyword in info["keywords"]:
            FEATURE_KEY_LOOKUP.setdefault(normalize_text(keyword), feature_key)


LLM_REVIEW_ANALYSIS_SCHEMA: Dict[str, Any] = {
    "name": "reviewiq_batch_analysis",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "reviews": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "id": {"type": "string"},
                        "language": {"type": "string"},
                        "translated_text": {"type": "string"},
                        "overall_sentiment": {"type": "number"},
                        "sentiment_label": {
                            "type": "string",
                            "enum": ["positive", "negative", "neutral", "mixed"],
                        },
                        "confidence": {"type": "number"},
                        "aspects": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "feature_key": {"type": "string"},
                                    "feature_label": {"type": "string"},
                                    "sentiment": {"type": "number"},
                                    "confidence": {"type": "number"},
                                    "severity": {
                                        "type": "string",
                                        "enum": ["critical", "high", "medium", "low"],
                                    },
                                    "evidence": {"type": "string"},
                                    "translated_evidence": {"type": "string"},
                                },
                                "required": [
                                    "feature_key",
                                    "feature_label",
                                    "sentiment",
                                    "confidence",
                                    "severity",
                                    "evidence",
                                    "translated_evidence",
                                ],
                                "additionalProperties": False,
                            },
                        },
                    },
                    "required": [
                        "id",
                        "language",
                        "translated_text",
                        "overall_sentiment",
                        "sentiment_label",
                        "confidence",
                        "aspects",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["reviews"],
        "additionalProperties": False,
    },
}

LLM_ISSUE_REWRITE_SCHEMA: Dict[str, Any] = {
    "name": "reviewiq_issue_rewrite",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "issues": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "feature_key": {"type": "string"},
                        "root_cause_summary": {"type": "string"},
                        "recommendation": {"type": "string"},
                        "business_risk_note": {"type": "string"},
                        "action_timeline": {"type": "string"},
                        "executive_summary": {"type": "string"},
                    },
                    "required": [
                        "feature_key",
                        "root_cause_summary",
                        "recommendation",
                        "business_risk_note",
                        "action_timeline",
                        "executive_summary",
                    ],
                    "additionalProperties": False,
                },
            }
        },
        "required": ["issues"],
        "additionalProperties": False,
    },
}


def normalize_feature_key(value: Any) -> str:
    normalized = normalize_text(str(value or ""))
    if not normalized:
        return "overall"
    if normalized in FEATURE_KEY_LOOKUP:
        return FEATURE_KEY_LOOKUP[normalized]
    for feature_key, info in ASPECTS.items():
        label = normalize_text(info["label"])
        if normalized in label or label in normalized:
            return feature_key
        if any(normalize_text(keyword) in normalized for keyword in info["keywords"]):
            return feature_key
    return "overall"


def build_feature_catalog() -> List[Dict[str, Any]]:
    # If pure LLM mode is enabled, avoid sending a fixed taxonomy to the
    # model so it can invent or choose appropriate feature keys/labels.
    if env_bool("REVIEWIQ_LLM_ONLY", False):
        return []
    return [
        {
            "feature_key": feature_key,
            "feature_label": info["label"],
            "keywords": info["keywords"][:6],
            "importance": round(float(info.get("importance", 1.0)), 2),
        }
        for feature_key, info in ASPECTS.items()
    ]


LLM_FULL_ANALYSIS_SCHEMA: Dict[str, Any] = {
    "name": "reviewiq_full_analysis",
    "strict": True,
    "schema": {
        "type": "object",
        "properties": {
            "summary": {
                "type": "object",
                "properties": {
                    "product_name": {"type": "string"},
                    "review_count": {"type": "integer"},
                    "avg_rating": {"type": ["number", "null"]},
                    "avg_sentiment": {"type": "number"},
                    "language_breakdown": {"type": "array"},
                    "platform_breakdown": {"type": "array"},
                    "top_issue": {"type": ["object", "null"]},
                },
                "required": ["product_name", "review_count", "avg_sentiment"],
            },
            "issues": {
                "type": "array",
                "items": {
                    "type": "object",
                    "properties": {
                        "feature_key": {"type": "string"},
                        "feature_label": {"type": "string"},
                        "impact_score": {"type": "number"},
                        "priority": {"type": "string"},
                        "executive_summary": {"type": "string"},
                        "recommendation": {"type": "string"},
                        "evidence": {"type": "array"},
                    },
                    "required": ["feature_key", "feature_label", "impact_score"],
                    "additionalProperties": True,
                },
            },
            "pipeline": {"type": "array"},
            "timeline": {"type": "object"},
            "emotion_map": {"type": "array"},
            "alerts": {"type": "array"},
            "review_previews": {"type": "array"},
        },
        "required": ["summary", "issues"],
        "additionalProperties": True,
    },
}


def llm_full_analysis(primary_rows: List[Dict[str, Any]], product_name: str, settings: Dict[str, Any]) -> Dict[str, Any]:
    """Ask the LLM to produce a full analysis (summary + issues + pipeline).

    This is used when REVIEWIQ_LLM_ONLY is enabled and OpenRouter is configured.
    """
    if not openrouter_is_enabled():
        raise RuntimeError("OpenRouter/LLM not enabled")

    # compact reviews to keep prompt size reasonable
    compact = [compact_review_record(r) for r in primary_rows][:200]
    prompt = {
        "task": "Produce an end-to-end analysis for the provided reviews. Return summary, issues (with impact scores and recommendations), a simple pipeline array, timeline, emotion_map, alerts, and review_previews.",
        "product_name": product_name,
        "settings": settings or {},
        "reviews": compact,
    }

    messages = [
        {"role": "system", "content": "You are ReviewIQ, a product intelligence engine. Return only JSON matching the requested schema."},
        {"role": "user", "content": json.dumps(prompt, ensure_ascii=False)},
    ]

    payload = openrouter_chat_completion(messages, LLM_FULL_ANALYSIS_SCHEMA, temperature=0.05, max_tokens=8192)
    return payload


def compact_review_record(row: Dict[str, Any]) -> Dict[str, Any]:
    text = normalize_whitespace(row.get("text", ""))
    return {
        "id": row.get("id") or "",
        "text": text[:420],
        "rating": row.get("rating"),
        "date": row.get("date") or "",
        "platform": row.get("platform") or "Unknown",
        "language": row.get("language") or "Unknown",
        "region": row.get("region") or "Unknown",
        "device": row.get("device") or "Unknown",
        "os": row.get("os") or "Unknown",
        "version": row.get("version") or "Unknown",
        "release_date": row.get("release_date") or "",
        "product": row.get("product") or "ReviewIQ Demo",
    }


def openrouter_is_enabled() -> bool:
    return bool(OPENROUTER_API_KEY)


def openrouter_headers() -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    if OPENROUTER_HTTP_REFERER:
        headers["HTTP-Referer"] = OPENROUTER_HTTP_REFERER
    if OPENROUTER_TITLE:
        headers["X-OpenRouter-Title"] = OPENROUTER_TITLE
    return headers


def openrouter_chat_completion(
    messages: List[Dict[str, str]],
    response_schema: Optional[Dict[str, Any]] = None,
    *,
    model: Optional[str] = None,
    temperature: float = 0.1,
    max_tokens: int = 4096,
) -> Dict[str, Any]:
    payload: Dict[str, Any] = {
        "model": model or OPENROUTER_MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if response_schema:
        payload["response_format"] = {
            "type": "json_schema",
            "json_schema": response_schema,
        }

    response = requests.post(
        f"{OPENROUTER_BASE_URL}/chat/completions",
        headers=openrouter_headers(),
        json=payload,
        timeout=OPENROUTER_TIMEOUT_SECONDS,
    )
    response.raise_for_status()
    data = response.json()
    content = data.get("choices", [{}])[0].get("message", {}).get("content", "{}")
    if isinstance(content, dict):
        parsed = content
    else:
        try:
            parsed = json.loads(content or "{}")
        except json.JSONDecodeError as exc:
            raise ValueError("OpenRouter returned invalid JSON content") from exc
    parsed["_openrouter_model"] = data.get("model", model or OPENROUTER_MODEL)
    return parsed


def build_review_batch_messages(batch_rows: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    prompt = {
        "task": "Analyze customer reviews for ReviewIQ.",
        "instructions": [
            "Detect the primary language for each review (e.g. 'English', 'Hindi', 'Tamil').",
            "Translate each review into natural, professional English and store it in 'translated_text'.",
            "Perform accurate Aspect-Based Sentiment Analysis (ABSA) on the translated text.",
            "Extract up to 3 aspect mentions per review.",
            "If a feature matches one in the taxonomy, use that feature_key. Otherwise, create a concise, new feature_key (e.g., 'charging_speed').",
            "Always provide a human-readable feature_label for any feature.",
            "Return concise evidence snippets copied or lightly normalized from the review.",
            "Overall sentiment score must be between -1.0 (very negative) and 1.0 (very positive).",
            "Keep confidence between 0.0 and 1.0.",
        ],
        "taxonomy": build_feature_catalog(),
        "reviews": [compact_review_record(row) for row in batch_rows],
    }
    return [
        {
            "role": "system",
            "content": (
                "You are ReviewIQ, an aspect-based sentiment analysis engine. "
                "Output only JSON that matches the provided schema."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(prompt, ensure_ascii=False),
        },
    ]


def build_issue_enrichment_messages(product_name: str, issues: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    prompt = {
        "product_name": product_name,
        "task": "Rewrite root cause and recommendation copy for the top product issues.",
        "instructions": [
            "Ground the explanation in the supplied issue metadata.",
            "Mention version, device, region, and language patterns when they are supplied.",
            "Keep recommendations specific and action oriented.",
            "Return a short executive_summary for each issue that a non-technical PM can read quickly.",
            "Preserve the feature_key value exactly.",
            "Keep action_timeline short, such as 'Patch before Friday' or 'Fix in the next sprint'.",
        ],
        "issues": issues,
    }
    return [
        {
            "role": "system",
            "content": (
                "You are ReviewIQ's recommendation engine. "
                "Return only JSON that matches the provided schema."
            ),
        },
        {
            "role": "user",
            "content": json.dumps(prompt, ensure_ascii=False),
        },
    ]


def analysis_mentions_from_row(row: Dict[str, Any], review_id: str) -> List[Dict[str, Any]]:
    analysis = row.get("analysis")
    if not isinstance(analysis, dict):
        return []

    translated_text = normalize_whitespace(
        str(analysis.get("translated_text") or row.get("translated_text") or translate_to_english(row.get("text", ""), row.get("language") or "English"))
    )
    overall_sentiment = safe_float(analysis.get("overall_sentiment"), 0.0) or 0.0
    overall_label = str(analysis.get("sentiment_label") or ("positive" if overall_sentiment > 0.18 else "negative" if overall_sentiment < -0.18 else "neutral")).lower()
    overall_confidence = clamp(safe_float(analysis.get("confidence"), 0.7) or 0.7, 0.0, 1.0)

    mentions: List[Dict[str, Any]] = []
    for aspect in analysis.get("aspects") or []:
        if not isinstance(aspect, dict):
            continue
        # Prefer the LLM-provided feature_key/feature_label when in LLM-only
        # mode; otherwise normalize to the static taxonomy.
        provided_key = aspect.get("feature_key") or aspect.get("feature_label")
        if env_bool("REVIEWIQ_LLM_ONLY", False):
            feature_key = (provided_key and normalize_text(provided_key)) or "overall"
            feature_label = aspect.get("feature_label") or provided_key or feature_key
            info = {"label": feature_label}
        else:
            feature_key = normalize_feature_key(aspect.get("feature_key") or aspect.get("feature_label"))
            info = ASPECTS.get(feature_key, ASPECTS["overall"])
        sentiment = safe_float(aspect.get("sentiment"), overall_sentiment)
        if sentiment is None:
            sentiment = overall_sentiment
        confidence = clamp(safe_float(aspect.get("confidence"), overall_confidence) or overall_confidence, 0.0, 1.0)
        severity = str(aspect.get("severity") or ("critical" if sentiment <= -0.75 else "high" if sentiment <= -0.45 else "medium" if sentiment <= -0.2 else "low")).lower()
        evidence = normalize_whitespace(str(aspect.get("evidence") or row.get("text") or ""))
        translated_evidence = normalize_whitespace(str(aspect.get("translated_evidence") or translated_text or evidence))
        mentions.append(
            {
                "review_id": review_id,
                "feature_key": feature_key,
                "feature_label": info.get("label") if isinstance(info, dict) else info["label"],
                "sentiment": round(float(sentiment), 4),
                "sentiment_label": overall_label if feature_key == "overall" else ("positive" if sentiment > 0.18 else "negative" if sentiment < -0.18 else "neutral"),
                "confidence": round(float(confidence), 4),
                "evidence": evidence,
                "translated_evidence": translated_evidence,
                "rating": row.get("rating"),
                "severity": severity,
            }
        )

    if not mentions:
        mentions.append(
            {
                "review_id": review_id,
                "feature_key": "overall",
                "feature_label": ASPECTS["overall"]["label"],
                "sentiment": round(overall_sentiment, 4),
                "sentiment_label": overall_label,
                "confidence": round(overall_confidence, 4),
                "evidence": normalize_whitespace(str(row.get("text", ""))),
                "translated_evidence": translated_text,
                "rating": row.get("rating"),
                "severity": "low" if abs(overall_sentiment) < 0.2 else "medium",
            }
        )

    return mentions


def analyze_openrouter_batch(batch_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    messages = build_review_batch_messages(batch_rows)
    payload = openrouter_chat_completion(messages, LLM_REVIEW_ANALYSIS_SCHEMA, temperature=0.1, max_tokens=6144)
    reviews = payload.get("reviews") or []
    mapping: Dict[str, Dict[str, Any]] = {}
    for item in reviews:
        if not isinstance(item, dict):
            continue
        review_id = str(item.get("id") or "").strip()
        if not review_id:
            continue
        mapping[review_id] = {**item, "_openrouter_model": payload.get("_openrouter_model")}
    return mapping


def enrich_rows_with_openrouter(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not rows or not openrouter_is_enabled():
        return rows, {
            "provider": "heuristic",
            "requested_model": None,
            "model_used": None,
            "enabled": False,
        }

    batch_size = max(4, min(24, REVIEWIQ_BATCH_SIZE))
    batches = chunked(rows, batch_size)
    resolved_rows = [dict(row) for row in rows]
    analysis_by_id: Dict[str, Dict[str, Any]] = {}
    used_local_fallback = False

    def build_local_analysis(row: Dict[str, Any]) -> Dict[str, Any]:
        review_id = row.get("id") or ""
        heuristic_mentions = extract_mentions({k: v for k, v in row.items() if k != "analysis"}, review_id)
        specific_mentions = [mention for mention in heuristic_mentions if mention.get("feature_key") != "overall"]
        selected_mentions = specific_mentions or heuristic_mentions
        sentiment, confidence, label = sentiment_from_text(row.get("text", ""), row.get("rating"))
        aspects = [
            {
                "feature_key": mention["feature_key"],
                "feature_label": mention["feature_label"],
                "sentiment": mention["sentiment"],
                "confidence": mention["confidence"],
                "severity": mention.get("severity") or ("critical" if mention["sentiment"] <= -0.75 else "high" if mention["sentiment"] <= -0.45 else "medium" if mention["sentiment"] <= -0.2 else "low"),
                "evidence": mention.get("evidence") or row.get("text", ""),
                "translated_evidence": mention.get("translated_evidence") or translate_to_english(row.get("text", ""), row.get("language") or "English"),
            }
            for mention in selected_mentions
        ]
        return {
            "id": review_id,
            "language": row.get("language") or detect_language(row.get("text", ""), row.get("language")),
            "translated_text": translate_to_english(row.get("text", ""), row.get("language") or "English"),
            "overall_sentiment": sentiment,
            "sentiment_label": label,
            "confidence": confidence,
            "aspects": aspects,
        }

    max_workers = max(1, min(REVIEWIQ_MAX_WORKERS, len(batches)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(analyze_openrouter_batch, batch): batch for batch in batches}
        for future in as_completed(future_map):
            batch = future_map[future]
            try:
                batch_mapping = future.result()
                analysis_by_id.update(batch_mapping)
            except Exception:
                if REVIEWIQ_ALLOW_LOCAL_FALLBACK:
                    used_local_fallback = True
                    for row in batch:
                        analysis_by_id[row.get("id") or ""] = build_local_analysis(row)
                else:
                    raise

    for row in resolved_rows:
        analysis = analysis_by_id.get(row.get("id") or "")
        if analysis:
            row["analysis"] = analysis
            row["language"] = analysis.get("language") or row.get("language") or "English"
            row["translated_text"] = normalize_whitespace(str(analysis.get("translated_text") or translate_to_english(row.get("text", ""), row.get("language") or "English")))
        else:
            row["analysis"] = build_local_analysis(row)
            row["translated_text"] = row["analysis"]["translated_text"]

    engine_info = {
        "provider": "openrouter",
        "requested_model": OPENROUTER_MODEL,
        "model_used": None,
        "enabled": True,
        "batch_size": batch_size,
        "fallback_used": used_local_fallback,
    }
    if analysis_by_id:
        engine_info["model_used"] = next(
            (item.get("_openrouter_model") for item in analysis_by_id.values() if item.get("_openrouter_model")),
            OPENROUTER_MODEL,
        )
    return resolved_rows, engine_info


def enrich_issues_with_openrouter(product_name: str, issues: List[Dict[str, Any]], rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Optional[Dict[str, Any]]]:
    if not issues or not openrouter_is_enabled():
        return issues, None

    top_issues = issues[:4]
    evidence_rows = rows[:24]
    compact_issues = []
    for item in top_issues:
        compact_issues.append(
            {
                "feature_key": item["feature_key"],
                "feature_label": item["feature_label"],
                "impact_score": item["impact_score"],
                "priority": item["priority"],
                "trend_growth_pct": item["trend_growth_pct"],
                "complaint_count": item["complaint_count"],
                "complaint_share": item["complaint_share"],
                "avg_sentiment": item["avg_sentiment"],
                "negativity": item["negativity"],
                "confidence": item["confidence"],
                "action_timeline": item["action_timeline"],
                "root_cause": item["root_cause"],
                "evidence": item["evidence"][:3],
            }
        )

    try:
        payload = openrouter_chat_completion(
            build_issue_enrichment_messages(product_name, compact_issues),
            LLM_ISSUE_REWRITE_SCHEMA,
            temperature=0.15,
            max_tokens=3072,
        )
    except Exception:
        if not REVIEWIQ_ALLOW_LOCAL_FALLBACK:
            raise
        return issues, {
            "provider": "heuristic",
            "model_used": None,
            "enabled": False,
            "fallback_used": True,
        }

    issue_updates = payload.get("issues") or []
    update_map = {
        str(item.get("feature_key") or ""): item
        for item in issue_updates
        if isinstance(item, dict) and item.get("feature_key")
    }
    for item in issues:
        update = update_map.get(item["feature_key"])
        if not update:
            continue
        if update.get("root_cause_summary"):
            item["root_cause"]["summary"] = update["root_cause_summary"]
        if update.get("recommendation"):
            item["recommendation"] = update["recommendation"]
        if update.get("business_risk_note"):
            item["business_risk"]["note"] = update["business_risk_note"]
        if update.get("action_timeline"):
            item["action_timeline"] = update["action_timeline"]
        if update.get("executive_summary"):
            item["executive_summary"] = update["executive_summary"]
    analysis_summary = {
        "provider": "openrouter",
        "requested_model": OPENROUTER_MODEL,
        "model_used": payload.get("_openrouter_model") or OPENROUTER_MODEL,
        "enabled": True,
        "fallback_used": False,
        "review_sample_size": len(evidence_rows),
    }
    return issues, analysis_summary


def importance_for_feature(feature_key: str, settings: Dict[str, Any] | None) -> float:
    base = DEFAULT_IMPORTANCE_OVERRIDES.get(feature_key, 1.0)
    if settings and feature_key in settings:
        override = safe_float(settings.get(feature_key), base)
        if override is not None:
            return clamp(float(override), 0.5, 3.0)
    return base


def feature_root_cause(feature_key: str, version: str, device: str, region: str, language: str) -> str:
    base = ASPECTS[feature_key]["root_cause"]
    if feature_key == "battery":
        return f"{base} concentrated in {version}, {device}, and {region}."
    if feature_key == "crash":
        return f"{base} appears in {version} and is strongest on {device} in {region}."
    if feature_key == "payment":
        return f"{base} is likely tied to {version} checkout flow changes."
    if feature_key == "camera":
        return f"{base} is more visible on {device} among {language} reviews."
    return f"{base} appears concentrated in {version}, {device}, and {region}."


def feature_recommendation(feature_key: str, priority: str) -> str:
    info = ASPECTS[feature_key]
    if feature_key == "battery":
        return "Release a hotfix that rolls back the thermal governor change and verify Android 12 power usage."
    if feature_key == "crash":
        return "Patch the crash path first, protect login and launch flows, and ship an emergency stability build."
    if feature_key == "payment":
        return "Audit the gateway, add fallback handling, and replay failed transactions for affected users."
    if feature_key == "camera":
        return "Tune the image pipeline and ship the zoom fix in the next sprint while preserving current quality."
    if feature_key == "ui":
        return "Simplify the navigation, reduce visual clutter, and validate the design with a quick usability pass."
    if feature_key == "support":
        return "Rebalance support routing and reduce SLA drift on unresolved tickets."
    if feature_key == "performance":
        return "Profile the slowest screens and remove the largest rendering bottlenecks."
    if feature_key == "search":
        return "Retune ranking and search indexing on the top queries before expanding the feature set."
    if feature_key == "delivery":
        return "Investigate region-level fulfillment delays and reroute late shipments."
    return info["action"]


def action_timeline(priority: str, growth_pct: float) -> str:
    if priority == "HIGH":
        if growth_pct >= 30:
            return "Patch before Friday"
        return "Fix in the next sprint"
    if priority == "MEDIUM":
        return "Investigate this sprint"
    return "Add to backlog"


def estimated_churn_risk(impact_score: float, growth_pct: float, complaint_share: float) -> float:
    risk = 1.1 + (impact_score * 0.02) + max(0.0, growth_pct) * 0.04 + complaint_share * 6.0
    return round(clamp(risk, 0.8, 12.0), 1)


def build_feature_aggregates(rows: List[Dict[str, Any]], settings: Dict[str, Any] | None = None) -> Tuple[Dict[str, Any], List[Dict[str, Any]], Counter, Counter, Counter, Counter]:
    mentions_by_feature: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    counts_by_feature: Counter = Counter()
    negative_counts_by_feature: Counter = Counter()
    positive_counts_by_feature: Counter = Counter()
    weekly_counts_by_feature: Dict[str, Counter] = defaultdict(Counter)

    languages = Counter()
    platforms = Counter()
    regions = Counter()
    versions = Counter()

    for row in rows:
        language = row.get("language") or "English"
        platforms[row.get("platform") or "Unknown"] += 1
        languages[language] += 1
        regions[row.get("region") or "Unknown"] += 1
        versions[row.get("version") or "Unknown"] += 1

        review_id = row.get("id") or f"review-{len(mentions_by_feature) + 1}"
        mentions = extract_mentions(row, review_id)
        for mention in mentions:
            feature_key = mention["feature_key"]
            mentions_by_feature[feature_key].append({**mention, **row})
            counts_by_feature[feature_key] += 1
            if mention["sentiment"] < -0.12:
                negative_counts_by_feature[feature_key] += 1
            elif mention["sentiment"] > 0.12:
                positive_counts_by_feature[feature_key] += 1

            review_date = parse_date(row.get("date"))
            if review_date:
                weekly_counts_by_feature[feature_key][week_label(review_date)] += 1

    feature_summary: Dict[str, Any] = {}
    total_mentions = sum(counts_by_feature.values()) or 1
    total_reviews = len(rows) or 1
    all_weeks = sorted(
        {
            week
            for counter in weekly_counts_by_feature.values()
            for week in counter.keys()
        }
    )
    if not all_weeks:
        all_weeks = [datetime.utcnow().date().isoformat()]

    feature_labels = {
        m["feature_key"]: m.get("feature_label") or m["feature_key"]
        for row in rows
        for m in row.get("analysis", {}).get("aspects", [])
    }

    for feature_key in mentions_by_feature.keys():
        feature_mentions = mentions_by_feature.get(feature_key, [])
        if not feature_mentions:
            continue
        
        info = ASPECTS.get(feature_key, {
            "label": feature_labels.get(feature_key, feature_key),
            "action": f"Address feedback regarding {feature_labels.get(feature_key, feature_key)}.",
            "root_cause": "Community reported issue",
            "importance": 1.0
        })

        sentiments = [item["sentiment"] for item in feature_mentions]
        confidences = [item["confidence"] for item in feature_mentions]
        complaint_count = negative_counts_by_feature[feature_key] + 0.35 * max(0, counts_by_feature[feature_key] - negative_counts_by_feature[feature_key] - positive_counts_by_feature[feature_key])
        complaint_share = complaint_count / total_reviews
        avg_sentiment = mean(sentiments)
        negativity = mean(max(0.0, -item["sentiment"]) for item in feature_mentions)
        confidence = mean(confidences)

        weekly_counts = weekly_counts_by_feature[feature_key]
        week_values = [weekly_counts.get(week, 0) for week in all_weeks]
        current_week = week_values[-1] if week_values else 0
        previous_window = week_values[-4:-1] if len(week_values) >= 4 else week_values[:-1]
        if previous_window:
            previous_avg = sum(previous_window) / max(1, len(previous_window))
        else:
            previous_avg = 0.0
        if previous_avg <= 0:
            growth_pct = 100.0 if current_week > 0 else 0.0
        else:
            growth_pct = ((current_week - previous_avg) / previous_avg) * 100.0
        trend_multiplier = clamp(1.0 + (growth_pct / 100.0), 0.35, 2.4)
        # Scale complaint frequency against a practical decision threshold so
        # the dashboard can distinguish between background noise and urgent issues.
        frequency = clamp(complaint_count / max(1.0, total_reviews * 0.18), 0.0, 1.0)
        importance = importance_for_feature(feature_key, settings)
        raw_impact = frequency * clamp(negativity, 0.05, 1.0) * trend_multiplier * importance
        impact_score = round(clamp(raw_impact * 100.0, 0.0, 100.0))
        priority = "HIGH" if impact_score >= 72 else "MEDIUM" if impact_score >= 42 else "LOW"
        severity = "critical" if impact_score >= 82 else "high" if impact_score >= 62 else "medium" if impact_score >= 40 else "low"
        direction = "rising" if growth_pct >= 15 else "falling" if growth_pct <= -10 else "stable"
        action_time = action_timeline(priority, growth_pct)

        version_counter = Counter(item.get("version") or "Unknown" for item in feature_mentions)
        device_counter = Counter(item.get("device") or "Unknown" for item in feature_mentions)
        region_counter = Counter(item.get("region") or "Unknown" for item in feature_mentions)
        language_counter = Counter(item.get("language") or "Unknown" for item in feature_mentions)
        platform_counter = Counter(item.get("platform") or "Unknown" for item in feature_mentions)

        top_version, top_version_count = version_counter.most_common(1)[0]
        top_device, top_device_count = device_counter.most_common(1)[0]
        top_region, top_region_count = region_counter.most_common(1)[0]
        top_language, top_language_count = language_counter.most_common(1)[0]

        evidence = [
            {
                "text": item.get("text", ""),
                "translated_text": normalize_whitespace(
                    str(item.get("translated_text") or translate_to_english(item.get("text", ""), item.get("language") or "English"))
                ),
                "date": item.get("date", ""),
                "rating": item.get("rating"),
                "platform": item.get("platform", "Unknown"),
                "language": item.get("language", "English"),
                "region": item.get("region", "Unknown"),
                "device": item.get("device", "Unknown"),
                "version": item.get("version", "Unknown"),
            }
            for item in sorted(
                feature_mentions,
                key=lambda entry: (entry.get("rating") or 3.0, entry.get("confidence", 0.0), entry.get("date", "")),
            )[:3]
        ]

        feature_summary[feature_key] = {
            "id": feature_key,
            "feature_key": feature_key,
            "feature_label": info["label"],
            "count": counts_by_feature[feature_key],
            "complaint_count": round(complaint_count, 2),
            "complaint_share": round(complaint_share, 4),
            "mentions": counts_by_feature[feature_key],
            "negative_mentions": negative_counts_by_feature[feature_key],
            "positive_mentions": positive_counts_by_feature[feature_key],
            "avg_sentiment": round(avg_sentiment, 4),
            "negativity": round(negativity, 4),
            "confidence": round(confidence, 4),
            "trend_growth_pct": round(growth_pct, 1),
            "trend_multiplier": round(trend_multiplier, 4),
            "importance_weight": round(importance, 3),
            "impact_score": int(impact_score),
            "priority": priority,
            "severity": severity,
            "direction": direction,
            "action_timeline": action_time,
            "recommendation": feature_recommendation(feature_key, priority),
            "business_risk": {
                "churn_risk_pct": estimated_churn_risk(impact_score, growth_pct, complaint_share),
                "unresolved_days": 7 if priority == "HIGH" else 14 if priority == "MEDIUM" else 21,
                "note": (
                    f"If unresolved for the next {7 if priority == 'HIGH' else 14 if priority == 'MEDIUM' else 21} days, "
                    f"the issue could drag ratings and retention for {feature_key} users."
                ),
            },
            "root_cause": {
                "summary": feature_root_cause(feature_key, top_version, top_device, top_region, top_language),
                "version": top_version,
                "version_share": round(top_version_count / max(1, len(feature_mentions)), 4),
                "device": top_device,
                "device_share": round(top_device_count / max(1, len(feature_mentions)), 4),
                "region": top_region,
                "region_share": round(top_region_count / max(1, len(feature_mentions)), 4),
                "language": top_language,
                "language_share": round(top_language_count / max(1, len(feature_mentions)), 4),
                "causal_hypothesis": ASPECTS[feature_key]["root_cause"],
            },
            "evidence": evidence,
            "weekly_counts": [{"week": week, "count": weekly_counts.get(week, 0)} for week in all_weeks],
            "platform_breakdown": top_counter_items(platform_counter, 3),
            "region_breakdown": top_counter_items(region_counter, 3),
            "device_breakdown": top_counter_items(device_counter, 3),
        }

    return feature_summary, all_weeks, languages, platforms, regions, versions


def build_timeline(feature_summary: Dict[str, Any], all_weeks: List[str]) -> Dict[str, Any]:
    ranked_source = [item for item in feature_summary.values() if item["feature_key"] != "overall"] or list(feature_summary.values())
    ranked = sorted(ranked_source, key=lambda item: item["impact_score"], reverse=True)[:4]
    palette = ["#f97316", "#55d6be", "#60a5fa", "#f7a440"]
    series = []
    for idx, item in enumerate(ranked):
        series.append(
            {
                "feature_key": item["feature_key"],
                "label": item["feature_label"],
                "values": [point["count"] for point in item.get("weekly_counts", [])],
                "color": palette[idx % len(palette)],
            }
        )

    totals = [0 for _ in all_weeks]
    for item in ranked:
        for idx, point in enumerate(item.get("weekly_counts", [])):
            totals[idx] += point["count"]

    annotations = []
    if ranked:
        top = ranked[0]
        if top["root_cause"]["version"] != "Unknown":
            annotations.append(
                {
                    "week": all_weeks[-1] if all_weeks else "",
                    "label": f"{top['root_cause']['version']} spike",
                }
            )

    return {
        "weeks": all_weeks,
        "series": series,
        "totals": totals,
        "annotations": annotations,
    }


def build_emotion_map(feature_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows_source = [item for item in feature_summary.values() if item["feature_key"] != "overall"] or list(feature_summary.values())
    rows = sorted(rows_source, key=lambda item: item["impact_score"], reverse=True)
    emotion_map = []
    for item in rows:
        total = max(1, item["mentions"])
        positive_share = item["positive_mentions"] / total
        negative_share = item["negative_mentions"] / total
        neutral_share = max(0.0, 1.0 - positive_share - negative_share)
        emotion_map.append(
            {
                "feature_key": item["feature_key"],
                "label": item["feature_label"],
                "sentiment_score": item["avg_sentiment"],
                "positive_share": round(positive_share, 4),
                "negative_share": round(negative_share, 4),
                "neutral_share": round(neutral_share, 4),
                "count": item["count"],
                "priority": item["priority"],
            }
        )
    return emotion_map[:8]


def build_alerts(feature_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    alerts = []
    ranked_source = [item for item in feature_summary.values() if item["feature_key"] != "overall"] or list(feature_summary.values())
    for item in sorted(ranked_source, key=lambda entry: entry["impact_score"], reverse=True):
        if item["trend_growth_pct"] >= 20 or item["priority"] == "HIGH":
            affected_segment = f"{item['root_cause']['device']} users in {item['root_cause']['region']}"
            alerts.append(
                {
                    "feature_key": item["feature_key"],
                    "feature_label": item["feature_label"],
                    "message": (
                        f"{item['feature_label']} complaints up {item['trend_growth_pct']:.1f}% this week. "
                        f"Spike is concentrated in {affected_segment}."
                    ),
                    "severity": item["severity"],
                    "growth_pct": round(item["trend_growth_pct"], 1),
                    "affected_segment": affected_segment,
                    "version": item["root_cause"]["version"],
                    "confidence": item["confidence"],
                    "days_to_act": 4 if item["priority"] == "HIGH" else 7,
                }
            )
    return alerts[:5]


def build_competitor_gap(primary_summary: Dict[str, Any], competitor_summary: Dict[str, Any]) -> List[Dict[str, Any]]:
    gap_rows = []
    all_features = sorted(set(primary_summary.keys()) | set(competitor_summary.keys()))
    for feature_key in all_features:
        ours = primary_summary.get(feature_key)
        theirs = competitor_summary.get(feature_key)
        if not ours or not theirs:
            continue
        our_sentiment = ours["avg_sentiment"]
        comp_sentiment = theirs["avg_sentiment"]
        gap = round(our_sentiment - comp_sentiment, 4)
        status = "lagging" if gap < -0.08 else "ahead" if gap > 0.08 else "parity"
        if status == "parity" and abs(gap) < 0.03:
            continue
        gap_rows.append(
            {
                "feature_key": feature_key,
                "feature_label": ours["feature_label"],
                "our_sentiment": round(our_sentiment, 4),
                "competitor_sentiment": round(comp_sentiment, 4),
                "gap": gap,
                "status": status,
                "recommendation": (
                    f"Your {ours['feature_label'].lower()} sentiment is {'worse' if gap < 0 else 'better'} than the competitor by {abs(gap):.2f}."
                ),
            }
        )
    return sorted(gap_rows, key=lambda item: item["gap"])[:6]


def build_processed_previews(rows: List[Dict[str, Any]], limit: int = 12) -> List[Dict[str, Any]]:
    previews = []
    scored_rows = []
    for row in rows:
        mentions = extract_mentions(row, row.get("id") or "preview")
        analysis = row.get("analysis") if isinstance(row.get("analysis"), dict) else {}
        if analysis:
            score = safe_float(analysis.get("overall_sentiment"), 0.0) or 0.0
        else:
            score = min((item["sentiment"] for item in mentions), default=sentiment_from_text(row.get("text", ""), row.get("rating"))[0])
        scored_rows.append((score, row, mentions))

    for score, row, mentions in sorted(scored_rows, key=lambda entry: entry[0])[:limit]:
        analysis = row.get("analysis") if isinstance(row.get("analysis"), dict) else {}
        previews.append(
            {
                "id": row.get("id"),
                "text": row.get("text", ""),
                "translated_text": normalize_whitespace(
                    str(
                        row.get("translated_text")
                        or (analysis.get("translated_text") if analysis else "")
                        or translate_to_english(row.get("text", ""), row.get("language") or "English")
                    )
                ),
                "language": row.get("language") or "English",
                "platform": row.get("platform") or "Unknown",
                "rating": row.get("rating"),
                "date": row.get("date"),
                "region": row.get("region"),
                "device": row.get("device"),
                "version": row.get("version"),
                "sentiment_score": round(score, 4),
                "aspects": [
                    {
                        "feature_key": item["feature_key"],
                        "feature_label": item["feature_label"],
                        "sentiment": item["sentiment"],
                        "confidence": item["confidence"],
                    }
                    for item in mentions[:3]
                ],
            }
        )
    return previews


def build_pipeline(
    rows: List[Dict[str, Any]],
    feature_summary: Dict[str, Any],
    languages: Counter,
    analysis_engine: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    total_mentions = sum(item["mentions"] for item in feature_summary.values())
    rising = sum(1 for item in feature_summary.values() if item["direction"] == "rising")
    critical = sum(1 for item in feature_summary.values() if item["priority"] == "HIGH")
    top_language = languages.most_common(1)[0][0] if languages else "English"
    engine_name = "OpenRouter free LLMs" if analysis_engine and analysis_engine.get("enabled") else "local fallback"
    return [
        {
            "name": "1. Ingestion",
            "detail": f"Parsed {len(rows)} reviews and normalized noisy text.",
            "progress": 100,
        },
        {
            "name": "2. ABSA",
            "detail": f"Extracted {total_mentions} feature-level sentiment signals across {len(feature_summary)} issues with {engine_name}.",
            "progress": 100,
        },
        {
            "name": "3. Trend Intelligence",
            "detail": f"Detected {rising} rising feature themes and {critical} critical spikes.",
            "progress": 100,
        },
        {
            "name": "4. Multilingual Layer",
            "detail": f"Processed reviews in {len(languages)} languages, led by {top_language}.",
            "progress": 100,
        },
        {
            "name": "5. Priority Scoring",
            "detail": "Applied the transparent impact formula to rank issues.",
            "progress": 100,
        },
        {
            "name": "6. Recommendations",
            "detail": "Generated root-cause backed actions for product owners.",
            "progress": 100,
        },
        {
            "name": "7. Dashboard",
            "detail": "Prepared a PM-friendly action list with evidence and priority.",
            "progress": 100,
        },
    ]


def build_summary(
    product_name: str,
    primary_rows: List[Dict[str, Any]],
    feature_summary: Dict[str, Any],
    languages: Counter,
    platforms: Counter,
    regions: Counter,
    versions: Counter,
    alerts: List[Dict[str, Any]],
) -> Dict[str, Any]:
    ratings = [row["rating"] for row in primary_rows if row.get("rating") is not None]
    all_mentions = [item for feature in feature_summary.values() for item in feature.get("evidence", [])]
    sentiment_values = [feature["avg_sentiment"] for feature in feature_summary.values()]
    ranked_source = [item for item in feature_summary.values() if item["feature_key"] != "overall"] or list(feature_summary.values())
    top_issue = max(ranked_source, key=lambda item: item["impact_score"], default=None)
    impacted_users = round(len(primary_rows) * (top_issue["complaint_share"] if top_issue else 0.0)) if top_issue else 0
    language_total = sum(languages.values()) or 1

    return {
        "product_name": product_name,
        "review_count": len(primary_rows),
        "avg_rating": round(mean(ratings), 2) if ratings else None,
        "avg_sentiment": round(mean(sentiment_values), 4) if sentiment_values else 0.0,
        "language_breakdown": top_counter_items(languages, 6),
        "platform_breakdown": top_counter_items(platforms, 6),
        "region_breakdown": top_counter_items(regions, 6),
        "version_breakdown": top_counter_items(versions, 6),
        "critical_issue_count": sum(1 for item in feature_summary.values() if item["priority"] == "HIGH"),
        "trend_alert_count": len(alerts),
        "impacted_users_estimate": impacted_users,
        "coverage_ratio": round(language_total / max(1, len(primary_rows)), 4),
        "top_issue": {
            "feature_key": top_issue["feature_key"] if top_issue else None,
            "feature_label": top_issue["feature_label"] if top_issue else None,
            "impact_score": top_issue["impact_score"] if top_issue else None,
            "priority": top_issue["priority"] if top_issue else None,
            "recommendation": top_issue["recommendation"] if top_issue else None,
        }
        if top_issue
        else None,
    }


def analyze_reviews(
    primary_rows: List[Dict[str, Any]],
    competitor_rows: Optional[List[Dict[str, Any]]] = None,
    product_name: str = "ReviewIQ Product",
    settings: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    competitor_rows = competitor_rows or []
    settings = settings or {}

    primary_rows = dedupe_rows([canonical_review_row(row, idx) for idx, row in enumerate(primary_rows) if row.get("text")])
    competitor_rows = dedupe_rows([canonical_review_row(row, idx) for idx, row in enumerate(competitor_rows) if row.get("text")])

    # If explicitly configured to be LLM-only, ask the LLM to produce the full analysis
    if openrouter_is_enabled() and env_bool("REVIEWIQ_LLM_ONLY", False):
        try:
            llm_payload = llm_full_analysis(primary_rows, product_name, settings)
            generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
            analysis_engine = {
                "provider": "openrouter",
                "requested_model": OPENROUTER_MODEL,
                "model_used": llm_payload.get("_openrouter_model") or OPENROUTER_MODEL,
                "batch_size": REVIEWIQ_BATCH_SIZE,
                "fallback_used": False,
                "enabled": True,
            }
            return {
                "generated_at": generated_at,
                "summary": llm_payload.get("summary", {}),
                "pipeline": llm_payload.get("pipeline", []),
                "issues": llm_payload.get("issues", []),
                "detected_features": [i.get("feature_label") for i in llm_payload.get("issues", [])],
                "alerts": llm_payload.get("alerts", []),
                "emotion_map": llm_payload.get("emotion_map", []),
                "timeline": llm_payload.get("timeline", {}),
                "competitive_gap": [],
                "review_previews": llm_payload.get("review_previews", []),
                "processed_reviews": len(primary_rows),
                "settings_applied": settings,
                "analysis_engine": analysis_engine,
            }
        except Exception as exc:
            # Fall back to local heuristic pipeline if LLM fails
            print(f"LLM full analysis failed, falling back to heuristics: {exc}")

    primary_rows, primary_engine = enrich_rows_with_openrouter(primary_rows)
    competitor_engine = None
    if competitor_rows:
        competitor_rows, competitor_engine = enrich_rows_with_openrouter(competitor_rows)

    primary_summary, all_weeks, languages, platforms, regions, versions = build_feature_aggregates(primary_rows, settings)
    timeline = build_timeline(primary_summary, all_weeks)
    emotion_map = build_emotion_map(primary_summary)
    alerts = build_alerts(primary_summary)
    pipeline = build_pipeline(primary_rows, primary_summary, languages, primary_engine)
    review_previews = build_processed_previews(primary_rows)

    competitor_gap = []
    competitor_summary = {}
    if competitor_rows:
        competitor_summary, _, _, _, _, _ = build_feature_aggregates(competitor_rows, settings)
        competitor_gap = build_competitor_gap(primary_summary, competitor_summary)

    issue_source = [item for item in primary_summary.values() if item["feature_key"] != "overall"] or list(primary_summary.values())
    issues = sorted(issue_source, key=lambda item: item["impact_score"], reverse=True)

    focus_feature = settings.get("focus_feature")
    if focus_feature:
        issues = [i for i in issues if i["feature_label"] == focus_feature or i["feature_key"] == focus_feature]

    issues, recommendation_engine = enrich_issues_with_openrouter(product_name, issues, primary_rows)
    
    detected_features = sorted(list({
        i["feature_label"] for i in issue_source
    }))
    summary = build_summary(
        product_name=product_name,
        primary_rows=primary_rows,
        feature_summary=primary_summary,
        languages=languages,
        platforms=platforms,
        regions=regions,
        versions=versions,
        alerts=alerts,
    )
    if summary.get("top_issue") and issues:
        summary["top_issue"]["recommendation"] = issues[0].get("recommendation", summary["top_issue"].get("recommendation"))
    recommendation_model = recommendation_engine.get("model_used") if recommendation_engine else None
    analysis_engine = {
        "provider": primary_engine.get("provider", "heuristic"),
        "requested_model": primary_engine.get("requested_model"),
        "model_used": recommendation_model or primary_engine.get("model_used"),
        "batch_size": primary_engine.get("batch_size"),
        "fallback_used": bool(primary_engine.get("fallback_used") or (recommendation_engine or {}).get("fallback_used")),
        "enabled": bool(primary_engine.get("enabled")),
    }
    if competitor_engine:
        analysis_engine["competitor_model_used"] = competitor_engine.get("model_used")

    generated_at = datetime.utcnow().replace(microsecond=0).isoformat() + "Z"
    return {
        "generated_at": generated_at,
        "summary": summary,
        "pipeline": pipeline,
        "issues": issues,
        "detected_features": detected_features,
        "alerts": alerts,
        "emotion_map": emotion_map,
        "timeline": timeline,
        "competitive_gap": competitor_gap,
        "review_previews": review_previews,
        "processed_reviews": len(primary_rows),
        "settings_applied": settings,
        "analysis_engine": analysis_engine,
    }


@lru_cache(maxsize=1)
def build_demo_text_templates() -> Dict[str, Any]:
    return {
        "English": {
            "battery_negative": "Battery drains too fast and the phone gets hot after ten minutes.",
            "battery_positive": "Battery life is solid and charging feels smooth.",
            "camera_negative": "Camera blur on zoom is frustrating.",
            "camera_positive": "Camera shots are sharp and stunning in low light.",
            "ui_negative": "The UI feels cluttered and laggy.",
            "ui_positive": "The UI is clean, fast, and easy to use.",
            "crash_negative": "The app crashes after login and freezes on launch.",
            "payment_negative": "Payment failed twice and checkout froze.",
            "support_negative": "Support replied late and the issue stayed unresolved.",
            "performance_negative": "Performance is slow and scrolling stutters.",
            "mixed": "Battery is terrible, but camera is stunning.",
        },
        "Hindi": {
            "battery_negative": "बैटरी बहुत जल्दी खत्म हो जाती है और फोन गर्म हो जाता है।",
            "battery_positive": "बैटरी लाइफ अच्छी है और चार्जिंग स्मूद लगती है।",
            "camera_negative": "ज़ूम पर कैमरा धुंधला आता है।",
            "camera_positive": "कैमरा बहुत अच्छा है और फोटो साफ़ आते हैं।",
            "ui_negative": "ऐप का इंटरफेस उलझा हुआ और लैगी है।",
            "ui_positive": "इंटरफेस साफ़, तेज़ और आसान है।",
            "crash_negative": "लॉगिन के बाद ऐप क्रैश करता है और लॉन्च पर फ्रीज हो जाता है।",
            "payment_negative": "भुगतान दो बार विफल हुआ और चेकआउट फ्रीज हो गया।",
            "support_negative": "सहायता ने देर से जवाब दिया और समस्या अनसुलझी रही।",
            "performance_negative": "परफॉर्मेंस धीमी है और स्क्रॉलिंग अटकती है।",
            "mixed": "बैटरी बहुत खराब है, लेकिन कैमरा शानदार है।",
        },
        "Tamil": {
            "battery_negative": "பேட்டரி மிக வேகமாக குறைந்து போன் சூடாகிறது.",
            "battery_positive": "பேட்டரி நல்லது மற்றும் சார்ஜிங் சீராக இருக்கிறது.",
            "camera_negative": "ஜூமில் கேமரா மங்கலாக உள்ளது.",
            "camera_positive": "கேமரா மிகவும் நல்லது, படங்கள் தெளிவாக உள்ளன.",
            "ui_negative": "UI குழப்பமான மற்றும் லேக்கி.",
            "ui_positive": "UI சுத்தமானது, வேகமானது, பயன்படுத்த எளிது.",
            "crash_negative": "லாகின் பிறகு ஆப் முடங்குகிறது மற்றும் தொடக்கத்தில் உறைகிறது.",
            "payment_negative": "பணம் இரண்டு முறை தோல்வியடைந்தது மற்றும் செக்அவுட் உறைந்தது.",
            "support_negative": "உதவி தாமதமாக பதிலளித்தது மற்றும் பிரச்சனை தீரவில்லை.",
            "performance_negative": "செயல்திறன் மெதுவாக உள்ளது மற்றும் ஸ்க்ரோலிங் தடைபடுகிறது.",
            "mixed": "பேட்டரி மோசம், ஆனால் கேமரா அருமை.",
        },
        "Telugu": {
            "battery_negative": "బ్యాటరీ చాలా త్వరగా అయిపోతుంది, ఫోన్ వేడెక్కుతోంది.",
            "battery_positive": "బ్యాటరీ మంచిది మరియు చార్జింగ్ సాఫీగా ఉంది.",
            "camera_negative": "జూమ్‌లో కెమెరా మసకగా ఉంది.",
            "camera_positive": "కెమెరా చాలా మంచి, ఫోటోలు స్పష్టంగా ఉన్నాయి.",
            "ui_negative": "ఇంటర్ఫేస్ గందరగోళంగా మరియు ల్యాగ్‌తో ఉంది.",
            "ui_positive": "ఇంటర్ఫేస్ శుభ్రంగా, వేగంగా, సులభంగా ఉంది.",
            "crash_negative": "లాగిన్ తర్వాత యాప్ క్రాష్ అవుతుంది మరియు ప్రారంభంలో ఫ్రీజ్ అవుతుంది.",
            "payment_negative": "చెల్లింపు రెండు సార్లు విఫలమైంది మరియు చెకౌట్ ఫ్రీజ్ అయింది.",
            "support_negative": "సహాయం ఆలస్యంగా స్పందించింది మరియు సమస్య పరిష్కరించబడలేదు.",
            "performance_negative": "పర్ఫార్మెన్స్ నెమ్మదిగా ఉంది మరియు స్క్రోలింగ్ ల్యాగ్ అవుతోంది.",
            "mixed": "బ్యాటరీ చాలా చెడు, కానీ కెమెరా అద్భుతం.",
        },
        "Kannada": {
            "battery_negative": "ಬ್ಯಾಟರಿ ತುಂಬಾ ಬೇಗ ಖಾಲಿ ಆಗುತ್ತದೆ ಮತ್ತು ಫೋನ್ ಬಿಸಿ ಆಗುತ್ತದೆ.",
            "battery_positive": "ಬ್ಯಾಟರಿ ಒಳ್ಳೆಯದು ಮತ್ತು ಚಾರ್ಜಿಂಗ್ ಮೃದುವಾಗಿದೆ.",
            "camera_negative": "ಜೂಮ್‌ನಲ್ಲಿ ಕ್ಯಾಮೆರಾ ಮಸುಕಾಗಿದೆ.",
            "camera_positive": "ಕ್ಯಾಮೆರಾ ತುಂಬಾ ಒಳ್ಳೆಯದು, ಫೋಟೋಗಳು ಸ್ಪಷ್ಟವಾಗಿವೆ.",
            "ui_negative": "ಇಂಟರ್ಫೇಸ್ ಗೊಂದಲ ಮತ್ತು ಲ್ಯಾಗ್‌ನೊಂದಿಗೆ ಇದೆ.",
            "ui_positive": "ಇಂಟರ್ಫೇಸ್ ಸ್ವಚ್ಛ, ವೇಗ ಮತ್ತು ಸುಲಭವಾಗಿದೆ.",
            "crash_negative": "ಲಾಗಿನ್ ನಂತರ ಆಪ್ ಕ್ರ್ಯಾಶ್ ಆಗುತ್ತದೆ ಮತ್ತು ಆರಂಭದಲ್ಲಿ ಫ್ರೀಜ್ ಆಗುತ್ತದೆ.",
            "payment_negative": "ಪಾವತಿ ಎರಡು ಬಾರಿ ವಿಫಲವಾಯಿತು ಮತ್ತು ಚೆಕ್ಔಟ್ ಫ್ರೀಜ್ ಆಯಿತು.",
            "support_negative": "ಸಹಾಯ ತಡವಾಗಿ ಪ್ರತಿಕ್ರಿಯಿಸಿತು ಮತ್ತು ಸಮಸ್ಯೆ ಪರಿಹಾರವಾಗಲಿಲ್ಲ.",
            "performance_negative": "ಕಾರ್ಯಕ್ಷಮತೆ ನಿಧಾನವಾಗಿದೆ ಮತ್ತು ಸ್ಕ್ರೋಲಿಂಗ್ ಲ್ಯಾಗ್ ಆಗುತ್ತಿದೆ.",
            "mixed": "ಬ್ಯಾಟರಿ ತುಂಬಾ ಕೆಟ್ಟದು, ಆದರೆ ಕ್ಯಾಮೆರಾ ಅದ್ಭುತವಾಗಿದೆ.",
        },
        "Bengali": {
            "battery_negative": "ব্যাটারি খুব দ্রুত শেষ হয়ে যায় এবং ফোন গরম হয়ে যায়।",
            "battery_positive": "ব্যাটারি ভালো এবং চার্জিং মসৃণ।",
            "camera_negative": "জুমে ক্যামেরা ঝাপসা।",
            "camera_positive": "ক্যামেরা খুব ভালো, ছবিগুলো পরিষ্কার।",
            "ui_negative": "ইন্টারফেস গোলমালপূর্ণ এবং ল্যাগি।",
            "ui_positive": "ইন্টারফেস পরিষ্কার, দ্রুত এবং সহজ।",
            "crash_negative": "লগইনের পরে অ্যাপ ক্র্যাশ করে এবং শুরুতে ফ্রিজ হয়ে যায়।",
            "payment_negative": "পেমেন্ট দুইবার ব্যর্থ হয়েছে এবং চেকআউট ফ্রিজ হয়েছে।",
            "support_negative": "সাহায্য দেরিতে উত্তর দিয়েছে এবং সমস্যা অমীমাংসিত রয়ে গেছে।",
            "performance_negative": "পারফরম্যান্স ধীর এবং স্ক্রোলিংয়ে ল্যাগ হচ্ছে।",
            "mixed": "ব্যাটারি খুব খারাপ, কিন্তু ক্যামেরা দারুণ।",
        },
    }


def build_review_sentence(language: str, feature: str, sentiment: str, rng: random.Random) -> str:
    templates = build_demo_text_templates().get(language, build_demo_text_templates()["English"])
    key = f"{feature}_{sentiment}"
    if feature == "mixed" and sentiment == "mixed":
        key = "mixed"
    if key in templates:
        return templates[key]

    fallback = build_demo_text_templates()["English"]
    if key in fallback:
        return fallback[key]
    return fallback["mixed"]


def build_demo_rows(product_name: str, competitor: bool = False) -> List[Dict[str, Any]]:
    rng = random.Random(17 if competitor else 42)
    today = start_of_week(datetime.utcnow()).date()
    week_starts = [today - timedelta(days=(7 * offset)) for offset in reversed(range(8))]
    version_schedule = ["v3.0", "v3.0", "v3.0", "v3.1", "v3.1", "v3.1", "v3.1.1", "v3.1.1"]
    release_dates = {"v3.0": week_starts[0].isoformat(), "v3.1": week_starts[3].isoformat(), "v3.1.1": week_starts[6].isoformat()}
    language_pool = [
        ("English", 0.43),
        ("Hindi", 0.17),
        ("Tamil", 0.1),
        ("Telugu", 0.1),
        ("Kannada", 0.1),
        ("Bengali", 0.1),
    ]
    platform_pool = ["Amazon", "Google Play", "Twitter/X", "Zendesk"]
    region_pool = [
        ("India", 0.62),
        ("United States", 0.18),
        ("United Kingdom", 0.08),
        ("Singapore", 0.06),
        ("Australia", 0.06),
    ]
    device_pool = [
        "Android 12",
        "Android 13",
        "iPhone 14",
        "Pixel 7",
        "Samsung A52",
        "OnePlus Nord",
    ]
    os_pool = [
        "Android 12",
        "Android 13",
        "iOS 17",
        "iOS 16",
    ]

    def pick_weighted(items):
        target = rng.random()
        cumulative = 0.0
        for item, weight in items:
            cumulative += weight
            if target <= cumulative:
                return item
        return items[-1][0]

    rows: List[Dict[str, Any]] = []
    review_index = 0

    for week_index, week_start in enumerate(week_starts):
        week_end = week_start + timedelta(days=7)
        base_volume = 28 + week_index * 4
        if competitor:
            base_volume = 24 + week_index * 3

        battery_negative = 4 + (week_index if week_index < 3 else 8 + week_index * 3)
        if competitor:
            battery_negative = 1 + max(0, week_index - 2)
        camera_positive = 5 + (1 if not competitor and week_index >= 4 else 0)
        camera_negative = 1 if week_index % 3 == 0 else 0
        ui_negative = 2 + (1 if week_index >= 4 and not competitor else 0)
        crash_negative = 1 + (1 if week_index >= 4 and not competitor else 0)
        payment_negative = 1 + (1 if week_index >= 5 else 0)
        support_negative = 2 + (1 if week_index >= 2 else 0)
        performance_negative = 1 + (1 if week_index >= 5 else 0)
        mixed_count = 2 if not competitor else 1
        search_positive = 1 if not competitor else 2
        notifications_negative = 1 if week_index >= 5 else 0
        storage_negative = 1 if not competitor and week_index >= 4 else 0

        weekly_specs: List[Tuple[str, str, int]] = [
            ("battery", "negative", battery_negative),
            ("battery", "positive", 1 if not competitor else 2),
            ("camera", "positive", camera_positive),
            ("camera", "negative", camera_negative),
            ("ui", "negative", ui_negative),
            ("ui", "positive", 1 if week_index % 2 == 0 else 0),
            ("crash", "negative", crash_negative),
            ("payment", "negative", payment_negative),
            ("support", "negative", support_negative),
            ("performance", "negative", performance_negative),
            ("search", "positive", search_positive),
            ("notifications", "negative", notifications_negative),
            ("storage", "negative", storage_negative),
            ("ads", "negative", 1 if competitor and week_index % 4 == 0 else 0),
            ("delivery", "negative", 1 if not competitor and week_index % 3 == 1 else 0),
        ]

        for feature, sentiment, count in weekly_specs:
            for _ in range(count):
                language = pick_weighted(language_pool)
                if feature == "battery" and sentiment == "negative" and week_index >= 3:
                    language = pick_weighted([("Hindi", 0.32), ("Tamil", 0.18), ("Telugu", 0.16), ("Kannada", 0.16), ("Bengali", 0.1), ("English", 0.08)])
                if feature == "support":
                    platform = "Zendesk"
                elif feature == "battery" and sentiment == "negative":
                    platform = pick_weighted([("Amazon", 0.35), ("Google Play", 0.3), ("Twitter/X", 0.2), ("Zendesk", 0.15)])
                else:
                    platform = pick_weighted([(item, 1 / len(platform_pool)) for item in platform_pool])
                region = pick_weighted(region_pool)
                device = rng.choice(device_pool)
                os_value = rng.choice(os_pool)
                version = version_schedule[min(week_index, len(version_schedule) - 1)]
                if feature == "battery" and sentiment == "negative" and week_index >= 3:
                    device = rng.choice(["Android 12", "Samsung A52", "Pixel 7"])
                    region = "India"
                    version = "v3.1"
                if competitor:
                    version = "v2.9" if week_index < 5 else "v3.0"
                review_date = (week_start + timedelta(days=rng.randint(0, 5))).isoformat()
                author = f"user-{review_index + 1}"
                review_index += 1
                text = build_review_sentence(language, feature, sentiment, rng)

                if feature == "battery" and sentiment == "negative" and not competitor and week_index >= 4 and rng.random() < 0.25:
                    text = build_review_sentence(language, "mixed", "mixed", rng)
                elif feature == "camera" and sentiment == "positive" and rng.random() < 0.2:
                    text = text + " " + build_review_sentence(language, "ui", "positive", rng)

                rating = {
                    "negative": rng.choice([1, 2]),
                    "positive": rng.choice([4, 5]),
                    "mixed": 3,
                }.get(sentiment, 3)
                if feature == "support" and sentiment == "negative":
                    rating = 1 if rng.random() < 0.65 else 2

                rows.append(
                    {
                        "id": f"{'comp' if competitor else 'rev'}-{review_index:04d}",
                        "product": product_name,
                        "text": text,
                        "rating": rating,
                        "date": review_date,
                        "platform": platform,
                        "language": language,
                        "region": region,
                        "device": device,
                        "os": os_value,
                        "version": version,
                        "release_date": release_dates.get(version, ""),
                        "author": author,
                    }
                )

        for _ in range(mixed_count):
            language = pick_weighted(language_pool)
            if not competitor and week_index >= 3:
                language = pick_weighted([("Hindi", 0.28), ("Tamil", 0.18), ("Telugu", 0.18), ("Kannada", 0.16), ("Bengali", 0.12), ("English", 0.08)])
            text = build_review_sentence(language, "mixed", "mixed", rng)
            review_date = (week_start + timedelta(days=rng.randint(0, 6))).isoformat()
            version = version_schedule[min(week_index, len(version_schedule) - 1)]
            if competitor:
                version = "v2.9"
            region = "India" if language != "English" and not competitor else pick_weighted(region_pool)
            device = rng.choice(device_pool)
            if language != "English" and not competitor:
                device = rng.choice(["Android 12", "Samsung A52", "Pixel 7"])
            rows.append(
                {
                    "id": f"{'comp' if competitor else 'rev'}-{review_index + 1:04d}",
                    "product": product_name,
                    "text": text,
                    "rating": 3,
                    "date": review_date,
                    "platform": "Google Play" if not competitor else "Amazon",
                    "language": language,
                    "region": region,
                    "device": device,
                    "os": rng.choice(os_pool),
                    "version": version,
                    "release_date": release_dates.get(version, ""),
                    "author": f"user-{review_index + 1}",
                }
            )
            review_index += 1

        def week_row_count() -> int:
            return sum(
                1
                for row in rows
                if week_start.isoformat() <= row["date"] < week_end.isoformat()
            )

        while week_row_count() < base_volume:
            feature = rng.choice(["camera", "ui", "performance", "search", "notifications"])
            sentiment = "positive" if feature in {"camera", "search"} else "negative"
            language = pick_weighted(language_pool)
            text = build_review_sentence(language, feature, sentiment, rng)
            review_date = (week_start + timedelta(days=rng.randint(0, 6))).isoformat()
            version = version_schedule[min(week_index, len(version_schedule) - 1)]
            rows.append(
                {
                    "id": f"{'comp' if competitor else 'rev'}-{review_index + 1:04d}",
                    "product": product_name,
                    "text": text,
                    "rating": rng.choice([4, 5]) if sentiment == "positive" else rng.choice([1, 2]),
                    "date": review_date,
                    "platform": rng.choice(platform_pool),
                    "language": language,
                    "region": pick_weighted(region_pool),
                    "device": rng.choice(device_pool),
                    "os": rng.choice(os_pool),
                    "version": version,
                    "release_date": release_dates.get(version, ""),
                    "author": f"user-{review_index + 1}",
                }
            )
            review_index += 1
            if len(rows) > 500:
                break

    return dedupe_rows(rows)


def rows_to_csv(rows: List[Dict[str, Any]]) -> str:
    if not rows:
        return ""
    fieldnames = [
        "id",
        "product",
        "text",
        "rating",
        "date",
        "platform",
        "language",
        "region",
        "device",
        "os",
        "version",
        "release_date",
        "author",
    ]
    buffer = io.StringIO()
    writer = csv.DictWriter(buffer, fieldnames=fieldnames)
    writer.writeheader()
    for row in rows:
        writer.writerow({key: row.get(key, "") for key in fieldnames})
    return buffer.getvalue()


def build_demo_payload() -> Dict[str, Any]:
    product_name = "Aurora X1"
    primary_rows = build_demo_rows(product_name, competitor=False)
    competitor_rows = build_demo_rows(f"{product_name} Pro", competitor=True)
    return {
        "product_name": product_name,
        "primary_csv": rows_to_csv(primary_rows),
        "competitor_csv": rows_to_csv(competitor_rows),
        "primary_row_count": len(primary_rows),
        "competitor_row_count": len(competitor_rows),
    }


@app.route("/api/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "service": "reviewiq-api",
            "database": "none",
            "analysis_engine": {
                "provider": "openrouter" if openrouter_is_enabled() else "heuristic",
                "requested_model": OPENROUTER_MODEL if openrouter_is_enabled() else None,
                "enabled": openrouter_is_enabled(),
            },
        }
    )


@app.route("/api/demo-data", methods=["GET"])
def demo_data():
    return jsonify(build_demo_payload())


@app.route("/api/analyze", methods=["POST", "OPTIONS"])
def analyze():
    if request.method == "OPTIONS":
        return ("", 204)

    payload = request.get_json(force=True, silent=True) or {}
    product_name = str(payload.get("product_name") or "ReviewIQ Product")
    settings = payload.get("settings") or {}

    primary_rows = payload.get("rows")
    competitor_rows = payload.get("competitor_rows")

    if primary_rows is None:
        primary_rows = parse_csv_text(str(payload.get("csv_text") or ""))
    if competitor_rows is None:
        competitor_csv = str(payload.get("competitor_csv_text") or "")
        competitor_rows = parse_csv_text(competitor_csv) if competitor_csv.strip() else []

    result = analyze_reviews(primary_rows, competitor_rows, product_name=product_name, settings=settings)
    result["source"] = {
        "product_name": product_name,
        "primary_row_count": len(primary_rows),
        "competitor_row_count": len(competitor_rows or []),
        "mode": "csv_text" if payload.get("csv_text") else "rows",
    }
    return jsonify(result)


@app.route("/api/search-apps", methods=["GET"])
def search_apps():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    try:
        raw_results = search(query, lang="en", country="us", n_hits=10)
        normalized: List[Dict[str, Any]] = []
        for item in (raw_results or []):
            if not isinstance(item, dict):
                continue
            app_id = (
                item.get("appId")
                or item.get("appID")
                or item.get("id")
                or item.get("app_id")
                or item.get("packageName")
                or item.get("package")
                or None
            )
            title = item.get("title") or item.get("name") or item.get("app") or ""
            developer = item.get("developer") or item.get("developerId") or item.get("developerName") or ""
            icon = item.get("icon") or item.get("iconUrl") or item.get("image") or None
            score = item.get("score") or item.get("scoreText") or None
            normalized.append({
                "appId": app_id,
                "title": title,
                "developer": developer,
                "icon": icon,
                "score": score,
                # include original for debugging/compat
                "_raw": item,
            })
        return jsonify(normalized)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@app.route("/api/analyze-play-store", methods=["POST", "OPTIONS"])
def analyze_play_store():
    if request.method == "OPTIONS":
        return ("", 204)
    
    payload = request.get_json(force=True, silent=True) or {}
    app_id = str(payload.get("app_id") or "").strip()
    settings = payload.get("settings") or {}
    
    if not app_id:
        return jsonify({"error": "No App ID provided."}), 400
        
    try:
        # If we have a recent cached analysis, return it immediately
        cached = PLAY_STORE_CACHE.get(app_id)
        if cached and (time.time() - cached[0]) < PLAY_CACHE_TTL:
            return jsonify(cached[1])

        # Fetch app details for the product name
        details = app_details(app_id, lang="en", country="us")
        product_name = details.get("title", "Play Store App")

        # Fetch top reviews (up to 200) with simple retry/backoff
        play_reviews = []
        play_ex = None
        attempts = 0
        max_attempts = 3
        while attempts < max_attempts:
            attempts += 1
            try:
                play_reviews, _ = reviews(
                    app_id,
                    lang="en",
                    country="us",
                    sort=Sort.NEWEST,
                    count=200,
                )
                play_ex = None
                break
            except Exception as err:
                play_ex = err
                # exponential-ish backoff
                time.sleep(0.5 * attempts)
        if play_ex:
            raise play_ex
        
        if not play_reviews:
            return jsonify({"error": "No reviews found for this app on Google Play."}), 404
            
        # Map to canonical row format
        rows = []
        for idx, r in enumerate(play_reviews):
            rows.append({
                "id": f"play-{idx:04d}",
                "product": product_name,
                "text": r.get("content", ""),
                "rating": r.get("score"),
                "date": r.get("at").isoformat() if hasattr(r.get("at"), "isoformat") else str(r.get("at")),
                "platform": "Google Play",
                "author": r.get("userName", "Anonymous"),
                "version": r.get("reviewCreatedVersion", "Unknown"),
            })
            
        result = analyze_reviews(rows, product_name=product_name, settings=settings)
        # Cache successful analysis for short TTL to reduce repeat scraping
        try:
            PLAY_STORE_CACHE[app_id] = (time.time(), result)
        except Exception:
            pass
        result["source"] = {
            "mode": "play_store",
            "app_id": app_id,
            "product_name": product_name,
            "app_icon": details.get("icon"),
            "row_count": len(rows),
        }
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": f"Play Store analysis failed: {exc}"}), 500


@app.route("/api/analyze-url", methods=["POST", "OPTIONS"])
def analyze_url():
    if request.method == "OPTIONS":
        return ("", 204)

    payload = request.get_json(force=True, silent=True) or {}
    product_name = str(payload.get("product_name") or "ReviewIQ Product")
    settings = payload.get("settings") or {}
    review_url = str(payload.get("review_url") or "").strip()
    manual_text = str(payload.get("manual_text") or "").strip()

    if manual_text:
        source_text = manual_text
    else:
        if not review_url:
            return jsonify({"error": "Provide a review URL or paste review text."}), 400
        try:
            source_text = fetch_review_page_text(review_url)
        except Exception as exc:
            return jsonify({"error": f"Unable to fetch the URL: {exc}"}), 400

    rows = parse_csv_text(source_text)
    if not rows:
        return jsonify({"error": "We could not find any review text to analyze."}), 400

    result = analyze_reviews(rows, product_name=product_name, settings=settings)
    result["source"] = {
        "product_name": product_name,
        "review_url": review_url,
        "mode": "manual_text" if manual_text else "url",
        "row_count": len(rows),
        "source_text_length": len(source_text),
        "favicon": f"https://www.google.com/s2/favicons?domain={urlparse(review_url).netloc}&sz=64" if review_url else None,
    }
    return jsonify(result)


@app.route("/", defaults={"path": ""})
@app.route("/<path:path>")
def spa_fallback(path: str):
    if path.startswith("api/"):
        abort(404)

    if DIST_DIR.exists():
        requested = DIST_DIR / path
        if path and requested.exists():
            return send_from_directory(DIST_DIR, path)
        return send_from_directory(DIST_DIR, "index.html")

    return jsonify(
        {
            "ok": True,
            "service": "reviewiq-api",
            "message": "Flask backend is running. Start the Vite frontend separately or build the app into dist/.",
            "endpoints": ["/api/health", "/api/demo-data", "/api/analyze"],
        }
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
