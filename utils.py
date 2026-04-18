from __future__ import annotations

import csv
import io
import html as html_lib
import json
import math
import re
import unicodedata
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import config


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
    # other language entries omitted for brevity in utils; kept in analysis if needed
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
    # Lightweight LLM mention lookup omitted here; analysis may supply it.
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
    from urllib.parse import urlparse
    import requests

    parsed = urlparse(review_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Please enter a valid http or https URL.")

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
        timeout=config.OPENROUTER_TIMEOUT_SECONDS,
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
if not config.env_bool("REVIEWIQ_LLM_ONLY", False):
    for feature_key, info in ASPECTS.items():
        FEATURE_KEY_LOOKUP[normalize_text(feature_key)] = feature_key
        FEATURE_KEY_LOOKUP[normalize_text(info["label"])] = feature_key
        for keyword in info["keywords"]:
            FEATURE_KEY_LOOKUP.setdefault(normalize_text(keyword), feature_key)
from __future__ import annotations

import csv
import io
import html as html_lib
import json
import math
import re
import unicodedata
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Optional, Tuple

from . import config


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
    # other language entries omitted for brevity in utils; kept in analysis if needed
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
    # Lightweight LLM mention lookup omitted here; analysis may supply it.
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
    from urllib.parse import urlparse
    import requests

    parsed = urlparse(review_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ValueError("Please enter a valid http or https URL.")

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
        timeout=config.OPENROUTER_TIMEOUT_SECONDS,
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
if not config.env_bool("REVIEWIQ_LLM_ONLY", False):
    for feature_key, info in ASPECTS.items():
        FEATURE_KEY_LOOKUP[normalize_text(feature_key)] = feature_key
        FEATURE_KEY_LOOKUP[normalize_text(info["label"])] = feature_key
        for keyword in info["keywords"]:
            FEATURE_KEY_LOOKUP.setdefault(normalize_text(keyword), feature_key)
