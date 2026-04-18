from __future__ import annotations

import json
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import lru_cache
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple, Counter

from . import config, utils
from .utils import (
    canonical_review_row,
    chunked,
)

import requests

try:
    from google_play_scraper import Sort, reviews, search, app as app_details
except Exception:
    Sort = None
    reviews = None
    search = None
    app_details = None


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


def openrouter_is_enabled() -> bool:
    return bool(config.OPENROUTER_API_KEY)


def openrouter_headers() -> Dict[str, str]:
    headers = {
        "Authorization": f"Bearer {config.OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
    }
    if config.OPENROUTER_HTTP_REFERER:
        headers["HTTP-Referer"] = config.OPENROUTER_HTTP_REFERER
    if config.OPENROUTER_TITLE:
        headers["X-OpenRouter-Title"] = config.OPENROUTER_TITLE
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
        "model": model or config.OPENROUTER_MODEL,
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
        f"{config.OPENROUTER_BASE_URL}/chat/completions",
        headers=openrouter_headers(),
        json=payload,
        timeout=config.OPENROUTER_TIMEOUT_SECONDS,
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
    parsed["_openrouter_model"] = data.get("model", model or config.OPENROUTER_MODEL)
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


def analyze_openrouter_batch(batch_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    messages = build_review_batch_messages(batch_rows)
    payload = openrouter_chat_completion(messages, LLM_REVIEW_ANALYSIS_SCHEMA, temperature=0.1, max_tokens=6144)
    reviews_payload = payload.get("reviews") or []
    mapping: Dict[str, Dict[str, Any]] = {}
    for item in reviews_payload:
        if not isinstance(item, dict):
            continue
        review_id = str(item.get("id") or "").strip()
        if not review_id:
            continue
        mapping[review_id] = {**item, "_openrouter_model": payload.get("_openrouter_model")}
    return mapping


def compact_review_record(row: Dict[str, Any]) -> Dict[str, Any]:
    text = utils.normalize_whitespace(row.get("text", ""))
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


def build_feature_catalog() -> List[Dict[str, Any]]:
    if config.env_bool("REVIEWIQ_LLM_ONLY", False):
        return []
    return [
        {
            "feature_key": feature_key,
            "feature_label": info["label"],
            "keywords": info["keywords"][:6],
            "importance": round(float(info.get("importance", 1.0)), 2),
        }
        for feature_key, info in utils.ASPECTS.items()
    ]


def enrich_rows_with_openrouter(rows: List[Dict[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not rows or not openrouter_is_enabled():
        return rows, {
            "provider": "heuristic",
            "requested_model": None,
            "model_used": None,
            "enabled": False,
        }

    batch_size = max(4, min(24, config.REVIEWIQ_BATCH_SIZE))
    batches = chunked(rows, batch_size)
    resolved_rows = [dict(row) for row in rows]
    analysis_by_id: Dict[str, Dict[str, Any]] = {}
    used_local_fallback = False

    def build_local_analysis(row: Dict[str, Any]) -> Dict[str, Any]:
        review_id = row.get("id") or ""
        heuristic_mentions = utils.extract_mentions({k: v for k, v in row.items() if k != "analysis"}, review_id)
        specific_mentions = [mention for mention in heuristic_mentions if mention.get("feature_key") != "overall"]
        selected_mentions = specific_mentions or heuristic_mentions
        sentiment, confidence, label = utils.sentiment_from_text(row.get("text", ""), row.get("rating"))
        aspects = [
            {
                "feature_key": mention["feature_key"],
                "feature_label": mention["feature_label"],
                "sentiment": mention["sentiment"],
                "confidence": mention["confidence"],
                "severity": mention.get("severity") or ("critical" if mention["sentiment"] <= -0.75 else "high" if mention["sentiment"] <= -0.45 else "medium" if mention["sentiment"] <= -0.2 else "low"),
                "evidence": mention.get("evidence") or row.get("text", ""),
                "translated_evidence": mention.get("translated_evidence") or utils.translate_to_english(row.get("text", ""), row.get("language") or "English"),
            }
            for mention in selected_mentions
        ]
        return {
            "id": review_id,
            "language": row.get("language") or utils.detect_language(row.get("text", ""), row.get("language")),
            "translated_text": utils.translate_to_english(row.get("text", ""), row.get("language") or "English"),
            "overall_sentiment": sentiment,
            "sentiment_label": label,
            "confidence": confidence,
            "aspects": aspects,
        }

    max_workers = max(1, min(config.REVIEWIQ_MAX_WORKERS, len(batches)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(analyze_openrouter_batch, batch): batch for batch in batches}
        for future in as_completed(future_map):
            batch = future_map[future]
            try:
                batch_mapping = future.result()
                analysis_by_id.update(batch_mapping)
            except Exception:
                if config.REVIEWIQ_ALLOW_LOCAL_FALLBACK:
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
            row["translated_text"] = utils.normalize_whitespace(str(analysis.get("translated_text") or utils.translate_to_english(row.get("text", ""), row.get("language") or "English")))
        else:
            row["analysis"] = build_local_analysis(row)
            row["translated_text"] = row["analysis"]["translated_text"]

    engine_info = {
        "provider": "openrouter",
        "requested_model": config.OPENROUTER_MODEL,
        "model_used": None,
        "enabled": True,
        "batch_size": batch_size,
        "fallback_used": used_local_fallback,
    }
    if analysis_by_id:
        engine_info["model_used"] = next(
            (item.get("_openrouter_model") for item in analysis_by_id.values() if item.get("_openrouter_model")),
            config.OPENROUTER_MODEL,
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
        if not config.REVIEWIQ_ALLOW_LOCAL_FALLBACK:
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
        "requested_model": config.OPENROUTER_MODEL,
        "model_used": payload.get("_openrouter_model") or config.OPENROUTER_MODEL,
        "enabled": True,
        "fallback_used": False,
        "review_sample_size": len(evidence_rows),
    }
    return issues, analysis_summary
