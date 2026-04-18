from __future__ import annotations

import time
from flask import Blueprint, current_app, jsonify, request, send_from_directory, abort

from . import config
from . import analysis
from . import utils
from . import app as legacy_app

bp = Blueprint("api", __name__)


@bp.after_app_request
def add_cors_headers(response):
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    return response


@bp.route("/api/health", methods=["GET"])
def health():
    return jsonify(
        {
            "ok": True,
            "service": "reviewiq-api",
            "database": "none",
            "analysis_engine": {
                "provider": "openrouter" if analysis.openrouter_is_enabled() else "heuristic",
                "requested_model": config.OPENROUTER_MODEL if analysis.openrouter_is_enabled() else None,
                "enabled": analysis.openrouter_is_enabled(),
            },
        }
    )


@bp.route("/api/demo-data", methods=["GET"])
def demo_data():
    # Build lightweight demo payload using analysis helpers
    try:
        payload = build_demo_payload()
    except Exception:
        payload = {"product_name": "Demo", "primary_csv": "", "competitor_csv": "", "primary_row_count": 0}
    return jsonify(payload)


def build_demo_payload():
    product_name = "Aurora X1"
    primary_rows = legacy_app.build_demo_rows(product_name, competitor=False)
    competitor_rows = legacy_app.build_demo_rows(f"{product_name} Pro", competitor=True)
    return {
        "product_name": product_name,
        "primary_csv": legacy_app.rows_to_csv(primary_rows),
        "competitor_csv": legacy_app.rows_to_csv(competitor_rows),
        "primary_row_count": len(primary_rows),
        "competitor_row_count": len(competitor_rows),
    }


@bp.route("/api/analyze", methods=["POST", "OPTIONS"])
def analyze():
    if request.method == "OPTIONS":
        return ("", 204)

    payload = request.get_json(force=True, silent=True) or {}
    product_name = str(payload.get("product_name") or "ReviewIQ Product")
    settings = payload.get("settings") or {}

    primary_rows = payload.get("rows")
    competitor_rows = payload.get("competitor_rows")

    if primary_rows is None:
        primary_rows = utils.parse_csv_text(str(payload.get("csv_text") or ""))
    if competitor_rows is None:
        competitor_csv = str(payload.get("competitor_csv_text") or "")
        competitor_rows = utils.parse_csv_text(competitor_csv) if competitor_csv.strip() else []

    result = analysis.analyze_reviews(primary_rows, competitor_rows, product_name=product_name, settings=settings)
    result["source"] = {
        "product_name": product_name,
        "primary_row_count": len(primary_rows),
        "competitor_row_count": len(competitor_rows or []),
        "mode": "csv_text" if payload.get("csv_text") else "rows",
    }
    return jsonify(result)


@bp.route("/api/search-apps", methods=["GET"])
def search_apps():
    query = request.args.get("q", "").strip()
    if not query:
        return jsonify([])
    try:
        raw_results = analysis.search(query, lang="en", country="us", n_hits=10) if analysis.search else []
        normalized = []
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
                "_raw": item,
            })
        return jsonify(normalized)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@bp.route("/api/analyze-play-store", methods=["POST", "OPTIONS"])
def analyze_play_store():
    if request.method == "OPTIONS":
        return ("", 204)

    payload = request.get_json(force=True, silent=True) or {}
    app_id = str(payload.get("app_id") or "").strip()
    settings = payload.get("settings") or {}

    if not app_id:
        return jsonify({"error": "No App ID provided."}), 400

    try:
        cached = config.PLAY_STORE_CACHE.get(app_id)
        if cached and (time.time() - cached[0]) < config.PLAY_CACHE_TTL:
            return jsonify(cached[1])

        details = analysis.app_details(app_id, lang="en", country="us") if analysis.app_details else {}
        product_name = details.get("title", "Play Store App")

        play_reviews = []
        play_ex = None
        attempts = 0
        max_attempts = 3
        while attempts < max_attempts:
            attempts += 1
            try:
                play_reviews, _ = analysis.reviews(
                    app_id,
                    lang="en",
                    country="us",
                    sort=analysis.Sort.NEWEST if analysis.Sort else None,
                    count=200,
                ) if analysis.reviews else ([], None)
                play_ex = None
                break
            except Exception as err:
                play_ex = err
                time.sleep(0.5 * attempts)
        if play_ex:
            raise play_ex

        if not play_reviews:
            return jsonify({"error": "No reviews found for this app on Google Play."}), 404

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

        result = analysis.analyze_reviews(rows, product_name=product_name, settings=settings)
        try:
            config.PLAY_STORE_CACHE[app_id] = (time.time(), result)
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


@bp.route("/api/analyze-url", methods=["POST", "OPTIONS"])
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
            source_text = utils.fetch_review_page_text(review_url)
        except Exception as exc:
            return jsonify({"error": f"Unable to fetch the URL: {exc}"}), 400

    rows = utils.parse_csv_text(source_text)
    if not rows:
        return jsonify({"error": "We could not find any review text to analyze."}), 400

    result = analysis.analyze_reviews(rows, product_name=product_name, settings=settings)
    result["source"] = {
        "product_name": product_name,
        "review_url": review_url,
        "mode": "manual_text" if manual_text else "url",
        "row_count": len(rows),
        "source_text_length": len(source_text),
        "favicon": f"https://www.google.com/s2/favicons?domain={review_url.split('/')[2]}&sz=64" if review_url else None,
    }
    return jsonify(result)


@bp.route("/", defaults={"path": ""})
@bp.route("/<path:path>")
def spa_fallback(path: str):
    if path.startswith("api/"):
        abort(404)

    if config.DIST_DIR.exists():
        requested = config.DIST_DIR / path
        if path and requested.exists():
            return send_from_directory(config.DIST_DIR, path)
        return send_from_directory(config.DIST_DIR, "index.html")

    return jsonify(
        {
            "ok": True,
            "service": "reviewiq-api",
            "message": "Flask backend is running. Start the Vite frontend separately or build the app into dist/.",
            "endpoints": ["/api/health", "/api/demo-data", "/api/analyze"],
        }
    )
