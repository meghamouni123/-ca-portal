"""
FastAPI Backend Server — PostgreSQL only.
Run with: uvicorn backend.server:app --reload --port 8000
Or use run.py for the simple built-in HTTP server.

Requires DATABASE_URL in .env:
    DATABASE_URL=postgresql://user:password@localhost:5432/ca_portal
"""

import os
import sys
import logging
import math
from datetime import date, datetime
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import uvicorn

from database import (
    init_db, get_articles, get_article_count,
    get_categories, get_dates_with_articles, get_stats,
    get_category_counts,
)

logger = logging.getLogger(__name__)

STATIC_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')

app = FastAPI(
    title="Current Affairs API",
    description="Exam-focused Current Affairs (MPNet + BART) — PostgreSQL backend",
    version="2.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup():
    """Init PostgreSQL schema and seed demo data if DB is empty."""
    init_db()
    stats = get_stats()
    if stats['total'] == 0:
        logger.info("Empty DB — seeding demo articles...")
        from pipeline import seed_demo_data
        seed_demo_data()
    logger.info("Server ready. DB stats: " + str(get_stats()))


# ─── Endpoints (section 2.6 of technical script) ─────────────────────────────

@app.get("/api/news")
async def api_news(
    date:           Optional[str]   = Query(None,  description="Date YYYY-MM-DD"),
    category:       Optional[str]   = Query(None,  description="Category name"),
    min_confidence: float           = Query(0.80,  description="Minimum confidence"),
    limit:          int             = Query(15,    le=200),
    page:           int             = Query(1,     ge=1),
    search:         Optional[str]   = Query(None,  description="Search headline/summary"),
    date_from:      Optional[str]   = Query(None,  description="Date range start YYYY-MM-DD"),
):
    offset   = (page - 1) * limit
    articles = get_articles(date, category, min_confidence, limit, offset, date_from, search)
    total    = get_article_count(date, category, min_confidence, date_from, search)
    pages    = math.ceil(total / limit) if total > 0 else 1
    return {
        "articles": articles,
        "count":    len(articles),
        "total":    total,
        "page":     page,
        "pages":    pages,
        "filters":  {"date": date, "category": category, "min_confidence": min_confidence},
    }


@app.get("/api/categories")
async def api_categories():
    return {"categories": get_categories()}


@app.get("/api/dates")
async def api_dates():
    return {"dates": get_dates_with_articles(30)}


@app.get("/api/stats")
async def api_stats():
    return get_stats()


@app.get("/api/category-counts")
async def api_category_counts(
    date:           Optional[str] = Query(None, description="Date YYYY-MM-DD"),
    date_from:      Optional[str] = Query(None, description="Date range start YYYY-MM-DD"),
    min_confidence: float         = Query(0.80),
):
    return get_category_counts(date, date_from, min_confidence)


@app.get("/api/today")
async def api_today(
    category:       Optional[str] = Query(None),
    min_confidence: float         = Query(0.80),
):
    today    = str(date.today())
    articles = get_articles(today, category, min_confidence, 100)
    return {"date": today, "articles": articles, "count": len(articles)}


@app.post("/api/fetch")
async def api_fetch(max_feeds: int = 10):
    """Manually trigger a pipeline run (fetches live RSS feeds)."""
    try:
        from pipeline import run_pipeline_once
        result = run_pipeline_once(use_rss=True, use_api=False, max_feeds=max_feeds)
        return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── Serve frontend ───────────────────────────────────────────────────────────

if os.path.exists(STATIC_DIR):
    app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

    @app.get("/")
    async def serve_index():
        return FileResponse(os.path.join(STATIC_DIR, 'index.html'))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)