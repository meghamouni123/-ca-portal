"""
FastAPI Backend Server
Serves CA articles with date/category filtering to the frontend.
"""

import os
import sys
import logging
from datetime import date, datetime, timedelta
from typing import Optional, List
import json

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Try FastAPI, fallback to built-in http.server
try:
    from fastapi import FastAPI, Query, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.staticfiles import StaticFiles
    from fastapi.responses import FileResponse, JSONResponse
    import uvicorn
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False

from database import (
    init_db, get_articles, get_article_count, get_categories, get_dates_with_articles,
    get_stats, insert_article
)

logger = logging.getLogger(__name__)

STATIC_DIR = os.path.join(os.path.dirname(__file__), '..', 'frontend')

if HAS_FASTAPI:
    app = FastAPI(
        title="Current Affairs API",
        description="Exam-focused Current Affairs for SSC/RRB aspirants",
        version="1.0.0"
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
        init_db()
        # Seed demo data if DB is empty
        from pipeline import seed_demo_data
        stats = get_stats()
        if stats['total'] == 0:
            logger.info("DB empty - seeding demo data...")
            seed_demo_data()
        logger.info("Server started. DB stats: " + str(get_stats()))

    @app.get("/api/news")
    async def get_news(
        date: Optional[str] = Query(None, description="Date filter YYYY-MM-DD"),
        category: Optional[str] = Query(None, description="Category filter"),
        min_confidence: float = Query(0.80, description="Minimum confidence score"),
        limit: int = Query(15, le=200),
        page: int = Query(1, ge=1),
        search: Optional[str] = Query(None, description="Search in headline/summary"),
        date_from: Optional[str] = Query(None, description="Date range start YYYY-MM-DD")
    ):
        import math
        offset = (page - 1) * limit
        articles = get_articles(
            date_filter=date,
            category_filter=category,
            min_confidence=min_confidence,
            limit=limit,
            offset=offset,
            date_from=date_from,
            search=search
        )
        total = get_article_count(
            date_filter=date,
            category_filter=category,
            min_confidence=min_confidence,
            date_from=date_from,
            search=search
        )
        pages = math.ceil(total / limit) if total > 0 else 1
        return {
            "articles": articles,
            "count": len(articles),
            "total": total,
            "page": page,
            "pages": pages,
            "filters": {"date": date, "category": category, "min_confidence": min_confidence}
        }

    @app.get("/api/categories")
    async def list_categories():
        cats = get_categories()
        return {"categories": cats}

    @app.get("/api/dates")
    async def list_dates():
        dates = get_dates_with_articles(days=30)
        return {"dates": dates}

    @app.get("/api/stats")
    async def db_stats():
        return get_stats()

    @app.get("/api/today")
    async def today_news(
        category: Optional[str] = Query(None),
        min_confidence: float = Query(0.80)
    ):
        today = str(date.today())
        articles = get_articles(
            date_filter=today,
            category_filter=category,
            min_confidence=min_confidence,
            limit=100
        )
        return {"date": today, "articles": articles, "count": len(articles)}

    @app.post("/api/fetch")
    async def trigger_fetch(max_feeds: int = 5):
        """Manually trigger a pipeline run."""
        try:
            from pipeline import run_pipeline_once
            result = run_pipeline_once(use_rss=True, use_api=False, max_feeds=max_feeds)
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))

    # Serve frontend
    if os.path.exists(STATIC_DIR):
        app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")

        @app.get("/")
        async def serve_frontend():
            return FileResponse(os.path.join(STATIC_DIR, 'index.html'))

    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        uvicorn.run(app, host="0.0.0.0", port=8000, reload=True)

else:
    # Fallback: simple HTTP server using Python built-ins
    import http.server
    import urllib.parse
    import socketserver

    class CARequestHandler(http.server.SimpleHTTPRequestHandler):
        def do_GET(self):
            parsed = urllib.parse.urlparse(self.path)
            path = parsed.path
            query = urllib.parse.parse_qs(parsed.query)

            if path == '/api/news':
                self._serve_json(self._get_news(query))
            elif path == '/api/categories':
                self._serve_json({"categories": get_categories()})
            elif path == '/api/dates':
                self._serve_json({"dates": get_dates_with_articles(30)})
            elif path == '/api/stats':
                self._serve_json(get_stats())
            elif path == '/' or path == '/index.html':
                self._serve_file(os.path.join(STATIC_DIR, 'index.html'), 'text/html')
            else:
                # Try static files
                file_path = os.path.join(STATIC_DIR, path.lstrip('/'))
                if os.path.exists(file_path):
                    ext = os.path.splitext(file_path)[1]
                    ctype = {'css': 'text/css', 'js': 'text/javascript'}.get(ext.lstrip('.'), 'text/plain')
                    self._serve_file(file_path, ctype)
                else:
                    self.send_response(404)
                    self.end_headers()

        def do_POST(self):
            parsed = urllib.parse.urlparse(self.path)
            if parsed.path == '/api/fetch':
                try:
                    from pipeline import run_pipeline_once
                    result = run_pipeline_once(use_rss=True, max_feeds=5)
                    self._serve_json(result)
                except Exception as e:
                    self._serve_json({"error": str(e)}, 500)

        def _get_news(self, query):
            import math
            date_filter = query.get('date', [None])[0]
            date_from   = query.get('date_from', [None])[0]
            category    = query.get('category', [None])[0]
            search      = query.get('search', [None])[0]
            min_conf    = float(query.get('min_confidence', ['0.80'])[0])
            limit       = int(query.get('limit', ['15'])[0])
            page        = int(query.get('page', ['1'])[0])
            offset      = (page - 1) * limit
            articles    = get_articles(date_filter, category, min_conf, limit, offset, date_from, search)
            total       = get_article_count(date_filter, category, min_conf, date_from, search)
            pages       = math.ceil(total / limit) if total > 0 else 1
            return {
                "articles": articles,
                "count": len(articles),
                "total": total,
                "page": page,
                "pages": pages
            }

        def _serve_json(self, data, status=200):
            body = json.dumps(data, default=str).encode('utf-8')
            self.send_response(status)
            self.send_header('Content-Type', 'application/json')
            self.send_header('Content-Length', len(body))
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(body)

        def _serve_file(self, path, ctype):
            try:
                with open(path, 'rb') as f:
                    body = f.read()
                self.send_response(200)
                self.send_header('Content-Type', ctype)
                self.send_header('Content-Length', len(body))
                self.end_headers()
                self.wfile.write(body)
            except FileNotFoundError:
                self.send_response(404)
                self.end_headers()

        def log_message(self, format, *args):
            logger.info(f"{self.address_string()} - {format % args}")

    def run_server(port=8000):
        init_db()
        from pipeline import seed_demo_data
        stats = get_stats()
        if stats['total'] == 0:
            seed_demo_data()

        with socketserver.TCPServer(("", port), CARequestHandler) as httpd:
            logger.info(f"Serving on http://localhost:{port}")
            httpd.serve_forever()

    if __name__ == "__main__":
        logging.basicConfig(level=logging.INFO)
        run_server(8000)