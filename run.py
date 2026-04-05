#!/usr/bin/env python3
"""
CA Portal - Main Entry Point
Run with: python3 run.py
Opens: http://localhost:8000

Starts HTTP server serving:
  - /           → frontend/index.html
  - /api/news   → filtered articles (date, category, confidence)
  - /api/stats  → db statistics
  - /api/fetch  → trigger live RSS fetch
"""

import os
import sys
import json
import logging
import urllib.parse
import http.server
import socketserver
import threading
from datetime import date, datetime

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env'))
except ImportError:
    pass

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, 'backend'))
FRONTEND_DIR = os.path.join(BASE_DIR, 'frontend')
DB_PATH = os.path.join(BASE_DIR, 'ca_articles.db')
os.environ.setdefault('DB_PATH', DB_PATH)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Request Handler
# ─────────────────────────────────────────
class CAHandler(http.server.BaseHTTPRequestHandler):

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path.rstrip('/')
        query  = urllib.parse.parse_qs(parsed.query)

        if path == '' or path == '/index.html':
            self._serve_file(os.path.join(FRONTEND_DIR, 'index.html'), 'text/html; charset=utf-8')

        elif path == '/api/news':
            from database import get_articles
            date_f   = query.get('date',           [None])[0]
            date_from= query.get('date_from',      [None])[0]
            cat_f    = query.get('category',        [None])[0]
            min_c    = float(query.get('min_confidence', ['0.80'])[0])
            limit    = int(query.get('limit',   ['100'])[0])
            offset   = int(query.get('offset',  ['0'])[0])
            articles = get_articles(date_f, cat_f, min_c, limit, offset, date_from)
            self._json({'articles': articles, 'count': len(articles)})

        elif path == '/api/categories':
            from database import get_categories
            self._json({'categories': get_categories()})

        elif path == '/api/dates':
            from database import get_dates_with_articles
            self._json({'dates': get_dates_with_articles(30)})

        elif path == '/api/stats':
            from database import get_stats
            self._json(get_stats())

        elif path == '/api/today':
            from database import get_articles
            cat_f = query.get('category', [None])[0]
            min_c = float(query.get('min_confidence', ['0.80'])[0])
            arts  = get_articles(str(date.today()), cat_f, min_c, 100, 0)
            self._json({'date': str(date.today()), 'articles': arts, 'count': len(arts)})

        else:
            self._not_found()

    def do_POST(self):
        parsed = urllib.parse.urlparse(self.path)
        path   = parsed.path

        if path == '/api/fetch':
            def _do_fetch():
                try:
                    from pipeline import run_pipeline_once
                    result = run_pipeline_once(use_rss=True, max_feeds=None)
                    return result
                except Exception as e:
                    return {'error': str(e)}
            # Run fetch in background thread, respond immediately
            result = {'status': 'started', 'message': 'Fetching news in background...', 'timestamp': datetime.now().isoformat()}
            self._json(result)
            t = threading.Thread(target=_do_fetch, daemon=True)
            t.start()
        else:
            self._json({'error': 'Not found'}, 404)

    def do_OPTIONS(self):
        self.send_response(200)
        self._cors_headers()
        self.end_headers()

    # ── Helpers ────────────────────────────────────────────────

    def _json(self, data, status=200):
        body = json.dumps(data, default=str, ensure_ascii=False).encode('utf-8')
        self.send_response(status)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', len(body))
        self._cors_headers()
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
            self._not_found()

    def _cors_headers(self):
        self.send_header('Access-Control-Allow-Origin',  '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type')

    def _not_found(self):
        self.send_response(404)
        self.end_headers()

    def log_message(self, fmt, *args):
        # Only log API calls, not static file requests
        if '/api/' in args[0] if args else '':
            logger.info(f"{self.address_string()} {fmt % args}")


# ─────────────────────────────────────────
# Startup
# ─────────────────────────────────────────
def startup():
    """Initialize DB and seed data if empty."""
    from database import init_db, get_stats
    init_db()
    stats = get_stats()
    if stats['total'] == 0:
        logger.info("Empty DB — seeding demo articles...")
        from pipeline import seed_demo_data
        seed_demo_data()
        # Also run extra seed
        try:
            sys.path.insert(0, BASE_DIR)
            import importlib.util, os
            seed_path = os.path.join(BASE_DIR, 'scripts', 'seed_extra.py')
            if os.path.exists(seed_path):
                spec = importlib.util.spec_from_file_location("seed_extra", seed_path)
                mod  = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
        except Exception as e:
            logger.warning(f"Extra seed error: {e}")
        stats = get_stats()
    logger.info(f"DB ready: {stats['total']} articles across {len(stats['by_category'])} categories")
    return stats


def main(port: int = 8000):
    logger.info("=" * 55)
    logger.info("  CA Portal — SSC & RRB Current Affairs")
    logger.info("=" * 55)

    stats = startup()

    logger.info(f"Starting server on http://localhost:{port}")
    logger.info(f"Articles in DB: {stats['total']}")
    logger.info("-" * 55)

    # Allow address reuse
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer(("", port), CAHandler) as httpd:
        logger.info(f"Open  →  http://localhost:{port}")
        logger.info("Press Ctrl+C to stop\n")
        try:
            httpd.serve_forever()
        except KeyboardInterrupt:
            logger.info("\nShutting down...")


if __name__ == '__main__':
    import argparse
    p = argparse.ArgumentParser(description='CA Portal Server')
    p.add_argument('--port', type=int, default=8000)
    p.add_argument('--train', action='store_true', help='Retrain classifier before starting')
    args = p.parse_args()

    if args.train:
        logger.info("Retraining classifier...")
        from backend.classifier import train_classifier
        acc = train_classifier()
        logger.info(f"Training complete — accuracy: {acc:.4f}")

    main(args.port)
