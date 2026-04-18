"""
Database Module — PostgreSQL only (psycopg2).

Set DATABASE_URL in your .env file before running:
    DATABASE_URL=postgresql://user:password@localhost:5432/ca_portal

All queries use %s placeholders (psycopg2 standard).
"""

import os
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

# ─── Connection ───────────────────────────────────────────────────────────────

DATABASE_URL = os.environ.get("DATABASE_URL", "")

if not DATABASE_URL:
    raise EnvironmentError(
        "\n\n  DATABASE_URL is not set!\n"
        "  Add it to your .env file:\n"
        "    DATABASE_URL=postgresql://user:password@localhost:5432/ca_portal\n"
        "  Then run:  python run.py\n"
    )


def get_connection():
    """Return a new psycopg2 connection to PostgreSQL."""
    import psycopg2
    import psycopg2.extras
    conn = psycopg2.connect(DATABASE_URL)
    return conn


def _placeholders(n: int) -> str:
    """Return n comma-separated %s placeholders for psycopg2."""
    return ",".join(["%s"] * n)


# ─── Schema Init ──────────────────────────────────────────────────────────────

def init_db():
    """
    Create tables and indexes if they don't exist.
    Safe to call on every startup (idempotent).

    Schema as per project technical script section 2.4:
        exam_ca_articles  — main articles table
        categories        — lookup table
    """
    conn = get_connection()
    cur  = conn.cursor()

    # Main articles table
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS exam_ca_articles (
                id          SERIAL PRIMARY KEY,
                date        TEXT NOT NULL,
                category    TEXT NOT NULL,
                headline    TEXT NOT NULL,
                summary     TEXT NOT NULL,
                source      TEXT,
                url         TEXT,
                url_hash    TEXT UNIQUE,
                confidence  REAL DEFAULT 0.0,
                word_count  INTEGER DEFAULT 0,
                fetched_at  TEXT DEFAULT CURRENT_TIMESTAMP,
                created_at  TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"exam_ca_articles create skipped: {e}")

    # Categories lookup table
    try:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS categories (
                id   SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL
            )
        """)
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.warning(f"categories create skipped: {e}")

    # Indexes on (date, category) for fast filtering — as per script section 2.4
    for idx_sql in [
        "CREATE INDEX IF NOT EXISTS idx_date_category ON exam_ca_articles(date, category)",
        "CREATE INDEX IF NOT EXISTS idx_date          ON exam_ca_articles(date)",
        "CREATE INDEX IF NOT EXISTS idx_category      ON exam_ca_articles(category)",
        "CREATE INDEX IF NOT EXISTS idx_confidence    ON exam_ca_articles(confidence)",
        "CREATE INDEX IF NOT EXISTS idx_url_hash      ON exam_ca_articles(url_hash)",
    ]:
        try:
            cur.execute(idx_sql)
            conn.commit()
        except Exception:
            conn.rollback()

    # Seed category lookup rows
    categories = [
        'Economy & Banking', 'Polity & Governance', 'International Relations',
        'Science & Technology', 'Schemes & Appointments', 'Reports & Indices',
        'Sports', 'Awards & Honours', 'Important Days & Obituaries',
        'Summits & Conferences', 'National News', 'State News',
    ]
    for cat in categories:
        try:
            cur.execute(
                "INSERT INTO categories (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (cat,)
            )
        except Exception:
            pass

    conn.commit()
    conn.close()
    logger.info("PostgreSQL database initialized.")


# ─── Insert ───────────────────────────────────────────────────────────────────

def insert_article(data: Dict[str, Any]) -> bool:
    """
    Insert one article. Uses ON CONFLICT (url_hash) DO UPDATE so re-runs
    are safe and always refresh summary/confidence if the URL was re-fetched.
    Returns True if a new row was inserted or updated.
    """
    conn = get_connection()
    cur  = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO exam_ca_articles
                (date, category, headline, summary, source, url,
                 url_hash, confidence, word_count, fetched_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (url_hash) DO UPDATE SET
                summary    = EXCLUDED.summary,
                confidence = EXCLUDED.confidence,
                url        = EXCLUDED.url,
                fetched_at = EXCLUDED.fetched_at
        """, (
            data.get('date',       str(date.today())),
            data['category'],
            data['headline'],
            data['summary'],
            data.get('source',     ''),
            data.get('url',        ''),
            data.get('url_hash',   ''),
            data.get('confidence', 0.0),
            data.get('word_count', 0),
            data.get('fetched_at', datetime.now().isoformat()),
        ))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logger.error(f"insert_article error: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


# ─── Queries ──────────────────────────────────────────────────────────────────

def get_articles(
    date_filter:     Optional[str] = None,
    category_filter: Optional[str] = None,
    min_confidence:  float         = 0.8,
    limit:           int           = 100,
    offset:          int           = 0,
    date_from:       Optional[str] = None,
    search:          Optional[str] = None,
) -> List[Dict]:
    """
    Return articles matching filters.
    Supports: date (exact), date_from (range), category, search (ILIKE), confidence threshold.
    As per script section 2.6 GET /api/news endpoint.
    """
    conn   = get_connection()
    cur    = conn.cursor()
    q      = """
        SELECT id, date, category, headline, summary,
               source, confidence, url, word_count, fetched_at
        FROM exam_ca_articles
        WHERE confidence >= %s
    """
    params = [min_confidence]

    if date_filter:
        q += " AND date = %s";      params.append(date_filter)
    elif date_from:
        q += " AND date >= %s";     params.append(date_from)

    if category_filter and category_filter != 'All':
        q += " AND category = %s";  params.append(category_filter)

    if search and search.strip():
        q += " AND (headline ILIKE %s OR summary ILIKE %s)"
        params.extend([f'%{search}%', f'%{search}%'])

    q += " ORDER BY date DESC, confidence DESC LIMIT %s OFFSET %s"
    params.extend([limit, offset])

    cur.execute(q, params)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    conn.close()
    return rows


def get_article_count(
    date_filter:     Optional[str] = None,
    category_filter: Optional[str] = None,
    min_confidence:  float         = 0.8,
    date_from:       Optional[str] = None,
    search:          Optional[str] = None,
) -> int:
    """Return total count matching the same filters as get_articles (for pagination)."""
    conn   = get_connection()
    cur    = conn.cursor()
    q      = "SELECT COUNT(*) FROM exam_ca_articles WHERE confidence >= %s"
    params = [min_confidence]

    if date_filter:
        q += " AND date = %s";      params.append(date_filter)
    elif date_from:
        q += " AND date >= %s";     params.append(date_from)

    if category_filter and category_filter != 'All':
        q += " AND category = %s";  params.append(category_filter)

    if search and search.strip():
        q += " AND (headline ILIKE %s OR summary ILIKE %s)"
        params.extend([f'%{search}%', f'%{search}%'])

    cur.execute(q, params)
    count = cur.fetchone()[0]
    conn.close()
    return count


def get_categories() -> List[str]:
    """Return all category names from lookup table."""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT name FROM categories ORDER BY name")
    cats = [r[0] for r in cur.fetchall()]
    conn.close()
    return cats


def get_dates_with_articles(days: int = 30) -> List[str]:
    """Return distinct dates that have at least one article, most recent first."""
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute(
        "SELECT DISTINCT date FROM exam_ca_articles "
        "WHERE confidence >= 0.8 ORDER BY date DESC LIMIT %s",
        (days,)
    )
    dates = [r[0] for r in cur.fetchall()]
    conn.close()
    return dates


def get_stats() -> Dict:
    """Return summary statistics for the dashboard."""
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM exam_ca_articles WHERE confidence >= 0.8")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT category, COUNT(*) AS cnt
        FROM exam_ca_articles
        WHERE confidence >= 0.8
        GROUP BY category ORDER BY cnt DESC
    """)
    by_cat = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        SELECT date, COUNT(*) AS cnt
        FROM exam_ca_articles
        WHERE confidence >= 0.8
        GROUP BY date ORDER BY date DESC LIMIT 30
    """)
    by_date = {r[0]: r[1] for r in cur.fetchall()}

    today_str  = str(date.today())
    week_dates = [str(date.today() - timedelta(days=i)) for i in range(7)]
    cur.execute(
        f"SELECT COUNT(*) FROM exam_ca_articles "
        f"WHERE date IN ({_placeholders(len(week_dates))}) AND confidence >= 0.8",
        week_dates
    )
    week_count  = cur.fetchone()[0]
    today_count = by_date.get(today_str, 0)

    conn.close()
    return {
        "total":       total,
        "by_category": by_cat,
        "by_date":     by_date,
        "today":       today_count,
        "this_week":   week_count,
    }


def get_category_counts(
    date_filter: Optional[str] = None,
    date_from:   Optional[str] = None,
    min_confidence: float = 0.80,
) -> Dict:
    """Return per-category article counts with optional date filtering."""
    conn = get_connection()
    cur  = conn.cursor()

    conditions = ["confidence >= %s"]
    params: List = [min_confidence]

    if date_filter:
        conditions.append("date = %s")
        params.append(date_filter)
    elif date_from:
        conditions.append("date >= %s")
        params.append(date_from)

    where = " AND ".join(conditions)

    cur.execute(
        f"SELECT COUNT(*) FROM exam_ca_articles WHERE {where}",
        params,
    )
    total = cur.fetchone()[0]

    cur.execute(
        f"SELECT category, COUNT(*) AS cnt FROM exam_ca_articles "
        f"WHERE {where} GROUP BY category",
        params,
    )
    by_cat = {r[0]: r[1] for r in cur.fetchall()}

    conn.close()
    return {"total": total, "by_category": by_cat}


def url_exists(url_hash: str) -> bool:
    """Check if an article with this url_hash is already in the DB."""
    conn   = get_connection()
    cur    = conn.cursor()
    cur.execute("SELECT 1 FROM exam_ca_articles WHERE url_hash = %s", (url_hash,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


# ─── Cleanup ─────────────────────────────────────────────────────────────────

# Keywords in headlines/summaries that indicate non-CA general news
GENERAL_NEWS_KEYWORDS = [
    # Weather & Climate (general)
    'weather forecast', 'weather update', 'temperature today', 'rain forecast',
    'monsoon update', 'cyclone warning', 'heat wave alert', 'cold wave alert',
    'fog alert', 'weather alert', 'imd forecast',
    # Crime & Accidents
    'murder case', 'robbery', 'theft case', 'arrested for', 'crime news',
    'road accident', 'car crash', 'train accident', 'bus accident',
    'fire breaks out', 'building collapse', 'blast kills', 'shooting incident',
    'rape case', 'kidnapping', 'drug bust', 'gang war',
    # Elections (local/state voting news, not policy)
    'voting begins', 'voting ends', 'voter turnout', 'exit poll',
    'election result', 'counting begins', 'bypolls', 'by-election result',
    # Traffic & Local
    'traffic jam', 'traffic update', 'road block', 'waterlogging',
    'pothole', 'flyover closed',
    # Entertainment
    'bollywood', 'box office', 'movie review', 'film review', 'web series',
    'celebrity gossip', 'celebrity wedding', 'celebrity baby',
    'tv serial', 'reality show', 'ott release',
    # Lifestyle
    'horoscope', 'astrology', 'zodiac', 'recipe', 'cooking tips',
    'weight loss', 'hair care', 'skin care', 'beauty tips', 'fashion week',
    # Sports scores (not CA relevant)
    'match score', 'live score', 'scorecard', 'ipl score',
]


def cleanup_database(dry_run: bool = False) -> Dict:
    """
    Remove articles from DB that:
    1. Have confidence < 0.85
    2. Contain general news keywords in headline or summary

    If dry_run=True, only returns count without deleting.
    Returns dict with counts of removed articles.
    """
    conn = get_connection()
    cur  = conn.cursor()

    removed_low_conf = 0
    removed_general  = 0

    # 1. Remove low confidence articles
    if dry_run:
        cur.execute("SELECT COUNT(*) FROM exam_ca_articles WHERE confidence < 0.85")
        removed_low_conf = cur.fetchone()[0]
    else:
        cur.execute("DELETE FROM exam_ca_articles WHERE confidence < 0.85")
        removed_low_conf = cur.rowcount
        conn.commit()
        logger.info(f"Removed {removed_low_conf} low-confidence articles (< 0.85)")

    # 2. Remove general news by keyword matching
    for kw in GENERAL_NEWS_KEYWORDS:
        pattern = f'%{kw}%'
        if dry_run:
            cur.execute(
                "SELECT COUNT(*) FROM exam_ca_articles "
                "WHERE LOWER(headline) LIKE %s OR LOWER(summary) LIKE %s",
                (pattern, pattern)
            )
            removed_general += cur.fetchone()[0]
        else:
            cur.execute(
                "DELETE FROM exam_ca_articles "
                "WHERE LOWER(headline) LIKE %s OR LOWER(summary) LIKE %s",
                (pattern, pattern)
            )
            removed_general += cur.rowcount
            conn.commit()

    if not dry_run:
        logger.info(f"Removed {removed_general} general news articles")

    conn.close()
    return {
        'removed_low_confidence': removed_low_conf,
        'removed_general_news':   removed_general,
        'total_removed':          removed_low_conf + removed_general,
        'dry_run':                dry_run,
    }


# ─── Quick test ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    init_db()
    print("PostgreSQL connected and schema ready.")
    print("Stats:", get_stats())