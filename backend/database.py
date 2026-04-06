"""
Database module - SQLite (dev) or PostgreSQL (prod).
Set DATABASE_URL env var to use PostgreSQL:
  DATABASE_URL=postgresql://user:pass@host:5432/dbname
"""

import os
import logging
from datetime import datetime, date, timedelta
from typing import Optional, List, Dict, Any

logger = logging.getLogger(__name__)

DATABASE_URL = os.environ.get("DATABASE_URL", "")
DB_PATH      = os.environ.get("DB_PATH", "ca_articles.db")

USE_PG = bool(DATABASE_URL)

# ─────────────────────────────────────────
# Connection
# ─────────────────────────────────────────

def get_connection():
    if USE_PG:
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL)
        return conn
    else:
        import sqlite3
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn


def _ph(n: int = 1) -> str:
    """Return placeholder: %s for PG, ? for SQLite."""
    return "%s" if USE_PG else "?"


def _placeholders(n: int) -> str:
    ph = "%s" if USE_PG else "?"
    return ",".join([ph] * n)


def _row_to_dict(row) -> Dict:
    if USE_PG:
        return dict(row)
    import sqlite3
    return dict(row)


# ─────────────────────────────────────────
# Init
# ─────────────────────────────────────────

def init_db():
    conn = get_connection()
    cur  = conn.cursor()

    if USE_PG:
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
            logger.warning(f"exam_ca_articles table create skipped: {e}")
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
            logger.warning(f"categories table create skipped: {e}")
        for idx in [
            "CREATE INDEX IF NOT EXISTS idx_date_category ON exam_ca_articles(date, category)",
            "CREATE INDEX IF NOT EXISTS idx_date          ON exam_ca_articles(date)",
            "CREATE INDEX IF NOT EXISTS idx_category      ON exam_ca_articles(category)",
            "CREATE INDEX IF NOT EXISTS idx_confidence    ON exam_ca_articles(confidence)",
            "CREATE INDEX IF NOT EXISTS idx_url_hash      ON exam_ca_articles(url_hash)",
        ]:
            try:
                cur.execute(idx)
                conn.commit()
            except Exception:
                conn.rollback()
        # Add url column if not exists — use separate try/rollback
        try:
            cur.execute("ALTER TABLE exam_ca_articles ADD COLUMN url TEXT")
            conn.commit()
        except Exception:
            conn.rollback()
    else:
        cur.executescript("""
            CREATE TABLE IF NOT EXISTS exam_ca_articles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
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
            );
            CREATE TABLE IF NOT EXISTS categories (
                id   INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_date_category ON exam_ca_articles(date, category);
            CREATE INDEX IF NOT EXISTS idx_date          ON exam_ca_articles(date);
            CREATE INDEX IF NOT EXISTS idx_category      ON exam_ca_articles(category);
            CREATE INDEX IF NOT EXISTS idx_confidence    ON exam_ca_articles(confidence);
            CREATE INDEX IF NOT EXISTS idx_url_hash      ON exam_ca_articles(url_hash);
        """)
        # Migrate: add url column if not exists
        try:
            cur.execute("ALTER TABLE exam_ca_articles ADD COLUMN url TEXT")
            conn.commit()
        except Exception:
            pass

    categories = [
        'Economy & Banking', 'Polity & Governance', 'International Relations',
        'Science & Technology', 'Schemes & Appointments', 'Reports & Indices',
        'Sports', 'Awards & Honours', 'Important Days & Obituaries',
        'Summits & Conferences', 'National News', 'State News'
    ]
    ph = _ph()
    for cat in categories:
        try:
            cur.execute(f"INSERT INTO categories (name) SELECT {ph} WHERE NOT EXISTS (SELECT 1 FROM categories WHERE name={ph})", (cat, cat))
        except Exception:
            pass

    conn.commit()

    # Backfill url for existing demo data rows that have url_hash but no url
    demo_url_map = {
        'rbi_repo_feb2026':       'https://rbi.org.in/news/2026/repo-rate',
        'isro_nvs02_jan2026':     'https://isro.gov.in/nvs02-launch',
        'india_france_mous_2026': 'https://mea.gov.in/india-france-mous',
        'india_awg2026':          'https://sports.gov.in/asian-winter-games-2026',
        'undp_hdi_2025':          'https://undp.org/hdi-2025',
        'vande_bharat_10_2026':   'https://indianrailways.gov.in/vande-bharat-2026',
        'padma_awards_2026':      'https://mha.gov.in/padma-2026',
        'wgs_dubai_2026':         'https://worldgovernmentsummit.org/2026',
        'budget_2026_27':         'https://indiabudget.gov.in/2026-27',
        'pm_surya_ghar_1cr':      'https://pmsuryaghar.gov.in/milestone',
        'aiims_proton_2026':      'https://aiims.edu/proton-therapy-centre',
        'forex_reserves_720bn':   'https://rbi.org.in/forex-reserves-jan2026',
        'nari_shakti_2026':       'https://loksabha.nic.in/women-reservation',
        'women_science_day_2026': 'https://dst.gov.in/women-science-day-2026',
        'kolkata_book_fair_2026': 'https://kolkatabookfair.net/2026',
        'gst_56th_council':       'https://gstcouncil.gov.in/56th-meeting',
    }
    ph2 = _ph()
    for url_hash, url in demo_url_map.items():
        try:
            if USE_PG:
                cur.execute("UPDATE exam_ca_articles SET url=%s WHERE url_hash=%s AND (url IS NULL OR url='')", (url, url_hash))
            else:
                cur.execute("UPDATE exam_ca_articles SET url=? WHERE url_hash=? AND (url IS NULL OR url='')", (url, url_hash))
        except Exception:
            pass

    conn.commit()
    conn.close()
    logger.info(f"Database initialized ({'PostgreSQL' if USE_PG else 'SQLite'})")


# ─────────────────────────────────────────
# Insert
# ─────────────────────────────────────────

def insert_article(data: Dict[str, Any]) -> bool:
    conn = get_connection()
    cur  = conn.cursor()
    try:
        if USE_PG:
            cur.execute("""
                INSERT INTO exam_ca_articles
                    (date, category, headline, summary, source, url, url_hash, confidence, word_count, fetched_at)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (url_hash) DO UPDATE SET
                    url        = EXCLUDED.url,
                    summary    = EXCLUDED.summary,
                    confidence = EXCLUDED.confidence,
                    fetched_at = EXCLUDED.fetched_at
            """, (
                data.get('date', str(date.today())),
                data['category'], data['headline'], data['summary'],
                data.get('source', ''), data.get('url', ''), data.get('url_hash', ''),
                data.get('confidence', 0.0), data.get('word_count', 0),
                data.get('fetched_at', datetime.now().isoformat())
            ))
        else:
            cur.execute("""
                INSERT OR IGNORE INTO exam_ca_articles
                    (date, category, headline, summary, source, url, url_hash, confidence, word_count, fetched_at)
                VALUES (?,?,?,?,?,?,?,?,?,?)
            """, (
                data.get('date', str(date.today())),
                data['category'], data['headline'], data['summary'],
                data.get('source', ''), data.get('url', ''), data.get('url_hash', ''),
                data.get('confidence', 0.0), data.get('word_count', 0),
                data.get('fetched_at', datetime.now().isoformat())
            ))
        conn.commit()
        return cur.rowcount > 0
    except Exception as e:
        logger.error(f"Insert error: {e}")
        conn.rollback()
        return False
    finally:
        conn.close()


# ─────────────────────────────────────────
# Query
# ─────────────────────────────────────────

def get_articles(
    date_filter:     Optional[str] = None,
    category_filter: Optional[str] = None,
    min_confidence:  float = 0.8,
    limit:           int   = 100,
    offset:          int   = 0,
    date_from:       Optional[str] = None,
    search:          Optional[str] = None,
) -> List[Dict]:
    conn = get_connection()
    cur  = conn.cursor()
    ph   = _ph()

    q      = f"SELECT id,date,category,headline,summary,source,confidence,url,word_count,fetched_at FROM exam_ca_articles WHERE confidence >= {ph}"
    params = [min_confidence]

    if date_filter:
        q += f" AND date = {ph}"; params.append(date_filter)
    elif date_from:
        q += f" AND date >= {ph}"; params.append(date_from)

    if category_filter and category_filter != 'All':
        q += f" AND category = {ph}"; params.append(category_filter)

    if search and search.strip():
        if USE_PG:
            q += f" AND (headline ILIKE {ph} OR summary ILIKE {ph})"
            params.extend([f'%{search}%', f'%{search}%'])
        else:
            q += f" AND (headline LIKE {ph} OR summary LIKE {ph})"
            params.extend([f'%{search}%', f'%{search}%'])

    q += f" ORDER BY date DESC, confidence DESC LIMIT {ph} OFFSET {ph}"
    params.extend([limit, offset])

    cur.execute(q, params)

    if USE_PG:
        cols = [d[0] for d in cur.description]
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
    else:
        rows = [dict(r) for r in cur.fetchall()]

    conn.close()
    return rows


def get_article_count(
    date_filter:     Optional[str] = None,
    category_filter: Optional[str] = None,
    min_confidence:  float = 0.8,
    date_from:       Optional[str] = None,
    search:          Optional[str] = None,
) -> int:
    conn = get_connection()
    cur  = conn.cursor()
    ph   = _ph()

    q      = f"SELECT COUNT(*) FROM exam_ca_articles WHERE confidence >= {ph}"
    params = [min_confidence]

    if date_filter:
        q += f" AND date = {ph}"; params.append(date_filter)
    elif date_from:
        q += f" AND date >= {ph}"; params.append(date_from)

    if category_filter and category_filter != 'All':
        q += f" AND category = {ph}"; params.append(category_filter)

    if search and search.strip():
        if USE_PG:
            q += f" AND (headline ILIKE {ph} OR summary ILIKE {ph})"
            params.extend([f'%{search}%', f'%{search}%'])
        else:
            q += f" AND (headline LIKE {ph} OR summary LIKE {ph})"
            params.extend([f'%{search}%', f'%{search}%'])

    cur.execute(q, params)
    count = cur.fetchone()[0]
    conn.close()
    return count


def get_categories() -> List[str]:
    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT name FROM categories ORDER BY name")
    cats = [r[0] for r in cur.fetchall()]
    conn.close()
    return cats


def get_dates_with_articles(days: int = 30) -> List[str]:
    conn = get_connection()
    cur  = conn.cursor()
    ph   = _ph()
    cur.execute(f"SELECT DISTINCT date FROM exam_ca_articles WHERE confidence >= 0.8 ORDER BY date DESC LIMIT {ph}", (days,))
    dates = [r[0] for r in cur.fetchall()]
    conn.close()
    return dates


def get_stats() -> Dict:
    conn = get_connection()
    cur  = conn.cursor()

    cur.execute("SELECT COUNT(*) FROM exam_ca_articles WHERE confidence >= 0.8")
    total = cur.fetchone()[0]

    cur.execute("""
        SELECT category, COUNT(*) as cnt FROM exam_ca_articles
        WHERE confidence >= 0.8
        GROUP BY category ORDER BY cnt DESC
    """)
    if USE_PG:
        by_cat = {r[0]: r[1] for r in cur.fetchall()}
    else:
        by_cat = {r[0]: r[1] for r in cur.fetchall()}

    cur.execute("""
        SELECT date, COUNT(*) as cnt FROM exam_ca_articles
        WHERE confidence >= 0.8
        GROUP BY date ORDER BY date DESC LIMIT 30
    """)
    by_date = {r[0]: r[1] for r in cur.fetchall()}

    today_str  = str(date.today())
    week_dates = [str(date.today() - timedelta(days=i)) for i in range(7)]
    phs        = _placeholders(len(week_dates))
    cur.execute(
        f"SELECT COUNT(*) FROM exam_ca_articles WHERE date IN ({phs}) AND confidence >= 0.8",
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


def url_exists(url_hash: str) -> bool:
    conn = get_connection()
    cur  = conn.cursor()
    ph   = _ph()
    cur.execute(f"SELECT 1 FROM exam_ca_articles WHERE url_hash = {ph}", (url_hash,))
    exists = cur.fetchone() is not None
    conn.close()
    return exists


if __name__ == "__main__":
    init_db()
    print("DB:", "PostgreSQL" if USE_PG else "SQLite @", DB_PATH)
    print("Stats:", get_stats())