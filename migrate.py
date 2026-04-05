import psycopg2
import psycopg2.extras

PG_URL = "postgresql://ca_portal_db_68b1_user:ZRjyNxKDO6PW8gwP8pqvri5SqgZUxFMD@dpg-d790j0nfte5s739cvlug-a.virginia-postgres.render.com/ca_portal_db_68b1"

# Local PostgreSQL connect
local = psycopg2.connect("postgresql://postgres:1234@localhost:5432/ca_portal")
local_cur = local.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

# Render PostgreSQL connect
render = psycopg2.connect(PG_URL)
render_cur = render.cursor()

# Add url column if not exists
render_cur.execute("ALTER TABLE exam_ca_articles ADD COLUMN IF NOT EXISTS url TEXT")
render.commit()

# Fetch all from local
local_cur.execute("SELECT * FROM exam_ca_articles")
rows = local_cur.fetchall()
print(f"Local articles: {len(rows)}")

ok = 0
skip = 0
for r in rows:
    try:
        render_cur.execute("""
        INSERT INTO exam_ca_articles 
        (date, category, headline, summary, source, url_hash, confidence, word_count, fetched_at, created_at, url)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (url_hash) DO NOTHING
        """, (
            r['date'], r['category'], r['headline'], r['summary'],
            r['source'], r['url_hash'], r['confidence'], r['word_count'],
            str(r['fetched_at']), str(r['created_at']), r['url']
        ))
        ok += 1
    except Exception as e:
        print(f"Skip: {e}")
        render.rollback()
        skip += 1

render.commit()
print(f"✅ Inserted: {ok}, Skipped: {skip}")

local.close()
render.close()