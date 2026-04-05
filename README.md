# CA Portal – Current Affairs for SSC & RRB

Exam-focused current affairs pipeline: fetch → classify → summarize → display.

## Quick Start (3 commands)

```bash
# 1. Install dependencies
pip install scikit-learn pandas numpy scipy feedparser requests trafilatura

# 2. Train the classifier on your dataset
python3 backend/classifier.py

# 3. Start the server (seeds demo data automatically)
python3 run.py
```
Open http://localhost:8000 in your browser.

---

## Full Setup

### Step 1 – Python requirements
```
pip install -r requirements.txt
```
Core packages needed:
- `scikit-learn`, `numpy`, `scipy`, `pandas` — classifier
- `feedparser`, `requests`, `trafilatura` — news fetching
- `nltk` — sentence tokenization (summarizer)

### Step 2 – Train the classifier
```bash
# Uses data/cleaned_dataset.csv (29k articles, 13 categories)
python3 backend/classifier.py
```
Expected accuracy: **~82%** across 12 exam-relevant categories.
Models saved to `models/ca_classifier.pkl` and `models/tfidf_vectorizer.pkl`.

### Step 3 – Run the server
```bash
python3 run.py
# Options:
python3 run.py --port 8080       # different port
python3 run.py --train           # retrain classifier then start
```

### Step 4 – Add your own PDF data (optional, improves accuracy)
Upload monthly CA PDFs to `data/pdfs/` then run:
```bash
python3 scripts/extract_pdfs.py   # extracts and labels from PDFs
python3 backend/classifier.py     # retrain with new data
```

---

## Architecture

```
RSS Feeds (46 sources)
    │
    ▼
news_fetcher.py          ← feedparser + requests + trafilatura
    │ (headline + full text)
    ▼
classifier.py            ← TF-IDF (word+char) + keywords + LogisticRegression
    │ confidence ≥ 0.80 AND category ≠ NOT_RELEVANT
    ▼
summarizer.py            ← Extractive bullet-point summarization
    │
    ▼
database.py              ← SQLite (date/category indexes)
    │
    ▼
run.py (HTTP server)     ← /api/news, /api/categories, /api/stats
    │
    ▼
frontend/index.html      ← Interactive portal with filters
```

## API Endpoints

| Endpoint | Description |
|---|---|
| `GET /api/news?date=YYYY-MM-DD&category=X&min_confidence=0.80` | Filtered articles |
| `GET /api/categories` | All 12 exam categories |
| `GET /api/stats` | Total count, by-category, by-date |
| `GET /api/dates` | Dates with articles (last 30 days) |
| `POST /api/fetch` | Trigger live RSS fetch (background) |

## Categories

| Category | SSC/RRB Relevance |
|---|---|
| Economy & Banking | RBI, Budget, GDP, GST, SEBI |
| Polity & Governance | Parliament, Constitution, Courts |
| International Relations | MoUs, UN, G20, BRICS, Summits |
| Science & Technology | ISRO, DRDO, Space, AI, Research |
| Schemes & Appointments | Yojanas, New appointments, Launches |
| Reports & Indices | HDI, GII, Rankings, Survey data |
| Sports | Olympics, CWG, World Cup, Medals |
| Awards & Honours | Padma, Nobel, National Awards |
| Important Days & Obituaries | World days, Anniversaries, Deaths |
| Summits & Conferences | COP, G20, SAARC, Book Fairs |
| National News | Central government, Policy, Railways |
| State News | State government, CM, State schemes |

## Environment Variables

```bash
DB_PATH=ca_articles.db              # SQLite database path
NEWSDATA_API_KEY=your_key_here      # Optional: NewsData.io API key
MAX_RSS_FEEDS=20                    # How many feeds to fetch per run
FETCH_INTERVAL_MINUTES=30          # Scheduler interval
```

## Files

```
ca_project/
├── run.py                  ← Main entry point
├── requirements.txt
├── ca_articles.db          ← SQLite database (auto-created)
├── backend/
│   ├── classifier.py       ← ML classification
│   ├── database.py         ← DB operations
│   ├── news_fetcher.py     ← RSS + API fetching
│   ├── pipeline.py         ← Main orchestrator
│   ├── scheduler.py        ← Periodic fetch scheduler
│   ├── server.py           ← FastAPI backend (optional)
│   └── summarizer.py       ← Bullet-point summarization
├── data/
│   ├── cleaned_dataset.csv ← 29k labeled training articles
│   └── rss_feeds.json      ← 46 RSS feed URLs
├── frontend/
│   └── index.html          ← Interactive web portal
├── models/
│   ├── ca_classifier.pkl   ← Trained LR model
│   └── tfidf_vectorizer.pkl← TF-IDF vectorizers
└── scripts/
    └── seed_extra.py       ← Extra seed data
```
