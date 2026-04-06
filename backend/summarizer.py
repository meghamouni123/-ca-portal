"""
Summarization Module
- Generates BULLET-POINT summaries ONLY from body content
- Headline is NEVER repeated as a bullet
- Articles with no extractable content are discarded (return None)
- Each bullet = one distinct fact, not a rehash of the headline
"""

import re
import math
import logging
from typing import List, Optional
from collections import Counter

logger = logging.getLogger(__name__)

IMPORTANT_TERMS = [
    'launched', 'inaugurated', 'signed', 'appointed', 'elected', 'won',
    'first', 'india', 'government', 'ministry', 'scheme', 'agreement',
    'awarded', 'ranked', 'record', 'approved', 'passed', 'introduced',
    'percentage', 'crore', 'billion', 'million', 'lakh', 'established',
]

NUM_PATTERN = re.compile(r'\b\d+(?:\.\d+)?(?:\s*%|\s*crore|\s*billion|\s*million|\s*lakh)?\b')
STOPWORDS = {
    'the','a','an','and','or','but','in','on','at','to','for','of','with',
    'by','from','is','are','was','were','be','been','has','have','had',
    'do','does','did','will','would','could','should','may','might',
    'this','that','these','those','it','its','as','also','which','who',
    'not','no','so','such','into','about','after','before','during',
    'said','says','told','according','he','she','they','we','their','his','her'
}


def _split_sentences(text: str) -> List[str]:
    # Abbreviations that should NOT trigger a sentence split
    ABBREVS = r'(?:Mr|Mrs|Ms|Dr|Prof|Sr|Jr|St|vs|etc|approx|dept|govt|min|max|avg|no|vol|fig|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec|U\.S|U\.K|Rs)'
    INITIALS = r'(?:[A-Z])'

    # Protect abbreviations: replace their dot with a placeholder
    protected = re.sub(rf'\b({ABBREVS})\.', r'\1<DOT>', text.strip())
    # Protect initials like "A. B. Kumar"
    protected = re.sub(rf'\b({INITIALS})\.(?=\s)', r'\1<DOT>', protected)

    # Now split on sentence-ending punctuation
    sents = re.split(r'(?<=[.!?])\s+', protected)

    # Restore placeholders and filter short
    sents = [s.replace('<DOT>', '.').strip() for s in sents]
    return [s for s in sents if len(s.split()) >= 6]


def _word_freq(sentences: List[str]) -> dict:
    all_words = []
    for s in sentences:
        words = [w.lower().strip('.,!?;:"()[]') for w in s.split()]
        all_words.extend([w for w in words if w not in STOPWORDS and len(w) > 2])
    if not all_words:
        return {}
    tf = Counter(all_words)
    total = sum(tf.values())
    doc_count = Counter()
    for s in sentences:
        doc_count.update(set(w.lower().strip('.,!?;:"()[]') for w in s.split()))
    n = len(sentences)
    return {w: (c/total) * math.log((n+1)/(doc_count.get(w,0)+1))
            for w, c in tf.items()}


def _score(sent: str, freq: dict, pos: int, total: int) -> float:
    words = sent.lower().split()
    if not words:
        return 0.0
    ws = sum(freq.get(w.strip('.,!?;:"()[]'), 0) for w in words) / len(words)
    ks = sum(1 for t in IMPORTANT_TERMS if t in sent.lower()) * 0.15
    ns = min(len(NUM_PATTERN.findall(sent)) * 0.12, 0.35)
    ps = 0.25 if pos == 0 else (0.15 if pos <= total*0.2 else 0.0)
    lp = 1.0 if len(words) <= 35 else 0.75
    return (ws + ks + ns + ps) * lp


def _is_similar_to_headline(sentence: str, headline: str, threshold: float = 0.55) -> bool:
    """Returns True if sentence is too similar to headline (headline rehash)."""
    def words(s):
        return set(re.sub(r'[^a-z0-9\s]', ' ', s.lower()).split()) - STOPWORDS
    h_words = words(headline)
    s_words = words(sentence)
    if not h_words:
        return False
    overlap = len(h_words & s_words) / len(h_words)
    return overlap >= threshold


def generate_summary(
    headline: str,
    text: str,
    num_bullets: int = 6,
    min_bullets: int = 2,          # discard if we can't get at least this many
) -> Optional[str]:
    """
    Generate bullet-point summary from body content.

    Returns:
        str  — newline-joined bullet points (each starts with '• ')
        None — if content is too thin to produce real bullets (article should be discarded)
    """
    # ── 1. Clean and split ──────────────────────────────────────
    body = text.strip()

    # Remove the headline itself from the body if it appears verbatim
    hl_clean = re.sub(r'[^\w\s]', '', headline.lower()).strip()
    sentences = _split_sentences(body)

    if not sentences:
        return None

    # ── 2. Filter out sentences that are just restating the headline ─
    non_hl_sents = [s for s in sentences if not _is_similar_to_headline(s, headline)]

    # If filtering removed everything, we have no real content
    if len(non_hl_sents) < min_bullets:
        return None

    # ── 3. Score and rank remaining sentences ────────────────────
    freq = _word_freq(non_hl_sents)
    scored = sorted(
        enumerate(non_hl_sents),
        key=lambda x: _score(x[1], freq, x[0], len(non_hl_sents)),
        reverse=True
    )

    # Take top N, restore original order
    top = sorted(scored[:num_bullets], key=lambda x: x[0])
    selected = [s for _, s in top]

    if len(selected) < min_bullets:
        return None

    # ── 4. Format as bullets ─────────────────────────────────────
    bullets = []
    for sent in selected:
        sent = sent.strip().rstrip('.')
        if not sent:
            continue
        # Truncate very long sentences
        if len(sent) > 210:
            sent = sent[:210].rsplit(' ', 1)[0] + '…'
        # Capitalise
        sent = sent[0].upper() + sent[1:]
        bullets.append(f'• {sent}.')

    if len(bullets) < min_bullets:
        return None

    return '\n'.join(bullets)


def regenerate_all_summaries(db_path: str = None):
    """
    Re-runs summarization on every article already in the DB.
    - Removes the headline-repeat first bullet
    - Discards articles where no real content remains
    """
    import os, sys
    if db_path:
        os.environ['DB_PATH'] = db_path
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from database import get_connection

    conn = get_connection()
    cur  = conn.cursor()
    cur.execute("SELECT id, headline, summary FROM exam_ca_articles")
    rows = cur.fetchall()

    updated = discarded = already_ok = 0

    for row_id, headline, summary in rows:
        if not summary:
            cur.execute("DELETE FROM exam_ca_articles WHERE id=?", (row_id,))
            discarded += 1
            continue

        # Parse existing bullet lines (strip '• ' prefix)
        raw_lines = [l.strip().lstrip('• ').rstrip('.')
                     for l in summary.split('\n') if l.strip()]

        # Remove lines that are too similar to headline
        clean = [l for l in raw_lines if not _is_similar_to_headline(l, headline)]

        if len(clean) < 2:
            # Not enough distinct content — discard
            cur.execute("DELETE FROM exam_ca_articles WHERE id=?", (row_id,))
            discarded += 1
            continue

        # Re-format cleanly
        new_summary = '\n'.join(f'• {l[0].upper()+l[1:]}.' for l in clean if l)

        if new_summary != summary:
            cur.execute("UPDATE exam_ca_articles SET summary=? WHERE id=?",
                        (new_summary, row_id))
            updated += 1
        else:
            already_ok += 1

    conn.commit()
    conn.close()
    logger.info(f"regenerate_all_summaries: updated={updated} discarded={discarded} ok={already_ok}")
    return updated, discarded


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    import os, sys
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

    tests = [
        (
            'RBI keeps repo rate unchanged at 6.5% for sixth consecutive time',
            """RBI keeps repo rate unchanged at 6.5% for sixth consecutive time.
            The Monetary Policy Committee voted 5:1 to hold the key policy rate.
            The RBI maintained its stance of withdrawal of accommodation.
            GDP growth for FY26 projected at 6.8%.
            Retail inflation forecast at 4.5% for Q4 FY26.
            The Standing Deposit Facility rate remains at 6.25%.
            Governor Sanjay Malhotra chaired the meeting held in Mumbai."""
        ),
        (
            'India wins 22 medals at 2026 Asian Winter Games in Harbin',
            """India wins 22 medals at 2026 Asian Winter Games in Harbin.
            Total medals: 7 gold, 8 silver, 7 bronze.
            Speed skater Aryan Singh won 3 gold medals.
            India finished 6th in the overall medal tally.
            China topped with 87 gold medals."""
        ),
        (
            'Fake headline with nothing else',
            'Fake headline with nothing else.'   # only headline — should return None
        ),
    ]

    for h, t in tests:
        result = generate_summary(h, t)
        print(f'\nHEADLINE: {h}')
        if result:
            print('SUMMARY:')
            for line in result.split('\n'):
                print(f'  {line}')
        else:
            print('RESULT: None — article discarded (no real content)')