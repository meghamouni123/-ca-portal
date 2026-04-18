"""
Main Pipeline
Orchestrates: fetch → classify → deduplicate → summarize → store

Runs on schedule (every 30 minutes) or can be triggered manually.
"""

import os
import sys
import logging
import hashlib
import json
import re
from datetime import datetime, date, timedelta
from typing import List, Dict, Optional, Tuple
import numpy as np

# Add parent dir to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from database import init_db, insert_article, url_exists, get_stats
from classifier import get_classifier
from summarizer import generate_summary

logger = logging.getLogger(__name__)

# Confidence threshold - only store articles above this
CONFIDENCE_THRESHOLD = 0.85
# Cosine similarity threshold for deduplication (if embeddings available)
DEDUP_SIMILARITY_THRESHOLD = 0.95


class Pipeline:
    """
    Main processing pipeline.
    classify → deduplicate → summarize → store
    """

    def __init__(self):
        self.classifier = get_classifier()
        self._url_hash_cache = set()  # In-memory URL dedup
        self._processed_count = 0
        self._stored_count = 0
        self._skipped_count = 0

    def _compute_text_hash(self, text: str) -> str:
        """Compute hash of first 200 chars for quick text dedup."""
        normalized = re.sub(r'\s+', ' ', text[:200].lower().strip())
        return hashlib.md5(normalized.encode()).hexdigest()[:12]

    def process_article(self, article: Dict) -> Optional[Dict]:
        """
        Process a single article through the pipeline.
        Returns stored article dict or None if discarded.
        """
        url_hash = article.get('url_hash', '')
        headline = article.get('headline', '').strip()
        text = article.get('text', '').strip()
        url = article.get('url', '')

        # Skip if URL already in DB or cache
        if url_hash:
            if url_hash in self._url_hash_cache:
                logger.debug(f"Skipping cached duplicate: {headline[:50]}")
                return None
            if url_exists(url_hash):
                logger.debug(f"Skipping DB duplicate: {headline[:50]}")
                self._url_hash_cache.add(url_hash)
                return None

        # Ensure we have enough text
        if len(text.split()) < 10:
            text = headline

        # ─── STEP 1: Classification ───
        full_text = f"{headline}. {text}" if headline not in text else text
        is_relevant, category, confidence = self.classifier.is_exam_relevant(
            full_text, threshold=CONFIDENCE_THRESHOLD
        )

        self._processed_count += 1

        if not is_relevant:
            logger.debug(f"Not relevant ({category}, {confidence:.2f}): {headline[:60]}")
            self._skipped_count += 1
            if url_hash:
                self._url_hash_cache.add(url_hash)
            return None

        logger.info(f"✓ [{category}] ({confidence:.2f}) {headline[:70]}")

        # ─── STEP 2: Summarization ───
        summary = generate_summary(headline, text, num_bullets=6)

        # Discard if summarizer couldn't extract real content
        if summary is None:
            logger.debug(f"Discarded (no extractable content): {headline[:60]}")
            self._skipped_count += 1
            if url_hash:
                self._url_hash_cache.add(url_hash)
            return None

        # ─── STEP 3: Store in DB ───
        article_date = article.get('date', str(date.today()))

        stored = {
            'date': article_date,
            'category': category,
            'headline': headline,
            'summary': summary,
            'source': article.get('source', ''),
            'url': url,
            'url_hash': url_hash,
            'confidence': round(confidence, 4),
            'word_count': len(text.split()),
            'fetched_at': article.get('fetched_at', datetime.now().isoformat()),
        }

        inserted = insert_article(stored)
        if inserted:
            self._stored_count += 1
            if url_hash:
                self._url_hash_cache.add(url_hash)
            return stored
        else:
            logger.debug(f"Already in DB: {headline[:50]}")
            return None

    def process_batch(self, articles: List[Dict]) -> List[Dict]:
        """Process a batch of articles."""
        stored = []
        for article in articles:
            result = self.process_article(article)
            if result:
                stored.append(result)

        logger.info(
            f"Batch complete: {self._processed_count} processed, "
            f"{self._stored_count} stored, {self._skipped_count} skipped"
        )
        return stored

    def get_stats(self) -> Dict:
        return {
            'processed': self._processed_count,
            'stored': self._stored_count,
            'skipped': self._skipped_count,
        }


# ─── Demo / Seed Data ───
DEMO_ARTICLES = [
    {
        "headline": "RBI keeps repo rate unchanged at 6.5% for sixth consecutive time",
        "text": """The Reserve Bank of India (RBI) has kept the repo rate unchanged at 6.5% for the sixth
        consecutive time in its monetary policy review meeting. The Monetary Policy Committee (MPC) voted
        5:1 to hold the key policy rate. The RBI maintained its stance of 'withdrawal of accommodation'.
        GDP growth for FY26 projected at 6.8%. Retail inflation forecast at 4.5% for Q4 FY26.
        The Standing Deposit Facility (SDF) rate remains at 6.25%. The Marginal Standing Facility (MSF)
        rate and Bank Rate stand at 6.75%. CRR maintained at 4% of NDTL. Governor Sanjay Malhotra
        chaired the meeting held in Mumbai from February 5-7, 2026.""",
        "url": "https://rbi.org.in/news/2026/repo-rate",
        "url_hash": "rbi_repo_feb2026",
        "source": "RBI Official",
        "date": "2026-02-07",
    },
    {
        "headline": "ISRO successfully launches NVS-02 navigation satellite aboard GSLV-F15",
        "text": """Indian Space Research Organisation (ISRO) has successfully launched the NVS-02 navigation
        satellite aboard the GSLV-F15 rocket from the Satish Dhawan Space Centre in Sriharikota on
        January 29, 2026. NVS-02 is the second satellite of the NavIC (Navigation with Indian Constellation)
        second-generation series. The satellite will strengthen India's independent regional navigation
        system. NVS-02 weighs approximately 2,250 kg and carries L1, L5, and S-band navigation payloads.
        GSLV-F15 is the 17th flight of GSLV and 11th with indigenous cryogenic stage. The NavIC system
        provides positioning services with 10 metre accuracy over India and 1,500 km beyond its borders.""",
        "url": "https://isro.gov.in/nvs02-launch",
        "url_hash": "isro_nvs02_jan2026",
        "source": "ISRO",
        "date": "2026-01-29",
    },
    {
        "headline": "India and France sign 5 MoUs including agreement on semiconductor technology",
        "text": """India and France have signed five Memoranda of Understanding (MoUs) during the bilateral
        summit between Prime Minister Narendra Modi and French President Emmanuel Macron in New Delhi.
        The agreements cover semiconductor technology, defence cooperation, renewable energy, digital
        infrastructure, and cultural exchanges. France has committed to invest Rs 10,000 crore in India's
        semiconductor ecosystem under the India Semiconductor Mission. The two nations also elevated their
        bilateral relationship to a 'Strategic Partnership 2.0'. France is India's 10th largest trading
        partner with bilateral trade worth USD 15 billion in FY25. A joint working group on cybersecurity
        was also established. India-France friendship dates back to 1998 Pokhran tests.""",
        "url": "https://mea.gov.in/india-france-mous",
        "url_hash": "india_france_mous_2026",
        "source": "MEA",
        "date": "2026-02-10",
    },
    {
        "headline": "India wins 22 medals at 2026 Asian Winter Games in Harbin",
        "text": """India achieved its best-ever performance at the Asian Winter Games 2026 held in Harbin,
        China, winning 22 medals including 7 gold, 8 silver, and 7 bronze medals. The Indian contingent
        comprised 45 athletes participating in 8 disciplines. Speed skater Aryan Singh became the hero
        of the Games winning 3 gold medals. India finished 6th overall in the medal tally. China topped
        the medal tally with 87 gold medals. Japan and South Korea finished 2nd and 3rd respectively.
        The 2026 Asian Winter Games were held from February 7-14, 2026. Union Sports Minister Mansukh
        Mandaviya congratulated the Indian athletes on their historic achievement.""",
        "url": "https://sports.gov.in/asian-winter-games-2026",
        "url_hash": "india_awg2026",
        "source": "Sports Ministry",
        "date": "2026-02-14",
    },
    {
        "headline": "India ranked 130th in Human Development Index 2025 Report by UNDP",
        "text": """India has been ranked 130th out of 193 countries in the Human Development Index (HDI) 2025
        Report released by the United Nations Development Programme (UNDP). India's HDI value improved to
        0.685 from 0.677, placing it in the 'Medium Human Development' category. India's life expectancy
        at birth is 67.7 years. Expected years of schooling stands at 11.9 years. Gross National Income
        (GNI) per capita is USD 9,047. Switzerland topped the HDI rankings followed by Norway and Iceland.
        Among BRICS nations, Russia ranked 58th, Brazil 84th, and China 79th. The report was themed
        'The Digital Divide and the Future of Human Development'.""",
        "url": "https://undp.org/hdi-2025",
        "url_hash": "undp_hdi_2025",
        "source": "UNDP",
        "date": "2026-01-15",
    },
    {
        "headline": "PM Modi inaugurates 10 Vande Bharat Express trains connecting Tier-2 cities",
        "text": """Prime Minister Narendra Modi inaugurated 10 new Vande Bharat Express trains simultaneously,
        connecting Tier-2 and Tier-3 cities across India. With this launch, India now has over 130 Vande
        Bharat trains operational. The new routes include Patna-Ranchi, Bhubaneswar-Kolkata, Dehradun-Delhi,
        Trichy-Chennai, and Nagpur-Mumbai among others. These semi-high-speed trains run at 160 kmph with
        features like automatic doors, GPS-based passenger information, and bio-vacuum toilets. The Indian
        Railways aims to run 200 Vande Bharat trains by March 2026. The trains are manufactured at Integral
        Coach Factory (ICF) Chennai and Modern Coach Factory (MCF) Raebareli.""",
        "url": "https://indianrailways.gov.in/vande-bharat-2026",
        "url_hash": "vande_bharat_10_2026",
        "source": "Indian Railways",
        "date": "2026-02-01",
    },
    {
        "headline": "Padma Awards 2026: 139 personalities including 7 Padma Vibhushan recipients",
        "text": """The Government of India announced the prestigious Padma Awards 2026 on the eve of Republic
        Day. A total of 139 personalities have been selected for the honour. The awards include 7 Padma
        Vibhushan, 19 Padma Bhushan, and 113 Padma Shri awards. Renowned classical dancer Yamini
        Krishnamurthy (posthumous) and historian Ramachandra Guha received Padma Vibhushan. Among Padma
        Bhushan recipients are actor Mithun Chakraborty and economist Surjit Bhalla. The Padma awards
        were instituted in 1954 and recognise exceptional and distinguished service to the nation.
        This year 30 women, 10 posthumous, 8 foreigners/NRI/PIO/OCI recipients are included.""",
        "url": "https://mha.gov.in/padma-2026",
        "url_hash": "padma_awards_2026",
        "source": "MHA",
        "date": "2026-01-25",
    },
    {
        "headline": "World Government Summit 2026 held in Dubai; India showcases AI achievements",
        "text": """The World Government Summit 2026 was held between February 3-5, 2026, at Madinat Jumeirah
        in Dubai, UAE. Over 140 countries participated with more than 4,000 delegates attending.
        The theme was 'Shaping Future Governments'. India showcased its achievements in AI through
        the IndiaAI Mission and Digital India programme. Union Minister Ashwini Vaishnaw represented
        India at the summit. Key discussions included AI governance, sustainable development, and
        the future of public services. The Summit is an annual event organized by the UAE government
        that brings together world leaders, ministers, and experts to discuss global governance challenges.
        India's Unified Payment Interface (UPI) was highlighted as a global model for digital payments.""",
        "url": "https://worldgovernmentsummit.org/2026",
        "url_hash": "wgs_dubai_2026",
        "source": "World Government Summit",
        "date": "2026-02-05",
    },
    {
        "headline": "Union Budget 2026-27: Key highlights - Income tax exemption limit raised to Rs 12 lakh",
        "text": """Finance Minister Nirmala Sitharaman presented the Union Budget 2026-27 in Parliament.
        The income tax exemption limit under the new tax regime has been raised to Rs 12 lakh per annum
        from Rs 7 lakh. Capital expenditure allocation increased to Rs 11.21 lakh crore, up 10.7% from
        previous year. Agriculture sector gets Rs 1.52 lakh crore allocation. Education sector receives
        Rs 1.28 lakh crore. The fiscal deficit target set at 4.4% of GDP for FY27. Infrastructure allocation
        includes Rs 2.5 lakh crore for railways. Defence budget increased to Rs 6.21 lakh crore.
        New 'Viksit Bharat' fund of Rs 1 lakh crore announced for manufacturing. GST on insurance
        premiums reduced. PM Kisan Samman Nidhi increased to Rs 9,000 per year.""",
        "url": "https://indiabudget.gov.in/2026-27",
        "url_hash": "budget_2026_27",
        "source": "Ministry of Finance",
        "date": "2026-02-01",
    },
    {
        "headline": "PM Surya Ghar Muft Bijli Yojana crosses 1 crore registrations milestone",
        "text": """The PM Surya Ghar Muft Bijli Yojana, India's ambitious rooftop solar scheme, has crossed
        one crore (10 million) registrations, making it the world's largest rooftop solar programme.
        The scheme was launched in February 2024 and provides free electricity up to 300 units per month
        to households installing solar panels on rooftops. Under the scheme, central financial assistance
        of Rs 30,000 to Rs 78,000 per household is provided. States with highest registrations: Gujarat,
        Maharashtra, and Rajasthan lead. The scheme targets installation of solar panels in 1 crore
        households by March 2027. Cumulative solar capacity through the scheme has reached 3.2 GW.
        The scheme is monitored through the National Portal for Rooftop Solar.""",
        "url": "https://pmsuryaghar.gov.in/milestone",
        "url_hash": "pm_surya_ghar_1cr",
        "source": "Ministry of New & Renewable Energy",
        "date": "2026-02-08",
    },
    {
        "headline": "National Cancer Grid releases guidelines; AIIMS Delhi opens proton therapy centre",
        "text": """The National Cancer Grid (NCG), comprising 300+ cancer centres, has released updated
        treatment guidelines for 15 types of cancer. Simultaneously, AIIMS New Delhi has inaugurated
        India's first government-sector proton therapy centre, making advanced cancer treatment accessible
        at affordable cost. The centre can treat 25-30 patients daily with precise radiation targeting
        tumours while minimizing damage to surrounding tissue. Proton therapy costs Rs 15-20 lakh at AIIMS
        compared to Rs 50-70 lakh at private hospitals. The NCG is managed by Tata Memorial Centre, Mumbai.
        India sees 14 lakh new cancer cases annually according to ICMR data. The National Cancer Mission
        has allocated Rs 5,000 crore for cancer infrastructure.""",
        "url": "https://aiims.edu/proton-therapy-centre",
        "url_hash": "aiims_proton_2026",
        "source": "AIIMS Delhi",
        "date": "2026-02-12",
    },
    {
        "headline": "India's forex reserves touch all-time high of USD 720 billion in January 2026",
        "text": """India's foreign exchange reserves have surged to an all-time high of USD 720.27 billion
        as of January 24, 2026, according to data released by the Reserve Bank of India (RBI). This marks
        an increase of USD 5.93 billion from the previous week. Foreign Currency Assets (FCA), the largest
        component of reserves, stood at USD 641.50 billion. Gold reserves increased to USD 68.47 billion.
        Special Drawing Rights (SDRs) with IMF stood at USD 5.97 billion. India's Reserve Tranche Position
        with IMF was USD 4.33 billion. India is now the 4th largest forex reserves holding country globally,
        after China, Japan, and Switzerland. At current import rate, India's reserves can cover 11.5 months
        of imports.""",
        "url": "https://rbi.org.in/forex-reserves-jan2026",
        "url_hash": "forex_reserves_720bn",
        "source": "RBI",
        "date": "2026-01-31",
    },
    {
        "headline": "India's Constitution Amendment Bill for 33% women reservation in Parliament passed",
        "text": """The Nari Shakti Vandan Adhiniyam implementation process advanced as the Delimitation
        Commission released draft proposals for constituency boundaries. The constitutional amendment,
        passed in September 2023, reserves 33% seats for women in Lok Sabha and State Legislative
        Assemblies. The reservation will come into force after the next census and subsequent delimitation.
        Women currently constitute 14.4% of Lok Sabha members. Among states, Bihar has highest women
        representation. The law also reserves one-third of the reserved seats (SC/ST) for women from
        those communities. 106th Constitutional Amendment Act provides for sunset clause of 15 years.
        Several countries including Rwanda (61%), Sweden (46%) have higher women representation in Parliament.""",
        "url": "https://loksabha.nic.in/women-reservation",
        "url_hash": "nari_shakti_2026",
        "source": "Lok Sabha Secretariat",
        "date": "2026-02-03",
    },
    {
        "headline": "International Day of Women and Girls in Science observed on February 11",
        "text": """The International Day of Women and Girls in Science is observed annually on February 11,
        established by the United Nations General Assembly in December 2015. The day aims to achieve full
        and equal access to and participation in science for women and girls. The 2026 theme is 'Breaking
        Barriers: Women Leading Scientific Discovery'. India observed the day with events organized by
        DST, CSIR, ISRO, and various IITs. Currently, women constitute only 28% of researchers globally.
        UNESCO leads international efforts to bridge the gender gap in STEM. India's percentage of women
        in R&D increased to 18.7% in 2025 from 15% in 2019. Notable Indian women scientists honoured
        include Dr Tessy Thomas, known as the 'Missile Woman of India'.""",
        "url": "https://dst.gov.in/women-science-day-2026",
        "url_hash": "women_science_day_2026",
        "source": "DST",
        "date": "2026-02-11",
    },
    {
        "headline": "49th Kolkata International Book Fair 2026 concludes; record 25 lakh visitors",
        "text": """The 49th Kolkata International Book Fair concluded on February 3, 2026 at Boimela Prangan,
        Salt Lake, Kolkata. The fair recorded a historic 25 lakh (2.5 million) visitors over 12 days.
        Germany was the focal theme country of this year's fair. Over 1,000 publishers from 22 countries
        participated with 700+ stalls. Total sales crossed Rs 25 crore. The fair is organized by
        Publishers and Booksellers Guild, Kolkata. Nobel laureate Abdulrazak Gurnah delivered the
        inaugural address. The Kolkata Book Fair is recognized by Frankfurt Book Fair as a significant
        global literary event. First held in 1976, it is the largest non-trade book fair in the world
        and the largest book fair in Asia by attendance.""",
        "url": "https://kolkatabookfair.net/2026",
        "url_hash": "kolkata_book_fair_2026",
        "source": "Publishers Guild",
        "date": "2026-02-03",
    },
    {
        "headline": "GST Council reduces tax rate on life insurance premiums to 5%",
        "text": """The GST Council, chaired by Finance Minister Nirmala Sitharaman, approved reduction in
        GST rate on life insurance premiums from 18% to 5% in its 56th meeting held in Jaisalmer,
        Rajasthan. Term life insurance policies will attract NIL GST. Health insurance premiums for
        senior citizens above 60 years exempted from GST. The Council also approved uniform GST rate of
        5% on all fortified rice. GST on used EVs set at 18% (was 12%). 31 items saw rate changes.
        The total GST revenue in January 2026 stood at Rs 1.96 lakh crore, 12.6% higher YoY.
        The GST Council was constituted on September 15, 2016 under Article 279A of the Constitution.
        All states and Centre are members with Centre having 1/3 voting power.""",
        "url": "https://gstcouncil.gov.in/56th-meeting",
        "url_hash": "gst_council_56th",
        "source": "GST Council",
        "date": "2026-01-20",
    },
]

def seed_demo_data():
    """Seed demo articles into database."""
    init_db()
    pipeline = Pipeline()
    stored = pipeline.process_batch(DEMO_ARTICLES)
    logger.info(f"Seeded {len(stored)} demo articles")
    return len(stored)


def run_pipeline_once(
    use_rss: bool = True,
    use_api: bool = True,
    max_feeds: int = None
) -> Dict:
    """
    Run one complete pipeline cycle.
    Returns statistics about what was processed/stored.
    """
    from news_fetcher import fetch_recent_news

    logger.info("=" * 60)
    logger.info("Starting CA Pipeline")
    logger.info("=" * 60)

    init_db()
    pipeline = Pipeline()

    # Fetch news
    logger.info("Step 1: Fetching news...")
    articles = fetch_recent_news(
        use_rss=use_rss,
        use_api=use_api,
        max_rss_feeds=max_feeds
    )
    logger.info(f"Fetched {len(articles)} articles")

    # Process
    logger.info("Step 2: Classifying and storing...")
    stored = pipeline.process_batch(articles)

    stats = pipeline.get_stats()
    db_stats = get_stats()

    result = {
        'fetched': len(articles),
        'processed': stats['processed'],
        'stored': stats['stored'],
        'skipped': stats['skipped'],
        'db_total': db_stats['total'],
        'timestamp': datetime.now().isoformat()
    }

    logger.info(f"Pipeline complete: {result}")
    return result


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s'
    )

    import argparse
    parser = argparse.ArgumentParser(description='CA Pipeline')
    parser.add_argument('--seed', action='store_true', help='Seed demo data')
    parser.add_argument('--run', action='store_true', help='Run full pipeline')
    parser.add_argument('--max-feeds', type=int, default=10, help='Max RSS feeds to use')
    args = parser.parse_args()

    if args.seed:
        n = seed_demo_data()
        print(f"Seeded {n} articles")

    elif args.run:
        result = run_pipeline_once(max_feeds=args.max_feeds)
        print(json.dumps(result, indent=2))

    else:
        # Default: just seed
        n = seed_demo_data()
        print(f"Default: seeded {n} demo articles")
