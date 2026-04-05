"""
Scheduler Module
Runs the news pipeline every 30 minutes using APScheduler or threading.Timer fallback.
"""

import os
import sys
import logging
import threading
import time
from datetime import datetime

logger = logging.getLogger(__name__)

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

FETCH_INTERVAL_MINUTES = int(os.environ.get("FETCH_INTERVAL_MINUTES", "30"))


def run_pipeline_job():
    """Job that runs one full fetch+classify+store cycle."""
    logger.info(f"[Scheduler] Running pipeline at {datetime.now().isoformat()}")
    try:
        from pipeline import run_pipeline_once
        result = run_pipeline_once(
            use_rss=True,
            use_api=bool(os.environ.get("NEWSDATA_API_KEY")),
            max_feeds=None  # Run all feeds
        )
        logger.info(f"[Scheduler] Done: {result}")
    except Exception as e:
        logger.error(f"[Scheduler] Pipeline error: {e}", exc_info=True)


class SimpleScheduler:
    """
    Threading-based scheduler as fallback when APScheduler is not available.
    Runs job every `interval_minutes` minutes in a daemon thread.
    """

    def __init__(self, interval_minutes: int = 30):
        self.interval = interval_minutes * 60
        self._timer = None
        self._running = False

    def _run(self):
        if not self._running:
            return
        run_pipeline_job()
        self._schedule_next()

    def _schedule_next(self):
        if self._running:
            self._timer = threading.Timer(self.interval, self._run)
            self._timer.daemon = True
            self._timer.start()

    def start(self):
        self._running = True
        # Run immediately on first start after a short delay (5 min)
        self._timer = threading.Timer(300, self._run)
        self._timer.daemon = True
        self._timer.start()
        logger.info(f"Scheduler started: pipeline every {self.interval//60} min")

    def stop(self):
        self._running = False
        if self._timer:
            self._timer.cancel()
        logger.info("Scheduler stopped.")


def get_scheduler(interval_minutes: int = None):
    """Return best available scheduler instance."""
    if interval_minutes is None:
        interval_minutes = FETCH_INTERVAL_MINUTES

    try:
        from apscheduler.schedulers.background import BackgroundScheduler
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            run_pipeline_job,
            'interval',
            minutes=interval_minutes,
            id='news_pipeline',
            next_run_time=None,  # Don't run immediately on start
            misfire_grace_time=300
        )
        logger.info(f"APScheduler ready: pipeline every {interval_minutes} min")
        return scheduler
    except ImportError:
        logger.info("APScheduler not available; using SimpleScheduler")
        return SimpleScheduler(interval_minutes)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    print(f"Testing scheduler — running pipeline once now...")
    run_pipeline_job()
