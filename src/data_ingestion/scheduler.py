"""Scheduled ingestion jobs for news and price updates."""

from __future__ import annotations

from apscheduler.schedulers.background import BackgroundScheduler


def build_scheduler(job_func, minutes: int = 60) -> BackgroundScheduler:
    scheduler = BackgroundScheduler()
    scheduler.add_job(job_func, "interval", minutes=minutes, id="ingestion_job", replace_existing=True)
    return scheduler

