"""FastAPI + APScheduler entrypoint.

Routes:
  GET  /health
  GET  /candidates?status=pending
  POST /topics          { "name": "fitness tips", "keywords": "fitness,workout" }
  POST /discover/run    — trigger discovery now (returns inserted candidates)
  POST /approval/push   — push current pending top-N to Telegram

The Telegram bot runs in the same process via python-telegram-bot's JobQueue-less
polling loop, started on FastAPI startup.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import asynccontextmanager

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from fastapi import FastAPI, HTTPException
from loguru import logger
from pydantic import BaseModel
from sqlalchemy import select

from src.approval.telegram_bot import build_application, push_daily_candidates
from src import log_buffer
from src.config import settings
from src.db import SessionLocal, init_db
from src.db.models import Candidate, CandidateStatus, ScheduledPost, Topic
from src.scheduler.pipeline import enqueue_approved, run_scheduled_post


class TopicIn(BaseModel):
    name: str
    keywords: str = ""


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    logger.info("DB initialized at {}", settings.database_url)

    telegram_app = None
    if settings.telegram_bot_token:
        telegram_app = build_application()
        await telegram_app.initialize()
        await telegram_app.start()
        asyncio.create_task(telegram_app.updater.start_polling())
        logger.info("Telegram bot polling started")
    else:
        logger.warning("TELEGRAM_BOT_TOKEN not set — bot disabled")

    scheduler = AsyncIOScheduler(timezone=settings.timezone)

    def _run_discovery_job():
        from src.discovery.service import run_discovery
        run_discovery()

    async def _push_job():
        if telegram_app is not None:
            await push_daily_candidates(telegram_app)

    def _enqueue_approved_job():
        posts = enqueue_approved()
        for p in posts:
            scheduler.add_job(
                run_scheduled_post,
                "date",
                run_date=p.run_at,
                args=[p.id],
                id=f"post-{p.id}",
                replace_existing=True,
            )

    # 08:00 TR — discovery
    scheduler.add_job(_run_discovery_job, CronTrigger(hour=8, minute=0))
    # 08:15 TR — push candidates to Telegram
    scheduler.add_job(_push_job, CronTrigger(hour=8, minute=15))
    # Every 15 min — pick up approved, process, queue at peak slots
    scheduler.add_job(_enqueue_approved_job, CronTrigger(minute="*/15"))
    scheduler.start()
    logger.info("Scheduler started (discover @08:00, push @08:15, enqueue */15m)")

    app.state.telegram_app = telegram_app
    app.state.scheduler = scheduler

    try:
        yield
    finally:
        scheduler.shutdown(wait=False)
        if telegram_app is not None:
            await telegram_app.updater.stop()
            await telegram_app.stop()
            await telegram_app.shutdown()


app = FastAPI(title="Viral Reels Agent", lifespan=lifespan)


@app.get("/health")
def health():
    return {"ok": True, "dry_run": settings.dry_run}


@app.get("/candidates")
def list_candidates(status: str | None = None, limit: int = 50):
    session = SessionLocal()
    try:
        stmt = select(Candidate).order_by(Candidate.discovered_at.desc()).limit(limit)
        if status:
            stmt = stmt.where(Candidate.status == CandidateStatus(status))
        rows = session.execute(stmt).scalars().all()
        return [
            {
                "id": c.id,
                "platform": c.platform.value,
                "author": c.author,
                "views": c.views,
                "likes": c.likes,
                "comments": c.comments,
                "source_url": c.source_url,
                "status": c.status.value,
                "discovered_at": c.discovered_at.isoformat(),
            }
            for c in rows
        ]
    finally:
        session.close()


@app.post("/topics", status_code=201)
def create_topic(payload: TopicIn):
    session = SessionLocal()
    try:
        existing = session.execute(select(Topic).where(Topic.name == payload.name)).scalar_one_or_none()
        if existing:
            raise HTTPException(status_code=409, detail="topic exists")
        topic = Topic(name=payload.name, keywords=payload.keywords, active=True)
        session.add(topic)
        session.commit()
        session.refresh(topic)
        return {"id": topic.id, "name": topic.name, "keywords": topic.keywords}
    finally:
        session.close()


@app.post("/discover/run")
def trigger_discovery():
    from src.discovery.service import run_discovery
    inserted = run_discovery()
    return {"inserted": len(inserted), "ids": [c.id for c in inserted]}


@app.post("/approval/push")
async def trigger_push():
    telegram_app = app.state.telegram_app
    if telegram_app is None:
        raise HTTPException(status_code=503, detail="telegram disabled")
    await push_daily_candidates(telegram_app)
    return {"ok": True}


@app.post("/pipeline/enqueue")
def trigger_enqueue():
    posts = enqueue_approved()
    scheduler = app.state.scheduler
    for p in posts:
        scheduler.add_job(
            run_scheduled_post,
            "date",
            run_date=p.run_at,
            args=[p.id],
            id=f"post-{p.id}",
            replace_existing=True,
        )
    return {"scheduled": [{"id": p.id, "run_at": p.run_at.isoformat()} for p in posts]}
