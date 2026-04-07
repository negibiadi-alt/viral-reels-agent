"""Telegram approval bot.

Sends the daily Top-N pending candidates to the approver chat and exposes
inline ✅ / ❌ callback buttons. Decisions update Candidate.status.
"""
from __future__ import annotations

from loguru import logger
from sqlalchemy import select
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

from src.config import settings
from src.db import SessionLocal
from src.db.models import Candidate, CandidateStatus


def _format_card(c: Candidate) -> str:
    er = c.metrics.get("engagement_rate", 0) if c.metrics else 0
    return (
        f"<b>{c.platform.value.upper()}</b> — @{c.author or 'unknown'}\n"
        f"👁 {c.views:,}  ❤️ {c.likes:,}  💬 {c.comments:,}\n"
        f"ER: {er:.1%}\n"
        f"{(c.caption or '')[:200]}\n"
        f"<a href='{c.source_url}'>Kaynak</a>"
    )


def _keyboard(candidate_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("✅ Onayla", callback_data=f"approve:{candidate_id}"),
                InlineKeyboardButton("❌ Reddet", callback_data=f"reject:{candidate_id}"),
            ]
        ]
    )


async def _cmd_start(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text(
        "Viral Reels onay botu aktif.\n"
        "/pending  — bekleyen adayları listele\n"
        "/run      — taramayı şimdi çalıştır"
    )


async def _cmd_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    session = SessionLocal()
    try:
        rows = (
            session.execute(
                select(Candidate)
                .where(Candidate.status == CandidateStatus.PENDING)
                .order_by(Candidate.discovered_at.desc())
                .limit(settings.daily_candidates)
            )
            .scalars()
            .all()
        )
        if not rows:
            await update.message.reply_text("Bekleyen aday yok.")
            return
        for cand in rows:
            await ctx.bot.send_message(
                chat_id=update.effective_chat.id,
                text=_format_card(cand),
                reply_markup=_keyboard(cand.id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
    finally:
        session.close()


async def _cmd_run(update: Update, _ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from src.discovery.service import run_discovery

    await update.message.reply_text("Tarama başladı…")
    inserted = run_discovery()
    await update.message.reply_text(f"Bitti. {len(inserted)} yeni aday.")
    await _cmd_pending(update, _ctx)


async def _on_callback(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    await query.answer()

    try:
        action, raw_id = query.data.split(":", 1)
        candidate_id = int(raw_id)
    except (ValueError, AttributeError):
        return

    session = SessionLocal()
    try:
        cand = session.get(Candidate, candidate_id)
        if cand is None:
            await query.edit_message_text("Aday bulunamadı.")
            return

        if action == "approve":
            cand.status = CandidateStatus.APPROVED
            suffix = "✅ Onaylandı"
        elif action == "reject":
            cand.status = CandidateStatus.REJECTED
            suffix = "❌ Reddedildi"
        else:
            return

        session.commit()
        await query.edit_message_text(
            text=f"{query.message.text_html or ''}\n\n<b>{suffix}</b>",
            parse_mode=ParseMode.HTML,
        )
    finally:
        session.close()


def build_application() -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")
    app = ApplicationBuilder().token(settings.telegram_bot_token).build()
    app.add_handler(CommandHandler("start", _cmd_start))
    app.add_handler(CommandHandler("pending", _cmd_pending))
    app.add_handler(CommandHandler("run", _cmd_run))
    app.add_handler(CallbackQueryHandler(_on_callback))
    return app


async def push_daily_candidates(app: Application) -> None:
    """Push current pending top-N to the approver chat (used by scheduler)."""
    if not settings.telegram_approver_chat_id:
        logger.warning("TELEGRAM_APPROVER_CHAT_ID not set — skipping push")
        return

    session = SessionLocal()
    try:
        rows = (
            session.execute(
                select(Candidate)
                .where(Candidate.status == CandidateStatus.PENDING)
                .order_by(Candidate.discovered_at.desc())
                .limit(settings.daily_candidates)
            )
            .scalars()
            .all()
        )
        if not rows:
            await app.bot.send_message(
                chat_id=settings.telegram_approver_chat_id,
                text="Bugün bekleyen aday yok.",
            )
            return
        await app.bot.send_message(
            chat_id=settings.telegram_approver_chat_id,
            text=f"📬 Günün {len(rows)} viral adayı:",
        )
        for cand in rows:
            await app.bot.send_message(
                chat_id=settings.telegram_approver_chat_id,
                text=_format_card(cand),
                reply_markup=_keyboard(cand.id),
                parse_mode=ParseMode.HTML,
            )
    finally:
        session.close()
