"""Telegram approval bot — PTB v20+ ConversationHandler pattern.

States:
  MAIN                 — ana menü (inline keyboard)
  WAITING_TOPIC_NAME   — konu adı bekleniyor
  WAITING_TOPIC_KEYWORDS — anahtar kelimeler bekleniyor

Komutlar:
  /start  /menu  /run  /pending  /cancel
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
    ConversationHandler,
    MessageHandler,
    filters,
)

from src.config import settings
from src.db import SessionLocal
from src.db.models import Candidate, CandidateStatus, Topic

# --- States ---
MAIN, WAITING_TOPIC_NAME, WAITING_TOPIC_KEYWORDS = range(3)

# --- Keyboards ---

def _main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🔍 Taramayı Başlat", callback_data="run")],
        [InlineKeyboardButton("📋 Adayları Göster", callback_data="pending")],
        [InlineKeyboardButton("➕ Konu Ekle", callback_data="add_topic")],
        [InlineKeyboardButton("📊 Konularım", callback_data="topics")],
        [InlineKeyboardButton("📜 Loglar", callback_data="logs")],
    ])


def _candidate_keyboard(candidate_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("✅ Onayla", callback_data=f"approve:{candidate_id}"),
            InlineKeyboardButton("❌ Reddet", callback_data=f"reject:{candidate_id}"),
        ]
    ])


# --- Helpers ---

def _format_card(c: Candidate) -> str:
    er = c.metrics.get("engagement_rate", 0) if c.metrics else 0
    return (
        f"<b>{c.platform.value.upper()}</b> — @{c.author or 'unknown'}\n"
        f"👁 {c.views:,}  ❤️ {c.likes:,}  💬 {c.comments:,}\n"
        f"ER: {er:.1%}\n"
        f"{(c.caption or '')[:200]}\n"
        f"<a href='{c.source_url}'>Kaynak</a>"
    )


async def _send_pending(chat_id: int, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    session = SessionLocal()
    try:
        rows = (
            session.execute(
                select(Candidate)
                .where(Candidate.status == CandidateStatus.PENDING)
                .order_by(Candidate.discovered_at.desc())
                .limit(settings.daily_candidates)
            )
            .scalars().all()
        )
        if not rows:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text="📭 Bekleyen aday yok.\n\nÖnce tarama başlat!",
                reply_markup=_main_keyboard(),
            )
            return
        await ctx.bot.send_message(chat_id=chat_id, text=f"📬 {len(rows)} aday:")
        for cand in rows:
            await ctx.bot.send_message(
                chat_id=chat_id,
                text=_format_card(cand),
                reply_markup=_candidate_keyboard(cand.id),
                parse_mode=ParseMode.HTML,
                disable_web_page_preview=False,
            )
    finally:
        session.close()


async def _do_discovery(chat_id: int, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    from src.discovery.service import run_discovery
    inserted = run_discovery()
    if inserted:
        await ctx.bot.send_message(chat_id=chat_id, text=f"✅ {len(inserted)} yeni aday bulundu!")
        await _send_pending(chat_id, ctx)
    else:
        await ctx.bot.send_message(
            chat_id=chat_id,
            text="⚠️ Aday bulunamadı. Konu ekleyip tekrar dene.",
            reply_markup=_main_keyboard(),
        )


# --- Entry point ---

async def _start(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text(
        "👋 Viral Reels Agent'a hoş geldin!",
        reply_markup=_main_keyboard(),
    )
    return MAIN


async def _menu(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("📌 Ana Menü:", reply_markup=_main_keyboard())
    return MAIN


# --- MAIN state callbacks ---

async def _cb_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Konu seçim ekranını göster."""
    query = update.callback_query
    await query.answer()
    session = SessionLocal()
    try:
        topics = session.execute(select(Topic).where(Topic.active == True)).scalars().all()
        if not topics:
            await query.edit_message_text(
                "📭 Konu yok. Önce konu ekle!",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("➕ Konu Ekle", callback_data="add_topic")]]),
            )
            return MAIN
        buttons = [[InlineKeyboardButton(f"📌 {t.name}", callback_data=f"run_topic:{t.id}")] for t in topics]
        buttons.append([InlineKeyboardButton("🔍 Tümünü Tara", callback_data="run_all")])
        buttons.append([InlineKeyboardButton("🏠 Ana Menü", callback_data="menu")])
        await query.edit_message_text(
            "🔍 Hangi konuyu tarayalım?",
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    finally:
        session.close()
    return MAIN


async def _cb_run_topic(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Seçilen konuyu tara."""
    query = update.callback_query
    await query.answer()
    topic_id = int(query.data.split(":")[1])
    session = SessionLocal()
    try:
        topic = session.get(Topic, topic_id)
        name = topic.name if topic else "?"
    finally:
        session.close()
    await query.edit_message_text(f"🔍 <b>{name}</b> taranıyor…", parse_mode=ParseMode.HTML)
    from src.discovery.service import discover_for_topic
    from src.db import SessionLocal as SL
    from src.db.models import CandidateStatus
    from datetime import datetime
    session2 = SL()
    inserted = []
    try:
        topic = session2.get(Topic, topic_id)
        if topic:
            from src.discovery.apify_client import RawCandidate
            from src.discovery.service import _exists
            from src.discovery.viral_filter import viral_score
            raw = discover_for_topic(topic)
            raw.sort(key=viral_score, reverse=True)
            for rc in raw:
                if _exists(session2, rc.source_url):
                    continue
                from src.db.models import Candidate, Platform
                cand = Candidate(
                    topic_id=topic.id, platform=rc.platform, source_url=rc.source_url,
                    thumbnail_url=rc.thumbnail_url, caption=rc.caption, author=rc.author,
                    views=rc.views, likes=rc.likes, comments=rc.comments, posted_at=rc.posted_at,
                    metrics={"engagement_rate": (rc.likes + rc.comments) / max(rc.views, 1)},
                    status=CandidateStatus.PENDING,
                )
                session2.add(cand)
                inserted.append(cand)
            topic.last_run_at = datetime.utcnow()
            session2.commit()
    finally:
        session2.close()
    chat_id = update.effective_chat.id
    if inserted:
        await ctx.bot.send_message(chat_id=chat_id, text=f"✅ {len(inserted)} yeni aday bulundu!")
        await _send_pending(chat_id, ctx)
    else:
        await ctx.bot.send_message(chat_id=chat_id, text="⚠️ Aday bulunamadı.", reply_markup=_main_keyboard())
    return MAIN


async def _cb_run_all(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    """Tüm konuları tara."""
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("🔍 Tüm konular taranıyor, lütfen bekle…")
    await _do_discovery(update.effective_chat.id, ctx)
    return MAIN


async def _cb_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text("📋 Adaylar yükleniyor…")
    await _send_pending(update.effective_chat.id, ctx)
    return MAIN


async def _cb_topics(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    session = SessionLocal()
    try:
        rows = session.execute(select(Topic).where(Topic.active == True)).scalars().all()
        if not rows:
            await query.edit_message_text(
                "📭 Henüz konu eklenmemiş.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Konu Ekle", callback_data="add_topic")],
                    [InlineKeyboardButton("🏠 Ana Menü", callback_data="menu")],
                ]),
            )
            return MAIN
        buttons = []
        for t in rows:
            buttons.append([
                InlineKeyboardButton(f"📌 {t.name}", callback_data=f"run_topic:{t.id}"),
                InlineKeyboardButton("🗑 Sil", callback_data=f"del_topic:{t.id}"),
            ])
        buttons.append([InlineKeyboardButton("➕ Yeni Konu Ekle", callback_data="add_topic")])
        buttons.append([InlineKeyboardButton("🏠 Ana Menü", callback_data="menu")])
        await query.edit_message_text(
            "📊 <b>Konularım</b>\n\nTaramak için konuya bas, silmek için 🗑:",
            parse_mode=ParseMode.HTML,
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    finally:
        session.close()
    return MAIN


async def _cb_del_topic(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    topic_id = int(query.data.split(":")[1])
    session = SessionLocal()
    try:
        topic = session.get(Topic, topic_id)
        if topic:
            topic.active = False
            session.commit()
            name = topic.name
        else:
            name = "?"
    finally:
        session.close()
    await query.answer(f"🗑 {name} silindi", show_alert=False)
    return await _cb_topics(update, ctx)


async def _cb_logs(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    from src.log_buffer import get_recent
    query = update.callback_query
    await query.answer()
    logs = get_recent(20)
    await query.edit_message_text(
        f"📜 <b>Son Loglar:</b>\n\n<code>{logs}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🏠 Ana Menü", callback_data="menu")]]),
    )
    return MAIN


async def _cb_add_topic(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    query = update.callback_query
    await query.answer()
    await query.edit_message_text(
        "➕ Eklemek istediğin konunun adını yaz:\n\nÖrnek: <code>fitness</code>",
        parse_mode=ParseMode.HTML,
    )
    return WAITING_TOPIC_NAME


# --- Topic ekleme akışı ---

async def _recv_topic_name(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    name = update.message.text.strip()
    ctx.user_data["topic_name"] = name
    await update.message.reply_text(
        f"✏️ <b>{name}</b> için anahtar kelimeler?\n\n"
        f"Virgülle yaz: <code>fitness,workout,gym</code>\n"
        f"Atlamak için: <code>-</code>",
        parse_mode=ParseMode.HTML,
    )
    return WAITING_TOPIC_KEYWORDS


async def _recv_topic_keywords(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    name = ctx.user_data.pop("topic_name", "")
    keywords = "" if update.message.text.strip() == "-" else update.message.text.strip()

    session = SessionLocal()
    try:
        existing = session.execute(select(Topic).where(Topic.name == name)).scalar_one_or_none()
        if existing:
            await update.message.reply_text(
                f"⚠️ <b>{name}</b> zaten var!",
                parse_mode=ParseMode.HTML,
                reply_markup=_main_keyboard(),
            )
            return MAIN
        session.add(Topic(name=name, keywords=keywords, active=True))
        session.commit()
    finally:
        session.close()

    await update.message.reply_text(
        f"✅ <b>{name}</b> eklendi! Tarama başlatmak ister misin?",
        parse_mode=ParseMode.HTML,
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔍 Taramayı Başlat", callback_data="run")],
            [InlineKeyboardButton("🏠 Ana Menü", callback_data="topics")],
        ]),
    )
    return MAIN


async def _cancel(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> int:
    await update.message.reply_text("İptal edildi.", reply_markup=_main_keyboard())
    return MAIN


# --- Onay / Red (ConversationHandler dışında) ---

async def _on_vote(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
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
        # Butonları kilitle, sadece sonucu göster
        await query.edit_message_text(
            text=f"{query.message.text_html or ''}\n\n<b>{suffix}</b>",
            parse_mode=ParseMode.HTML,
            reply_markup=None,
        )
    finally:
        session.close()


# --- Komutlar (ConversationHandler dışında hızlı erişim) ---

async def _cmd_run(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await update.message.reply_text("🔍 Tarama başladı…")
    await _do_discovery(update.effective_chat.id, ctx)


async def _cmd_pending(update: Update, ctx: ContextTypes.DEFAULT_TYPE) -> None:
    await _send_pending(update.effective_chat.id, ctx)


# --- Build ---

def build_application() -> Application:
    if not settings.telegram_bot_token:
        raise RuntimeError("TELEGRAM_BOT_TOKEN is not set")

    app = ApplicationBuilder().token(settings.telegram_bot_token).build()

    conv = ConversationHandler(
        entry_points=[
            CommandHandler("start", _start),
            CommandHandler("menu", _menu),
        ],
        states={
            MAIN: [
                CallbackQueryHandler(_cb_run, pattern="^run$"),
                CallbackQueryHandler(_cb_run_all, pattern="^run_all$"),
                CallbackQueryHandler(_cb_run_topic, pattern="^run_topic:"),
                CallbackQueryHandler(_cb_pending, pattern="^pending$"),
                CallbackQueryHandler(_cb_add_topic, pattern="^add_topic$"),
                CallbackQueryHandler(_cb_topics, pattern="^topics$"),
                CallbackQueryHandler(_cb_del_topic, pattern="^del_topic:"),
                CallbackQueryHandler(_cb_logs, pattern="^logs$"),
                CallbackQueryHandler(_start, pattern="^menu$"),
            ],
            WAITING_TOPIC_NAME: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _recv_topic_name),
            ],
            WAITING_TOPIC_KEYWORDS: [
                MessageHandler(filters.TEXT & ~filters.COMMAND, _recv_topic_keywords),
            ],
        },
        fallbacks=[CommandHandler("cancel", _cancel)],
        per_message=False,
    )

    app.add_handler(conv)
    app.add_handler(CommandHandler("run", _cmd_run))
    app.add_handler(CommandHandler("pending", _cmd_pending))
    app.add_handler(CallbackQueryHandler(_on_vote, pattern="^(approve|reject):"))

    return app


async def push_daily_candidates(app: Application) -> None:
    """Scheduler tarafından çağrılır — günlük adayları gönderir."""
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
            .scalars().all()
        )
        if not rows:
            await app.bot.send_message(
                chat_id=settings.telegram_approver_chat_id,
                text="📭 Bugün bekleyen aday yok.",
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
                reply_markup=_candidate_keyboard(cand.id),
                parse_mode=ParseMode.HTML,
            )
    finally:
        session.close()
