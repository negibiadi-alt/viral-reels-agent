"""Application settings loaded from environment."""
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Apify
    apify_token: str = ""

    # Telegram
    telegram_bot_token: str = ""
    telegram_approver_chat_id: int = 0

    # Instagram Graph API (opsiyonel)
    ig_user_id: str = ""
    ig_access_token: str = ""

    # Instagram instagrapi (alternatif)
    ig_username: str = ""
    ig_password: str = ""

    # YouTube
    yt_client_secrets_file: Path = Path("./secrets/yt_client_secret.json")
    yt_token_file: Path = Path("./secrets/yt_token.json")

    # DB
    database_url: str = "sqlite:///./storage/app.db"

    # Runtime
    timezone: str = "Europe/Istanbul"
    daily_candidates: int = 5
    max_candidate_age_days: int = 7
    dry_run: bool = True

    # Viral thresholds
    min_views: int = 1_000
    min_engagement_rate: float = 0.01
    min_views_per_hour: int = 10

    # Storage
    download_dir: Path = Path("./storage/downloaded")
    processed_dir: Path = Path("./storage/processed")

    # YouTube Data API v3 (sadece API key, OAuth yok)
    youtube_api_key: str = ""

    # TikTok — Omkar Cloud (ücretsiz 5K/ay)
    omkar_api_key: str = ""

    # Claude
    anthropic_api_key: str = ""


settings = Settings()
