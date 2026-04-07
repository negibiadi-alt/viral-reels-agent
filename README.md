# Viral Reels Agent

YouTube Shorts & Instagram Reels için viral içerik keşif, Telegram onay ve otomatik paylaşım sistemi.

Mimari ve plan için `C:\Users\Lenovo\.claude\plans\sunny-booping-russell.md` dosyasına bak.

## Hızlı başlangıç (lokal)

```bash
cp .env.example .env
# .env içindeki token'ları doldur
pip install -e .[dev]
uvicorn src.main:app --reload
```

## Docker

```bash
docker compose up --build
```

## Akış

1. **Discovery** — her gün 08:00 TR, aktif konularda Apify ile tarar → DB'ye `pending` aday yazar
2. **Approval** — günlük 5 aday Telegram'da kart olarak sunulur (✅/❌)
3. **Processing** — onaylananlar yt-dlp ile indirilip ffmpeg ile modifiye edilir (duplicate bypass)
4. **Publishing** — IG Graph API + YouTube Data API ile peak saatlere planlanır
