"""YouTube OAuth bootstrap scripti — sadece laptop'ta bir kere çalıştırılır.

Kullanım:
    pip install google-auth-oauthlib
    python scripts/bootstrap_yt_auth.py

Tarayıcı açılır, Google hesabınla giriş yaparsın.
Token JSON terminale basılır → Render'da YT_TOKEN_JSON env var'ına yapıştır.
"""
import json
import sys
from pathlib import Path

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("Eksik paket: pip install google-auth-oauthlib")
    sys.exit(1)

SCOPES = ["https://www.googleapis.com/auth/youtube.upload"]

# client_secret.json dosyasını bul
candidates = [
    Path("secrets/yt_client_secret.json"),
    Path("yt_client_secret.json"),
    Path("client_secret.json"),
]
secret_file = next((p for p in candidates if p.exists()), None)

if not secret_file:
    print("HATA: client_secret.json bulunamadı.")
    print("Google Cloud Console'dan indirip secrets/ klasörüne koy.")
    sys.exit(1)

print(f"Client secret: {secret_file}")
print("Tarayıcı açılıyor...")

flow = InstalledAppFlow.from_client_secrets_file(str(secret_file), SCOPES)
creds = flow.run_local_server(port=0)

token_json = creds.to_json()

# Dosyaya kaydet (lokal test için)
out = Path("secrets/yt_token.json")
out.parent.mkdir(exist_ok=True)
out.write_text(token_json)
print(f"\nToken dosyaya kaydedildi: {out}")

print("\n" + "="*60)
print("Render'a eklemek için YT_TOKEN_JSON env var değeri:")
print("="*60)
print(token_json)
print("="*60)
print("\nBu JSON'u kopyala → Render Environment → YT_TOKEN_JSON")
