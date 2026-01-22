# Deployment Guide: boda8-src (Python Bot) to VPS

This guide covers deploying the `boda8-src` Python/Flask bot to a VPS using **Docker Compose**.

## Prerequisites

- A VPS (Ubuntu/Debian recommended).
- Docker & Docker Compose installed.
- A valid Telegram Bot Token.

## Database Migration (IMPORTANT)

**Data tidak akan berpindah sendiri.** Anda perlu pindahkan data dari Cloud SQL ke VPS supaya user dan settings tidak hilang.

### 1. Export Data (Dari PC/Laptop)

Gunakan `cloud-sql-proxy` yang ada dalam folder ini untuk connect ke database lama.

1. Buka terminal di folder `boda8-src`.
2. Start proxy:

    ```powershell
    .\cloud-sql-proxy.exe -instances=boda8-6879:asia-southeast1:boda8-pg=tcp:5433
    ```

    *(Biarkan terminal ini terbuka)*

3. Buka terminal **baru** dan dump data:

    ```powershell
    # Password: Boda8Pass12345 (Ada dalam deploy.bat)
    pg_dump -h 127.0.0.1 -p 5433 -U boda8user -d boda8 --no-owner --no-privileges -f backup.sql
    ```

### 2. Import Data (Di VPS)

Selepas `docker compose up -d` di VPS:

1. Upload `backup.sql` ke VPS (dalam folder `boda8-src`).
2. Copy fail ke dalam container dan restore:

    ```bash
    # Copy masuk container
    docker cp backup.sql boda8-db:/backup.sql

    # Login DB & Restore
    docker exec -i boda8-db psql -U boda8 -d boda8db -f /backup.sql
    ```

### 3. Verify Data (Wajib Buat)

Pastikan data ada sebelum tukar webhook!

```bash
docker exec -it boda8-db psql -U boda8 -d boda8db -c "SELECT count(*) FROM users;"
# Patut keluar nombor user yang sama macam di Cloud.
```

---

## Configuration Steps

### 1. Transfer Files

Copy the `boda8-src` folder to your VPS.

```bash
# Example using scp (run from your PC)
scp -r boda8-src root@your-vps-ip:/root/
```

### 2. Configure Environment

On your VPS, inside the `boda8-src` folder, create a `.env` file:

```bash
cd boda8-src
nano .env
```

Paste the following (fill in your secrets):

```ini
# Telegram Token (REQUIRED)
TOKEN=123456:ABC-DEF...

# Domain (Optional, but good for webhook generation)
PUBLIC_BASE_URL=https://bot.yourdomain.com

# Database (Already configured in docker-compose, but if you change it, update here)
# DATABASE_URL=postgresql://boda8:boda8secret@db:5432/boda8db

# Other settings
LOG_LEVEL=INFO
TZ=Asia/Kuala_Lumpur
```

### 3. Deploy

Run the containers:

```bash
docker compose up -d --build
```

### 4. Database Initialization

The bot code (`main.py`) contains an `init_db()` function that runs on startup (`if __name__ == "__main__":` or top level).
Check the logs to confirm the database was initialized:

```bash
docker logs boda8-bot
```

Look for: `✅ DB Init OK`

### 5. Webhook Setup

For the bot to receive messages, you need to set the webhook.

**Option A: Manual cURL**

```bash
curl "https://api.telegram.org/bot<YOUR_TOKEN>/setWebhook?url=https://bot.yourdomain.com/telegram/webhook"
```

**Option B: Reverse Proxy (SSL)**
Telegram requires HTTPS. You need to use Nginx or Caddy to proxy port `8080`.

*Example Caddyfile:*

```text
bot.yourdomain.com {
    reverse_proxy localhost:8080
}
```

## Troubleshooting

- **Cloud Tasks Error**: If you see errors related to Google Cloud Tasks, ignore them if you are not using background tasks, or disable the feature in code.
- **Database Connection**: Ensure the `db` container is healthy. `docker compose ps`.
