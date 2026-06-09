# God Madara Hosting Bot — Setup Guide

## Prerequisites

- Python 3.10+ (3.11 recommended)
- A Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- A server / VPS (Ubuntu 20.04+, Debian, or any Linux)
- (Optional) MongoDB Atlas or local MongoDB instance

---

## Quick Start (VPS / AWS / any Linux)

### 1. System packages

```bash
sudo apt-get update
sudo apt-get install -y python3 python3-pip python3-venv nodejs npm default-jdk maven git curl
```

### 2. Upload & extract the bot

```bash
mkdir ~/madara-bot && cd ~/madara-bot
# Upload GOD-MADARA-BOT-FINAL.zip to this folder, then:
unzip GOD-MADARA-BOT-FINAL.zip
```

### 3. Install Python dependencies

```bash
pip3 install -r requirements.txt
```

### 4. Configure environment variables

Create a `.env` file (or `export` them directly):

```env
# ── Required ──────────────────────────────────────────────────
BOT_TOKEN=123456:ABC-your-bot-token-here
OWNER_ID=123456789          # Your Telegram user ID (get from @userinfobot)
OWNER_USERNAME=your_username

# ── Web file manager (required for file editing in browser) ───
BASE_URL=http://your-server-ip-or-domain:5000
PORT=5000

# ── Database (choose ONE option below) ────────────────────────

# Option A — Local SQLite (no external service needed, recommended for start)
DB_BACKEND=local

# Option B — MongoDB (for multi-server / backup support)
# DB_BACKEND=mongodb
# MONGODB_URI=mongodb+srv://user:pass@cluster.mongodb.net
# DATABASE_NAME=madara_bot

# ── Optional ──────────────────────────────────────────────────
MAX_CONCURRENT_PROJECTS=10   # max projects running at same time (RAM guard)
```

### 5. Load env and start

```bash
export $(cat .env | xargs)
python3 main.py
```

Or use a process manager so it stays alive:

```bash
pip3 install supervisor
# or use screen / tmux:
screen -S madara-bot
python3 main.py
# Ctrl+A then D to detach
```

---

## Running with systemd (auto-start on reboot)

```bash
sudo nano /etc/systemd/system/madara-bot.service
```

Paste:

```ini
[Unit]
Description=God Madara Hosting Bot
After=network.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/madara-bot
EnvironmentFile=/home/ubuntu/madara-bot/.env
ExecStart=/usr/bin/python3 /home/ubuntu/madara-bot/main.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:

```bash
sudo systemctl daemon-reload
sudo systemctl enable madara-bot
sudo systemctl start madara-bot
sudo systemctl status madara-bot
```

---

## Database Options

### Option A — Local SQLite (default, easiest)

Set `DB_BACKEND=local`. The bot creates `data/bot_local.db` automatically.  
No extra setup, no external service, works offline.

**Switching from MongoDB → Local SQLite from Admin Panel:**
Admin Panel → 🗄 DB: MongoDB → Switch → confirm.

### Option B — MongoDB

1. Create a free cluster at [mongodb.com/atlas](https://cloud.mongodb.com)
2. Create a database user (Database Access tab)
3. Add your server IP to Network Access (or use `0.0.0.0/0` for all)
4. Copy the connection string and set `MONGODB_URI=...`
5. Set `DB_BACKEND=mongodb`

---

## Features

### Bot Lock Mode (Admin)
Admins can lock the bot to Premium-only mode from the Admin Panel.
- **🔒 Lock Bot** — Free users get a "locked" message and cannot use the bot
- **🔓 Unlock Bot** — All users can access normally

### Premium Expiry Project Lock
When a user's premium expires:
- All their projects except 1 are automatically stopped and locked
- The user receives a notification with locked project names
- Projects unlock as soon as premium is renewed

### RAM Optimization
- `MAX_CONCURRENT_PROJECTS` (default 10) limits simultaneous running projects
- Log files auto-truncate at 2 MB to prevent disk bloat
- Background process monitor restarts crashed projects (auto-restart mode)

### File Manager
Access your project files via browser: `http://your-ip:5000/files/`

### HTML Hosting
HTML projects get a permanent URL: `http://your-ip:5000/html/USER_ID/PROJECT_NAME/`

---

## Environment Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `BOT_TOKEN` | ✅ | — | Telegram bot token from @BotFather |
| `OWNER_ID` | ✅ | — | Owner's Telegram user ID |
| `OWNER_USERNAME` | ✅ | — | Owner's Telegram username (no @) |
| `BASE_URL` | ✅ | — | Public URL of your server (for file manager & HTML hosting) |
| `PORT` | ✅ | 5000 | Port for Flask file manager |
| `DB_BACKEND` | ❌ | mongodb | `mongodb` or `local` |
| `MONGODB_URI` | ❌ | — | MongoDB connection string (only for mongodb backend) |
| `DATABASE_NAME` | ❌ | madara_bot | MongoDB database name |
| `MAX_CONCURRENT_PROJECTS` | ❌ | 10 | Max simultaneous running projects |

---

## Troubleshooting

**Bot not responding after start**
- Check `BOT_TOKEN` is correct
- Check internet connection (bot needs to reach `api.telegram.org`)

**File manager not accessible**
- Check `PORT` is open in firewall: `sudo ufw allow 5000`
- Check `BASE_URL` matches your server's actual IP/domain

**Maven / Java projects failing**
- Ensure `maven` and `default-jdk` are installed: `apt-get install -y maven default-jdk`

**Out of memory / bot crashing with 5+ projects**
- Lower `MAX_CONCURRENT_PROJECTS` to `5`
- Use a server with at least 1 GB RAM for 5 projects, 2 GB for 10+

**Switching from MongoDB to Local fails**
- Make sure `data/` directory is writable: `chmod 755 data/`

---

## Project Structure

```
madara-bot/
├── main.py          ← Main bot (all logic here)
├── file_manager.py  ← Flask file manager (browser-based editor)
├── requirements.txt ← Python dependencies
├── runtime.txt      ← Python version
├── apt-packages     ← System packages list
├── data/            ← Auto-created: SQLite DB + DB backend setting
│   ├── bot_local.db ← Local SQLite database (auto-created)
│   └── db_backend.txt ← Current DB backend setting
└── projects/        ← Auto-created: one folder per user+project
    └── USER_ID/
        └── PROJECT_NAME/
            ├── (your project files)
            └── output.log
```

---

*God Madara Hosting Bot — supports Python, Node.js, Java, and HTML projects.*
