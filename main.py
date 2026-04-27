from fastapi import FastAPI, UploadFile, File, BackgroundTasks, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import json
import base64
import calendar
import uuid
import re
import asyncio
import datetime
import hmac
import hashlib
from urllib.parse import urljoin, urlparse
from typing import Optional
from zoneinfo import ZoneInfo

app = FastAPI()

# Only allow requests from your own frontend domains
ALLOWED_ORIGINS = [
    "https://chumartai.com",
    "https://www.chumartai.com",
    "https://ai-assistant-front-iota.vercel.app",  # Vercel preview
    os.getenv("FRONTEND_URL", ""),  # Optional extra origin via env var
]
ALLOWED_ORIGINS = [o for o in ALLOWED_ORIGINS if o]  # Remove empty strings

app.add_middleware(CORSMiddleware,
    allow_origins=ALLOWED_ORIGINS,
    allow_methods=["GET", "POST", "DELETE", "OPTIONS"],
    allow_headers=["Content-Type", "Authorization", "X-Session-Token"],
    allow_credentials=False,
)

ODOO_URL      = os.getenv("ODOO_URL", "")
ODOO_DB       = os.getenv("ODOO_DB", "")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")
DATABASE_URL  = os.getenv("DATABASE_URL", "")

# Cloudflare R2
R2_ACCOUNT_ID = os.getenv("R2_ACCOUNT_ID", "")
R2_ACCESS_KEY = os.getenv("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.getenv("R2_SECRET_KEY", "")
R2_BUCKET     = os.getenv("R2_BUCKET", "chumart-docs")
R2_PUBLIC_URL = os.getenv("R2_PUBLIC_URL", "")

# MinerU Cloud API (PDF OCR)
MINERU_API_TOKEN = os.getenv("MINERU_API_TOKEN", "")

# Reminder notifications
SENDGRID_API_KEY   = os.getenv("SENDGRID_API_KEY", "")
SENDGRID_FROM      = os.getenv("SENDGRID_FROM", "ai@chumartai.com")
SENDGRID_FROM_NAME = os.getenv("SENDGRID_FROM_NAME", "Chumart AI")
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "")
TWILIO_AUTH_TOKEN   = os.getenv("TWILIO_AUTH_TOKEN", "")
TWILIO_FROM_NUMBER  = os.getenv("TWILIO_FROM_NUMBER", "")

# Payment Channels — Stripe / Square / Zelle
STRIPE_SECRET_KEY       = os.getenv("STRIPE_SECRET_KEY", "")
STRIPE_WEBHOOK_SECRET   = os.getenv("STRIPE_WEBHOOK_SECRET", "")
SQUARE_ACCESS_TOKEN           = os.getenv("SQUARE_ACCESS_TOKEN", "")
SQUARE_WEBHOOK_SIGNATURE_KEY  = os.getenv("SQUARE_WEBHOOK_SIGNATURE_KEY", "")
SQUARE_ENVIRONMENT            = os.getenv("SQUARE_ENVIRONMENT", "production")
GMAIL_CREDENTIALS_JSON  = os.getenv("GMAIL_CREDENTIALS_JSON", "")
GMAIL_USER_EMAIL        = os.getenv("GMAIL_USER_EMAIL", "")
ZELLE_BANK_SENDER       = os.getenv("ZELLE_BANK_SENDER", "")

LA_TZ = ZoneInfo("America/Los_Angeles")
UTC_TZ = datetime.timezone.utc

VALID_STATES = ["paid", "in_payment", "reversed"]
CA_STATE_ID  = 13

# In-memory file cache with timestamps for TTL cleanup
FILE_CACHE: dict = {}  # file_id -> {b64, media_type, name, created_at}

# Server-side session store: token -> {uid, role, username, created_at}
# This prevents clients from forging their own role
SESSION_STORE: dict = {}
SESSION_TTL_HOURS = 12  # Sessions expire after 12 hours (mobile clients can request 30 days)
FILE_CACHE_TTL_HOURS = 2  # Uploaded files expire after 2 hours

def cleanup_caches():
    """Remove expired sessions and file cache entries."""
    now = datetime.datetime.now()
    expired_sessions = [t for t, s in SESSION_STORE.items()
                        if (now - s["created_at"]).total_seconds() > SESSION_TTL_HOURS * 3600]
    for t in expired_sessions:
        del SESSION_STORE[t]
    expired_files = [f for f, v in FILE_CACHE.items()
                     if (now - v.get("created_at", now)).total_seconds() > FILE_CACHE_TTL_HOURS * 3600]
    for f in expired_files:
        del FILE_CACHE[f]
    if expired_sessions or expired_files:
        print(f"CACHE CLEANUP: removed {len(expired_sessions)} sessions, {len(expired_files)} files")


async def db_save_session(token: str, uid: int, username: str, role: str):
    """Persist a session to DB so it survives server restarts."""
    conn = await get_db_conn()
    if not conn:
        return
    try:
        expires_at = datetime.datetime.now(UTC_TZ) + datetime.timedelta(hours=SESSION_TTL_HOURS)
        await conn.execute("""
            INSERT INTO user_sessions (token, uid, username, role, expires_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (token) DO UPDATE SET
                uid = EXCLUDED.uid, username = EXCLUDED.username,
                role = EXCLUDED.role, expires_at = EXCLUDED.expires_at
        """, token, uid, username, role, expires_at)
    except Exception as e:
        print(f"db_save_session error: {e}")
    finally:
        await conn.close()


async def db_load_session(token: str) -> dict | None:
    """Load a session from DB. Returns None if expired or missing."""
    if not token:
        return None
    conn = await get_db_conn()
    if not conn:
        return None
    try:
        row = await conn.fetchrow("""
            SELECT uid, username, role, expires_at FROM user_sessions
            WHERE token = $1 AND expires_at > NOW()
        """, token)
        if not row:
            return None
        return {
            "uid": row["uid"],
            "username": row["username"],
            "role": row["role"],
            "created_at": datetime.datetime.now(),  # for compat with in-memory format
        }
    except Exception as e:
        print(f"db_load_session error: {e}")
        return None
    finally:
        await conn.close()


async def db_delete_session(token: str):
    """Delete a session from DB (logout)."""
    conn = await get_db_conn()
    if not conn:
        return
    try:
        await conn.execute("DELETE FROM user_sessions WHERE token = $1", token)
    except Exception as e:
        print(f"db_delete_session error: {e}")
    finally:
        await conn.close()

# Target websites for knowledge base
TARGET_SITES = [
    {"url": "https://www.chumartusa.com",    "name": "Chumart USA",     "category": "own"},
    {"url": "https://www.polarmanusa.com",   "name": "Polarman USA",    "category": "own"},
    {"url": "https://www.flamasterusa.com",  "name": "Flamaster USA",   "category": "own"},
    {"url": "https://www.chefasstusa.com",   "name": "ChefAsst USA",    "category": "own"},
]

# ─────────────────────────────────────────────
# Database setup
# ─────────────────────────────────────────────

async def get_db_conn():
    try:
        import asyncpg
        conn = await asyncpg.connect(DATABASE_URL)
        return conn
    except Exception as e:
        print(f"DB connect error: {e}")
        return None

async def init_db():
    """Create tables and enable pgvector on startup."""
    conn = await get_db_conn()
    if not conn:
        print("WARNING: Cannot connect to DB, knowledge base disabled")
        return
    try:
        await conn.execute("CREATE EXTENSION IF NOT EXISTS vector")
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS knowledge_chunks (
                id          SERIAL PRIMARY KEY,
                site_name   TEXT,
                site_url    TEXT,
                page_url    TEXT,
                page_title  TEXT,
                chunk_text  TEXT,
                embedding   vector(1536),
                category    TEXT DEFAULT 'own',
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        # Drop old IVFFlat index if exists, replace with HNSW
        # IVFFlat requires REINDEX after new inserts; HNSW updates automatically
        await conn.execute("DROP INDEX IF EXISTS knowledge_embedding_idx")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS knowledge_embedding_hnsw_idx
            ON knowledge_chunks
            USING hnsw (embedding vector_cosine_ops)
            WITH (m = 16, ef_construction = 64)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS crawl_log (
                id         SERIAL PRIMARY KEY,
                site_url   TEXT,
                status     TEXT,
                pages      INTEGER DEFAULT 0,
                chunks     INTEGER DEFAULT 0,
                started_at TIMESTAMP DEFAULT NOW(),
                finished_at TIMESTAMP
            )
        """)
        # Chat history
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS chat_sessions (
                id         TEXT PRIMARY KEY,
                uid        INTEGER NOT NULL,
                username   TEXT,
                title      TEXT,
                messages   JSONB DEFAULT '[]',
                created_at TIMESTAMP DEFAULT NOW(),
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS chat_sessions_uid_idx
            ON chat_sessions(uid, updated_at DESC)
        """)
        # User memory
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_memory (
                uid        INTEGER PRIMARY KEY,
                username   TEXT,
                memories   JSONB DEFAULT '[]',
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        # Document library
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS documents (
                id          TEXT PRIMARY KEY,
                filename    TEXT NOT NULL,
                original_name TEXT NOT NULL,
                category    TEXT DEFAULT 'general',
                description TEXT DEFAULT '',
                file_size   INTEGER DEFAULT 0,
                mime_type   TEXT DEFAULT '',
                r2_key      TEXT NOT NULL,
                public_url  TEXT NOT NULL,
                uploaded_by TEXT DEFAULT '',
                chunk_count INTEGER DEFAULT 0,
                created_at  TIMESTAMP DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS knowledge_chunks_doc_idx
            ON knowledge_chunks(site_url)
            WHERE site_url LIKE 'doc:%'
        """)
        # Odoo write audit log — records EVERY create/update the AI makes to Odoo,
        # so you can verify exactly what was changed and by whom.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS odoo_write_audit (
                id           SERIAL PRIMARY KEY,
                ts           TIMESTAMP DEFAULT NOW(),
                who_uid      INTEGER,
                who_name     TEXT,
                tool_name    TEXT,
                model        TEXT NOT NULL,
                record_id    INTEGER,
                operation    TEXT NOT NULL,
                old_values   JSONB,
                new_values   JSONB,
                extra_info   JSONB,
                status       TEXT DEFAULT 'success'
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS odoo_write_audit_ts_idx
            ON odoo_write_audit(ts DESC)
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS odoo_write_audit_model_idx
            ON odoo_write_audit(model, record_id)
        """)
        # Reminders & Events (personal assistant)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS reminders (
                id           SERIAL PRIMARY KEY,
                uid          INTEGER NOT NULL,
                user_name    TEXT,
                content      TEXT NOT NULL,
                fire_at      TIMESTAMPTZ NOT NULL,
                channels     TEXT[] DEFAULT ARRAY['email']::TEXT[],
                target_email TEXT,
                target_phone TEXT,
                fired        BOOLEAN DEFAULT FALSE,
                fired_at     TIMESTAMPTZ,
                error        TEXT,
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS reminders_pending_idx
            ON reminders(fire_at) WHERE fired = FALSE
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS reminders_uid_idx
            ON reminders(uid, fire_at DESC)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS events (
                id          SERIAL PRIMARY KEY,
                uid         INTEGER NOT NULL,
                user_name   TEXT,
                title       TEXT NOT NULL,
                notes       TEXT DEFAULT '',
                location    TEXT DEFAULT '',
                start_at    TIMESTAMPTZ NOT NULL,
                end_at      TIMESTAMPTZ,
                created_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS events_uid_start_idx
            ON events(uid, start_at)
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_contacts (
                uid        INTEGER PRIMARY KEY,
                email      TEXT,
                phone      TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        # Persistent user sessions (survives server restarts) — for 30-day login
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS user_sessions (
                token       TEXT PRIMARY KEY,
                uid         INTEGER NOT NULL,
                username    TEXT,
                name        TEXT,
                role        TEXT,
                client_type TEXT DEFAULT 'web',
                created_at  TIMESTAMPTZ DEFAULT NOW(),
                expires_at  TIMESTAMPTZ NOT NULL
            )
        """)
        # Add missing columns if table existed from older version (idempotent)
        await conn.execute("ALTER TABLE user_sessions ADD COLUMN IF NOT EXISTS name TEXT")
        await conn.execute("ALTER TABLE user_sessions ADD COLUMN IF NOT EXISTS client_type TEXT DEFAULT 'web'")
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS user_sessions_uid_idx
            ON user_sessions(uid, expires_at DESC)
        """)
        # Pending payments — accumulates partial payments from different channels
        # until total >= SO amount, then triggers auto-invoice
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS pending_payments (
                id           SERIAL PRIMARY KEY,
                so_name      TEXT NOT NULL,
                so_id        INTEGER,
                so_amount    NUMERIC(12,2) NOT NULL,
                channel      TEXT NOT NULL,
                amount       NUMERIC(12,2) NOT NULL,
                reference    TEXT DEFAULT '',
                status       TEXT DEFAULT 'pending',
                created_at   TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS pending_payments_so_idx
            ON pending_payments(so_name) WHERE status = 'pending'
        """)
        # Received payments — webhook captures all incoming payments here.
        # User must explicitly "release" SO via AI to trigger invoice creation.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS received_payments (
                id            SERIAL PRIMARY KEY,
                so_name       TEXT NOT NULL,
                channel       TEXT NOT NULL,
                amount        NUMERIC(12,2) NOT NULL,
                external_ref  TEXT DEFAULT '',
                customer_name TEXT DEFAULT '',
                status        TEXT DEFAULT 'received',
                released_at   TIMESTAMPTZ,
                invoice_name  TEXT DEFAULT '',
                created_at    TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS received_payments_so_idx
            ON received_payments(so_name, status, created_at DESC)
        """)
        await conn.execute("""
            CREATE UNIQUE INDEX IF NOT EXISTS received_payments_external_ref_uniq
            ON received_payments(channel, external_ref)
            WHERE external_ref != ''
        """)
        print("DB initialized OK")
    except Exception as e:
        print(f"DB init error: {e}")
    finally:
        await conn.close()

async def audit_odoo_write(
    who_uid: int = 0,
    who_name: str = "",
    tool_name: str = "",
    model: str = "",
    record_id: Optional[int] = None,
    operation: str = "",
    old_values: Optional[dict] = None,
    new_values: Optional[dict] = None,
    extra_info: Optional[dict] = None,
    status: str = "success",
):
    """Log every AI-initiated write to Odoo. Non-blocking, never raises."""
    try:
        conn = await get_db_conn()
        if not conn:
            # Still print to Railway logs as fallback
            print(f"[AUDIT FALLBACK] who={who_name}({who_uid}) tool={tool_name} "
                  f"{operation} {model}#{record_id} old={old_values} new={new_values} "
                  f"status={status}")
            return
        try:
            await conn.execute(
                """
                INSERT INTO odoo_write_audit
                    (who_uid, who_name, tool_name, model, record_id,
                     operation, old_values, new_values, extra_info, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb, $8::jsonb, $9::jsonb, $10)
                """,
                who_uid or 0,
                who_name or "",
                tool_name or "",
                model or "",
                record_id,
                operation or "",
                json.dumps(old_values) if old_values is not None else None,
                json.dumps(new_values) if new_values is not None else None,
                json.dumps(extra_info) if extra_info is not None else None,
                status or "success",
            )
            # Also mirror to Railway logs for live monitoring
            print(f"[AUDIT] who={who_name}({who_uid}) tool={tool_name} "
                  f"{operation} {model}#{record_id} "
                  f"old={old_values} new={new_values} status={status}")
        finally:
            await conn.close()
    except Exception as e:
        # Never let audit failure break the actual business operation
        print(f"[AUDIT ERROR] failed to write audit: {e} "
              f"(operation={operation} model={model} rec={record_id})")

@app.on_event("startup")
async def startup():
    print("=" * 60)
    print("CHUMART AI BACKEND — BUILD: payment-channels-v1 (2026-04-26)")
    print("=" * 60)
    await init_db()
    # Start reminder scanner (checks every 60 seconds for due reminders)
    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        scheduler = AsyncIOScheduler()
        scheduler.add_job(_check_due_reminders, "interval", seconds=60)
        # Zelle Gmail monitor (every 90 seconds)
        if GMAIL_CREDENTIALS_JSON and GMAIL_USER_EMAIL:
            scheduler.add_job(_check_zelle_emails, "interval", seconds=90)
            print("Zelle Gmail monitor started (90s interval)")
        scheduler.start()
        print("Reminder scheduler started (60s interval)")
    except ImportError:
        print("WARNING: apscheduler not installed — reminders will not fire automatically")
    except Exception as e:
        print(f"WARNING: Failed to start reminder scheduler: {e}")


# ─────────────────────────────────────────────
# Reminder & Event helpers
# ─────────────────────────────────────────────

def _parse_iso_to_utc(iso_str: str) -> datetime.datetime:
    """Parse ISO datetime. Naive = America/Los_Angeles. Returns UTC."""
    iso_str = iso_str.strip().replace("Z", "+00:00")
    dt = datetime.datetime.fromisoformat(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=LA_TZ)
    return dt.astimezone(UTC_TZ)

def _fmt_la(dt_val: datetime.datetime) -> str:
    """Format datetime for display in LA time."""
    if dt_val.tzinfo is None:
        dt_val = dt_val.replace(tzinfo=UTC_TZ)
    return dt_val.astimezone(LA_TZ).strftime("%Y-%m-%d %H:%M %Z")

async def _get_user_contact(uid: int) -> dict:
    """Find user's email + phone from user_contacts table, fallback to Odoo."""
    email = phone = None
    conn = await get_db_conn()
    if conn:
        try:
            row = await conn.fetchrow("SELECT email, phone FROM user_contacts WHERE uid=$1", uid)
            if row:
                email = row["email"] or None
                phone = row["phone"] or None
        except Exception as e:
            print(f"get_user_contact DB error: {e}")
        finally:
            await conn.close()
    if not email or not phone:
        try:
            users_r = json.loads(await odoo_query("res.users", [["id","=",uid]],
                                                   ["login","email","partner_id"], limit=1))
            if isinstance(users_r, list) and users_r:
                u = users_r[0]
                if not email:
                    email = u.get("email") or u.get("login")
                if not phone and u.get("partner_id"):
                    pid = u["partner_id"][0]
                    partners_r = json.loads(await odoo_query("res.partner", [["id","=",pid]],
                                                             ["phone","mobile"], limit=1))
                    if isinstance(partners_r, list) and partners_r:
                        p = partners_r[0]
                        phone = p.get("mobile") or p.get("phone") or None
        except Exception as e:
            print(f"get_user_contact Odoo error: {e}")
    return {"email": email, "phone": phone}

async def _send_email(to_email: str, subject: str, body_text: str) -> tuple:
    """Send email via SendGrid."""
    if not SENDGRID_API_KEY:
        return False, "SENDGRID_API_KEY not configured"
    if not to_email:
        return False, "no recipient email"
    body_html = f"<pre style='font-family:system-ui,sans-serif;font-size:15px;white-space:pre-wrap'>{body_text}</pre>"
    payload = {
        "personalizations": [{"to": [{"email": to_email}]}],
        "from": {"email": SENDGRID_FROM, "name": SENDGRID_FROM_NAME},
        "subject": subject,
        "content": [{"type": "text/plain", "value": body_text}, {"type": "text/html", "value": body_html}],
    }
    try:
        async with httpx.AsyncClient(timeout=15) as c:
            r = await c.post("https://api.sendgrid.com/v3/mail/send",
                headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
                json=payload)
        return (True, None) if r.status_code in (200, 202) else (False, f"SendGrid {r.status_code}: {r.text[:200]}")
    except Exception as e:
        return False, f"SendGrid exception: {e}"

async def _send_sms(to_phone: str, body: str) -> tuple:
    """Send SMS via Twilio."""
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER):
        return False, "Twilio not configured"
    if not to_phone:
        return False, "no recipient phone"
    try:
        async with httpx.AsyncClient(timeout=15,
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)) as c:
            r = await c.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Messages.json",
                data={"From": TWILIO_FROM_NUMBER, "To": to_phone, "Body": body[:1000]})
        return (True, None) if r.status_code in (200, 201) else (False, f"Twilio SMS {r.status_code}: {r.text[:200]}")
    except Exception as e:
        return False, f"Twilio SMS exception: {e}"

async def _send_voice_call(to_phone: str, message: str) -> tuple:
    """Place Twilio voice call that reads message aloud, repeats once."""
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_FROM_NUMBER):
        return False, "Twilio not configured"
    if not to_phone:
        return False, "no recipient phone"
    safe = (message.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            .replace('"',"&quot;").replace("'","&apos;"))
    has_cjk = any('\u4e00' <= ch <= '\u9fff' for ch in message)
    voice = "Polly.Zhiyu" if has_cjk else "Polly.Joanna"
    lang  = "cmn-CN" if has_cjk else "en-US"
    twiml = (f'<Response><Pause length="1"/>'
             f'<Say voice="{voice}" language="{lang}">{safe}</Say>'
             f'<Pause length="1"/>'
             f'<Say voice="{voice}" language="{lang}">{safe}</Say></Response>')
    try:
        async with httpx.AsyncClient(timeout=15,
            auth=(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)) as c:
            r = await c.post(
                f"https://api.twilio.com/2010-04-01/Accounts/{TWILIO_ACCOUNT_SID}/Calls.json",
                data={"From": TWILIO_FROM_NUMBER, "To": to_phone, "Twiml": twiml})
        return (True, None) if r.status_code in (200, 201) else (False, f"Twilio Call {r.status_code}: {r.text[:200]}")
    except Exception as e:
        return False, f"Twilio Call exception: {e}"

async def _check_due_reminders():
    """Called every 60s by APScheduler. Fires due reminders."""
    conn = await get_db_conn()
    if not conn:
        return
    try:
        now_utc = datetime.datetime.now(UTC_TZ)
        rows = await conn.fetch("""
            SELECT id, uid, user_name, content, fire_at, channels,
                   target_email, target_phone
            FROM reminders WHERE fired = FALSE AND fire_at <= $1
            ORDER BY fire_at LIMIT 50
        """, now_utc)
        if not rows:
            return
        print(f"[REMINDER] {len(rows)} due reminder(s)")
        for r in rows:
            content = r["content"]
            channels = list(r["channels"] or ["email"])
            email, phone = r["target_email"], r["target_phone"]
            if (not email and "email" in channels) or \
               (not phone and ("sms" in channels or "call" in channels)):
                contact = await _get_user_contact(r["uid"])
                email = email or contact["email"]
                phone = phone or contact["phone"]
            errors = []
            if "email" in channels:
                ok, err = await _send_email(email, "⏰ 提醒 / Reminder",
                    f"Chumart AI Reminder\n\n📌 {content}\n\nScheduled: {_fmt_la(r['fire_at'])}")
                if not ok: errors.append(f"email:{err}")
            if "sms" in channels:
                ok, err = await _send_sms(phone, f"⏰ Chumart AI: {content}")
                if not ok: errors.append(f"sms:{err}")
            if "call" in channels:
                has_cjk = any('\u4e00' <= ch <= '\u9fff' for ch in content)
                msg = f"你好,这是 Chumart AI 的提醒。{content}" if has_cjk else f"Hello, Chumart AI reminder. {content}"
                ok, err = await _send_voice_call(phone, msg)
                if not ok: errors.append(f"call:{err}")
            print(f"[REMINDER] id={r['id']} fired, errors={errors or 'none'}")
            await conn.execute("UPDATE reminders SET fired=TRUE, fired_at=NOW(), error=$1 WHERE id=$2",
                ("; ".join(errors) if errors else None), r["id"])
    except Exception as e:
        print(f"check_due_reminders error: {e}")
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# Embedding
# ─────────────────────────────────────────────

async def get_embedding(text: str) -> Optional[list]:
    """Get text embedding via OpenAI text-embedding-3-small (1536 dims)."""
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        print("ERROR: OPENAI_API_KEY not set — cannot generate embeddings")
        return None
    try:
        async with httpx.AsyncClient(timeout=20) as c:
            r = await c.post(
                "https://api.openai.com/v1/embeddings",
                headers={
                    "Authorization": f"Bearer {openai_key}",
                    "Content-Type": "application/json"
                },
                json={
                    "model": "text-embedding-3-small",
                    "input": text[:8000]
                }
            )
            data = r.json()
            if "error" in data:
                print(f"OpenAI embedding API error: {data['error'].get('message', data['error'])}")
                return None
            if "data" not in data or not data["data"]:
                print(f"OpenAI embedding unexpected response: {data}")
                return None
            return data["data"][0]["embedding"]
    except Exception as e:
        print(f"OpenAI embedding exception: {e}")
        return None


# ─────────────────────────────────────────────
# Cloudflare R2 helpers
# ─────────────────────────────────────────────

def get_r2_client():
    """Get boto3 S3 client configured for Cloudflare R2."""
    try:
        import boto3
        return boto3.client(
            "s3",
            endpoint_url=f"https://{R2_ACCOUNT_ID}.r2.cloudflarestorage.com",
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto"
        )
    except Exception as e:
        print(f"R2 client error: {e}")
        return None

async def r2_upload(file_bytes: bytes, r2_key: str, content_type: str) -> bool:
    """Upload file to R2."""
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        client = get_r2_client()
        if not client:
            return False
        await loop.run_in_executor(None, lambda: client.put_object(
            Bucket=R2_BUCKET,
            Key=r2_key,
            Body=file_bytes,
            ContentType=content_type
        ))
        return True
    except Exception as e:
        print(f"R2 upload error: {e}")
        return False

async def r2_delete(r2_key: str) -> bool:
    """Delete file from R2."""
    try:
        import asyncio
        loop = asyncio.get_event_loop()
        client = get_r2_client()
        if not client:
            return False
        await loop.run_in_executor(None, lambda: client.delete_object(
            Bucket=R2_BUCKET, Key=r2_key
        ))
        return True
    except Exception as e:
        print(f"R2 delete error: {e}")
        return False

# ─────────────────────────────────────────────
# Document text extraction
# ─────────────────────────────────────────────

async def extract_text_from_file(file_bytes: bytes, filename: str, mime_type: str) -> str:
    """Extract text from PDF, Word, image, or plain text files.
    PDF: MinerU Cloud API (OCR + layout-aware), fallback to Claude Vision.
    Image: Claude Vision.
    Word: python-docx, fallback to Claude Vision.
    """
    fname = filename.lower()

    # Plain text
    if fname.endswith(('.txt', '.md', '.csv')):
        return file_bytes.decode('utf-8', errors='ignore')

    # PDF — use MinerU Cloud API (with Claude Vision fallback)
    if fname.endswith('.pdf'):
        if MINERU_API_TOKEN:
            text = await _extract_pdf_mineru(file_bytes, filename)
            if text:
                return text
            print(f"MINERU FAILED for {filename}, falling back to Claude Vision")
        else:
            print(f"MINERU_API_TOKEN not set, using Claude Vision for {filename}")
        return await _extract_via_claude_vision(file_bytes, filename, "document", "application/pdf")

    # Image — Claude Vision
    if fname.endswith(('.png', '.jpg', '.jpeg', '.webp')):
        ext = fname.split('.')[-1].replace('jpg', 'jpeg')
        return await _extract_via_claude_vision(file_bytes, filename, "image", f"image/{ext}")

    # Word — python-docx first, then Claude Vision fallback
    if fname.endswith(('.docx', '.doc')):
        try:
            import io
            from docx import Document as DocxDocument
            doc = DocxDocument(io.BytesIO(file_bytes))
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            return text
        except Exception:
            return await _extract_via_claude_vision(
                file_bytes, filename, "document",
                "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
            )

    # Unknown format — try as text
    return file_bytes.decode('utf-8', errors='ignore')


async def _extract_pdf_mineru(file_bytes: bytes, filename: str) -> str:
    """Extract text from PDF using MinerU Cloud API.
    Flow: request upload URL → PUT file → poll for result → download zip → extract markdown.
    """
    import zipfile, io

    data_id = str(uuid.uuid4())[:8]
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {MINERU_API_TOKEN}"
    }

    print(f"MINERU START: {filename} ({len(file_bytes)//1024}KB)")

    try:
        # Step 1: Request upload URL
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post(
                "https://mineru.net/api/v4/file-urls/batch",
                headers=headers,
                json={
                    "files": [{"name": filename, "data_id": data_id}],
                    "model_version": "vlm",
                    "is_ocr": True,
                    "enable_table": True,
                    "language": "en"
                }
            )
            result = r.json()
            if result.get("code") != 0:
                print(f"MINERU UPLOAD-URL FAIL: {result.get('msg', result)}")
                return ""

            batch_id = result["data"]["batch_id"]
            upload_url = result["data"]["file_urls"][0]
            print(f"MINERU BATCH: {batch_id}, uploading file...")

        # Step 2: PUT file bytes to the upload URL (600s for large files over slow links)
        async with httpx.AsyncClient(timeout=600) as c:
            r = await c.put(upload_url, content=file_bytes)
            if r.status_code not in (200, 201):
                print(f"MINERU UPLOAD FAIL: HTTP {r.status_code}")
                return ""
            print(f"MINERU UPLOAD OK")

        # Step 3: Poll for result (max 15 min, check every 15s)
        max_wait = 900
        interval = 15
        elapsed = 0
        full_zip_url = None

        async with httpx.AsyncClient(timeout=30) as c:
            while elapsed < max_wait:
                await asyncio.sleep(interval)
                elapsed += interval

                r = await c.get(
                    f"https://mineru.net/api/v4/extract-results/batch/{batch_id}",
                    headers=headers
                )
                poll = r.json()
                if poll.get("code") != 0:
                    print(f"MINERU POLL ERROR [{elapsed}s]: {poll.get('msg', poll)}")
                    continue

                results = poll.get("data", {}).get("extract_result", [])
                if not results:
                    print(f"MINERU POLL [{elapsed}s]: no results yet")
                    continue

                item = results[0]
                state = item.get("state", "unknown")

                if state == "done":
                    full_zip_url = item.get("full_zip_url", "")
                    print(f"MINERU DONE [{elapsed}s]: {full_zip_url[:80]}...")
                    break
                elif state in ("failed", "error"):
                    print(f"MINERU FAILED [{elapsed}s]: {item.get('err_msg', 'unknown error')}")
                    return ""
                else:
                    progress = item.get("extract_progress", {})
                    pages_done = progress.get("extracted_pages", "?")
                    pages_total = progress.get("total_pages", "?")
                    print(f"MINERU POLL [{elapsed}s]: state={state} pages={pages_done}/{pages_total}")

        if not full_zip_url:
            print(f"MINERU TIMEOUT after {max_wait}s")
            return ""

        # Step 4: Download zip and extract markdown
        async with httpx.AsyncClient(timeout=300) as c:
            r = await c.get(full_zip_url)
            if r.status_code != 200:
                print(f"MINERU ZIP DOWNLOAD FAIL: HTTP {r.status_code}")
                return ""

            zip_bytes = r.content
            print(f"MINERU ZIP: {len(zip_bytes)//1024}KB downloaded")

        # Extract .md file from zip
        text = ""
        with zipfile.ZipFile(io.BytesIO(zip_bytes), 'r') as zf:
            md_files = [n for n in zf.namelist() if n.endswith('.md')]
            # Prefer full.md or the largest .md file
            target = None
            for n in md_files:
                if 'full' in n.lower():
                    target = n
                    break
            if not target and md_files:
                # Pick the largest .md file
                target = max(md_files, key=lambda n: zf.getinfo(n).file_size)

            if target:
                text = zf.read(target).decode('utf-8', errors='ignore')
                print(f"MINERU EXTRACT: {target} → {len(text)} chars")
            else:
                print(f"MINERU: no .md file found in zip. Contents: {zf.namelist()[:10]}")

        return text

    except Exception as e:
        print(f"MINERU EXCEPTION: {e}")
        return ""


async def _extract_via_claude_vision(file_bytes: bytes, filename: str, doc_type: str, media_type: str) -> str:
    """Extract text from PDF/image/docx using Claude Vision (legacy fallback)."""
    b64 = base64.standard_b64encode(file_bytes).decode('utf-8')
    print(f"TEXT EXTRACT: {filename} ({len(file_bytes)//1024}KB) via Claude Vision")

    async def extract_pass(prompt_suffix=""):
        try:
            async with httpx.AsyncClient(timeout=300) as c:
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-5",
                        "max_tokens": 8000,
                        "messages": [{
                            "role": "user",
                            "content": [
                                {"type": doc_type, "source": {"type": "base64", "media_type": media_type, "data": b64}},
                                {"type": "text", "text": f"Extract ALL text content from this document completely. Include every section, table, specification, error code, procedure, troubleshooting steps, parts list, and detail. Do NOT skip or summarize any section. Return raw text only, no commentary.{prompt_suffix}"}
                            ]
                        }]
                    }
                )
                data = r.json()
                if "error" in data:
                    print(f"TEXT EXTRACT ERROR {filename}: {data['error']}")
                    return ""
                text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
                return text
        except Exception as e:
            print(f"Text extraction error {filename}: {e}")
            return ""

    text = await extract_pass()
    print(f"TEXT EXTRACT OK: {filename} → {len(text)} chars")

    # If PDF is large (>100KB) and text seems short (<3000 chars), try a second focused pass
    if len(file_bytes) > 100_000 and len(text) < 3000 and doc_type == "document":
        print(f"TEXT EXTRACT: result may be truncated, trying focused pass on later sections...")
        text2 = await extract_pass(" Focus especially on the LATTER HALF of the document: troubleshooting, service procedures, error codes, parts lists, maintenance sections.")
        if len(text2) > len(text):
            text = text + "\n\n--- [Additional content from second extraction pass] ---\n\n" + text2
            print(f"TEXT EXTRACT MERGED: {filename} → {len(text)} chars total")

    return text

async def process_document_to_kb(doc_id: str, doc_name: str, text: str, category: str, description: str = ""):
    """Chunk document text and store in knowledge base."""
    if not text.strip():
        return 0

    conn = await get_db_conn()
    if not conn:
        return 0

    try:
        await conn.execute("DELETE FROM knowledge_chunks WHERE site_url = $1", f"doc:{doc_id}")
        chunks = chunk_text(text, chunk_size=600, overlap=100)
        count = 0

        # Index chunk: filename + category + description (model numbers) + first content
        # This is the "findability" chunk — searched when user mentions model numbers
        index_parts = [f"Document: {doc_name}", f"Category: {category}"]
        if description:
            index_parts.append(f"Contains: {description}")
        index_parts.append(text[:800])
        index_chunk = "\n".join(index_parts)

        index_embedding = await get_embedding(index_chunk)
        if index_embedding:
            await conn.execute("""
                INSERT INTO knowledge_chunks
                (site_name, site_url, page_url, page_title, chunk_text, embedding, category)
                VALUES ($1, $2, $3, $4, $5, $6::vector, $7)
            """, doc_name, f"doc:{doc_id}", f"doc:{doc_id}", doc_name,
                index_chunk, json.dumps(index_embedding), category)
            count += 1

        for chunk in chunks:
            if not chunk.strip():
                continue
            embedding = await get_embedding(chunk)
            if embedding:
                await conn.execute("""
                    INSERT INTO knowledge_chunks
                    (site_name, site_url, page_url, page_title, chunk_text, embedding, category)
                    VALUES ($1, $2, $3, $4, $5, $6::vector, $7)
                """, doc_name, f"doc:{doc_id}", f"doc:{doc_id}", doc_name, chunk,
                    json.dumps(embedding), category)
                count += 1

        # Update chunk count
        await conn.execute("UPDATE documents SET chunk_count=$1 WHERE id=$2", count, doc_id)
        return count
    except Exception as e:
        print(f"Document KB processing error: {e}")
        return 0
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# Web crawler
# ─────────────────────────────────────────────

def clean_html(html: str) -> str:
    """Strip HTML tags and clean whitespace."""
    text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<[^>]+>', ' ', text)
    text = re.sub(r'&nbsp;', ' ', text)
    text = re.sub(r'&amp;', '&', text)
    text = re.sub(r'&lt;', '<', text)
    text = re.sub(r'&gt;', '>', text)
    text = re.sub(r'&quot;', '"', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def extract_title(html: str) -> str:
    m = re.search(r'<title[^>]*>(.*?)</title>', html, re.IGNORECASE | re.DOTALL)
    return clean_html(m.group(1)) if m else ""

def extract_links(html: str, base_url: str) -> list:
    """Extract all internal links from a page."""
    base_domain = urlparse(base_url).netloc
    links = []
    for href in re.findall(r'href=["\']([^"\']+)["\']', html):
        if href.startswith('#') or href.startswith('mailto:') or href.startswith('tel:'):
            continue
        full_url = urljoin(base_url, href)
        parsed = urlparse(full_url)
        if parsed.netloc == base_domain and parsed.scheme in ('http', 'https'):
            # Strip query params and fragments for deduplication
            clean = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
            if clean not in links:
                links.append(clean)
    return links

def chunk_text(text: str, chunk_size: int = 600, overlap: int = 100) -> list:
    """Split text into overlapping chunks."""
    if len(text) <= chunk_size:
        return [text] if text.strip() else []
    chunks = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if chunk.strip():
            chunks.append(chunk.strip())
        start += chunk_size - overlap
    return chunks

async def crawl_site(site: dict, log_id: int):
    """Crawl all pages of a site and store chunks in DB."""
    conn = await get_db_conn()
    if not conn:
        return

    base_url = site["url"]
    site_name = site["name"]
    category = site["category"]
    visited = set()
    to_visit = [base_url]
    total_chunks = 0
    total_pages = 0

    # Delete old data for this site
    await conn.execute("DELETE FROM knowledge_chunks WHERE site_url = $1", base_url)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "en-US,en;q=0.9"
    }

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        while to_visit and len(visited) < 200:  # Max 200 pages per site
            url = to_visit.pop(0)
            if url in visited:
                continue
            visited.add(url)

            try:
                r = await client.get(url, headers=headers)
                if r.status_code != 200:
                    continue
                if 'text/html' not in r.headers.get('content-type', ''):
                    continue

                html = r.text
                title = extract_title(html)
                text = clean_html(html)

                # Skip very short pages
                if len(text) < 100:
                    continue

                # Get new links to crawl
                new_links = extract_links(html, url)
                for link in new_links:
                    if link not in visited and link not in to_visit:
                        to_visit.append(link)

                # Chunk and embed
                chunks = chunk_text(f"Page: {title}\nURL: {url}\n\n{text}")
                for chunk in chunks:
                    embedding = await get_embedding(chunk)
                    if embedding:
                        await conn.execute("""
                            INSERT INTO knowledge_chunks
                            (site_name, site_url, page_url, page_title, chunk_text, embedding, category)
                            VALUES ($1, $2, $3, $4, $5, $6::vector, $7)
                        """, site_name, base_url, url, title, chunk, json.dumps(embedding), category)
                        total_chunks += 1

                total_pages += 1
                print(f"Crawled: {url} ({len(chunks)} chunks)")
                await asyncio.sleep(0.5)  # Be polite

            except Exception as e:
                print(f"Error crawling {url}: {e}")
                continue

    # Update crawl log
    await conn.execute("""
        UPDATE crawl_log SET status='done', pages=$1, chunks=$2, finished_at=NOW()
        WHERE id=$3
    """, total_pages, total_chunks, log_id)
    await conn.close()
    print(f"Done crawling {site_name}: {total_pages} pages, {total_chunks} chunks")

# ─────────────────────────────────────────────
# Knowledge base search
# ─────────────────────────────────────────────

async def search_knowledge(query: str, top_k: int = 10, category: str = None, doc_name_filter: str = None) -> list:
    """Vector similarity search in knowledge base."""
    conn = await get_db_conn()
    if not conn:
        return []
    try:
        embedding = await get_embedding(query)
        if not embedding:
            return []

        if category and doc_name_filter:
            rows = await conn.fetch("""
                SELECT site_name, site_url, page_url, page_title, chunk_text,
                       1 - (embedding <=> $1::vector) AS similarity
                FROM knowledge_chunks
                WHERE (category = $2 OR category = $3)
                  AND site_name ILIKE $4
                ORDER BY embedding <=> $1::vector
                LIMIT $5
            """, json.dumps(embedding), category, f"doc_{category}", f"%{doc_name_filter}%", top_k)
        elif doc_name_filter:
            rows = await conn.fetch("""
                SELECT site_name, site_url, page_url, page_title, chunk_text,
                       1 - (embedding <=> $1::vector) AS similarity
                FROM knowledge_chunks
                WHERE site_name ILIKE $2
                ORDER BY embedding <=> $1::vector
                LIMIT $3
            """, json.dumps(embedding), f"%{doc_name_filter}%", top_k)
        elif category:
            rows = await conn.fetch("""
                SELECT site_name, site_url, page_url, page_title, chunk_text,
                       1 - (embedding <=> $1::vector) AS similarity
                FROM knowledge_chunks
                WHERE category = $2 OR category = $3
                ORDER BY embedding <=> $1::vector
                LIMIT $4
            """, json.dumps(embedding), category, f"doc_{category}", top_k)
        else:
            rows = await conn.fetch("""
                SELECT site_name, site_url, page_url, page_title, chunk_text,
                       1 - (embedding <=> $1::vector) AS similarity
                FROM knowledge_chunks
                ORDER BY embedding <=> $1::vector
                LIMIT $2
            """, json.dumps(embedding), top_k)

        return [dict(r) for r in rows]
    except Exception as e:
        print(f"Search error: {e}")
        return []
    finally:
        await conn.close()

async def get_kb_context(query: str) -> str:
    """Get formatted knowledge base context for a query."""
    results = await search_knowledge(query, top_k=5)
    if not results:
        return ""
    parts = ["=== KNOWLEDGE BASE (from our websites) ==="]
    for r in results:
        if r.get("similarity", 0) > 0.3:  # Only include relevant results
            parts.append(f"\n[Source: {r['site_name']} - {r['page_title']}]")
            parts.append(r["chunk_text"])
    if len(parts) == 1:
        return ""
    return "\n".join(parts) + "\n=== END KNOWLEDGE BASE ==="

# ─────────────────────────────────────────────
# Odoo Write operations
# ─────────────────────────────────────────────

async def odoo_get_session():
    """Get a reusable authenticated session (cookies) for Odoo."""
    async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
        login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
        })
        return dict(login_r.cookies)

async def odoo_create(model: str, vals: dict, cookies=None) -> dict:
    """Create a record in Odoo. Returns {id} or {error}."""
    try:
        async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
            if not cookies:
                login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                    "jsonrpc": "2.0", "method": "call", "id": 1,
                    "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
                })
                cookies = dict(login_r.cookies)
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {"model": model, "method": "create", "args": [vals], "kwargs": {}}
            }, cookies=cookies)
            data = r.json()
            if data.get("error"):
                return {"error": data["error"].get("data", {}).get("message", str(data["error"]))}
            return {"id": data.get("result"), "success": True}
    except Exception as e:
        return {"error": str(e)}


async def resolve_po_vendor(suggested_partner_id, line_product_ids: list, cookies=None):
    """
    Verify & auto-fix the vendor for a purchase order.

    Returns: (final_partner_id, final_vendor_name, fix_note)
      - fix_note is None if no fix was needed, else a human-readable string.

    Logic:
      1. Check if suggested partner is a real supplier (supplier_rank > 0).
      2. Query supplierinfo for ALL products in the PO.
      3. Keep AI's choice only if valid supplier AND actually supplies these products.
      4. Otherwise pick the vendor covering the MOST products.
    """
    final_partner_id = suggested_partner_id
    final_vendor_name = ""
    fix_note = None

    if not line_product_ids:
        print(f"RESOLVE_VENDOR: no product_ids passed, keeping suggested partner_id={suggested_partner_id}")
        return final_partner_id, final_vendor_name, fix_note

    # (a) Is the suggested partner actually a supplier?
    is_valid_supplier = False
    if suggested_partner_id:
        chk = await odoo_query("res.partner",
            [["id","=",suggested_partner_id]],
            ["id","name","supplier_rank"], limit=1, cookies=cookies)
        chk_data = json.loads(chk)
        if isinstance(chk_data, list) and chk_data:
            final_vendor_name = chk_data[0].get("name", "")
            is_valid_supplier = (chk_data[0].get("supplier_rank", 0) or 0) > 0

    # (b) Map products → templates
    # (b) Map products → templates. AI sometimes passes product_tmpl_id or
    # even invalid IDs — also query by product_tmpl_id as a fallback so we
    # can resolve vendors across as many of the PO's products as possible.
    prod_r = await odoo_query("product.product",
        [["id","in",line_product_ids]],
        ["id","product_tmpl_id"], limit=500, cookies=cookies)
    prod_rows = json.loads(prod_r)
    if not isinstance(prod_rows, list):
        prod_rows = []
    tmpl_ids = {p["product_tmpl_id"][0] for p in prod_rows
                if p.get("product_tmpl_id")}
    found_pids = {p["id"] for p in prod_rows}

    # Treat any IDs that weren't valid product.product records as possible
    # product.template IDs and verify them
    unknown_ids = [pid for pid in line_product_ids if pid not in found_pids]
    if unknown_ids:
        tmpl_r = await odoo_query("product.template",
            [["id","in",unknown_ids]],
            ["id"], limit=500, cookies=cookies)
        tmpl_rows = json.loads(tmpl_r)
        if isinstance(tmpl_rows, list):
            for t in tmpl_rows:
                tmpl_ids.add(t["id"])
            print(f"RESOLVE_VENDOR: {len(tmpl_rows)}/{len(unknown_ids)} "
                  f"unknown IDs were actually template IDs (AI error)")

    tmpl_ids = list(tmpl_ids)
    if not tmpl_ids:
        print(f"RESOLVE_VENDOR: no templates resolved from products {line_product_ids}")
        return final_partner_id, final_vendor_name, fix_note

    # (c) Query supplierinfo for ALL products in this PO
    sup_r = await odoo_query("product.supplierinfo",
        [["product_tmpl_id","in",tmpl_ids]],
        ["product_tmpl_id","partner_id"], limit=1000,
        order="sequence asc", cookies=cookies)
    sup_rows = json.loads(sup_r)
    if not isinstance(sup_rows, list):
        print(f"RESOLVE_VENDOR: supplierinfo query failed: {sup_rows}")
        return final_partner_id, final_vendor_name, fix_note

    # Count unique templates each vendor supplies
    vendor_info = {}  # vid -> {name, tmpls: set()}
    for s in sup_rows:
        if not s.get("partner_id"): continue
        vid = s["partner_id"][0]
        vname = s["partner_id"][1]
        tid = s["product_tmpl_id"][0] if s.get("product_tmpl_id") else None
        if tid is None: continue
        if vid not in vendor_info:
            vendor_info[vid] = {"name": vname, "tmpls": set()}
        vendor_info[vid]["tmpls"].add(tid)

    print(f"RESOLVE_VENDOR: {len(tmpl_ids)} templates, "
          f"{len(vendor_info)} candidate vendors: "
          f"{[(v['name'], len(v['tmpls'])) for v in vendor_info.values()]}")

    if not vendor_info:
        print(f"RESOLVE_VENDOR: no supplierinfo found for tmpls {tmpl_ids}, "
              f"keeping suggested partner_id={suggested_partner_id}")
        return final_partner_id, final_vendor_name, fix_note

    # Keep AI's choice ONLY if: valid supplier AND actually supplies these products
    keep_suggested = (is_valid_supplier
                      and suggested_partner_id in vendor_info
                      and len(vendor_info[suggested_partner_id]["tmpls"]) > 0)
    if keep_suggested:
        final_vendor_name = vendor_info[suggested_partner_id]["name"]
        print(f"RESOLVE_VENDOR: keeping AI's choice → {final_vendor_name} "
              f"(id={suggested_partner_id}, "
              f"covers {len(vendor_info[suggested_partner_id]['tmpls'])}/{len(tmpl_ids)})")
        return final_partner_id, final_vendor_name, fix_note

    # Pick vendor with greatest coverage
    best_vid = max(vendor_info.keys(),
        key=lambda v: len(vendor_info[v]["tmpls"]))
    best = vendor_info[best_vid]
    fix_note = (f"AI suggested '{final_vendor_name}' "
                f"(id={suggested_partner_id}, "
                f"supplier_rank={'>0' if is_valid_supplier else '=0'}), "
                f"replaced with '{best['name']}' (id={best_vid}, "
                f"covers {len(best['tmpls'])}/{len(tmpl_ids)} products)")
    print(f"VENDOR FIX: {fix_note}")
    return best_vid, best["name"], fix_note

async def odoo_write_record(model: str, record_id: int, vals: dict, cookies=None) -> dict:
    """Update a record in Odoo."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
            if not cookies:
                login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                    "jsonrpc": "2.0", "method": "call", "id": 1,
                    "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
                })
                cookies = dict(login_r.cookies)
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {"model": model, "method": "write", "args": [[record_id], vals], "kwargs": {}}
            }, cookies=cookies)
            data = r.json()
            if data.get("error"):
                return {"error": data["error"].get("data", {}).get("message", str(data["error"]))}
            return {"success": True}
    except Exception as e:
        return {"error": str(e)}

async def odoo_call_method(model: str, record_id: int, method: str) -> dict:
    """Call an action method on a record (e.g. button_confirm)."""
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
            login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            cookies = login_r.cookies
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {"model": model, "method": method, "args": [[record_id]], "kwargs": {}}
            }, cookies=cookies)
            data = r.json()
            if data.get("error"):
                return {"error": data["error"].get("data", {}).get("message", str(data["error"]))}
            return {"success": True}
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# Odoo helpers (unchanged)
# ─────────────────────────────────────────────

async def odoo_query(model, domain, fields, limit=2000, order="id desc", cookies=None, offset=0):
    last_error = None
    for attempt in range(3):  # Auto-retry up to 3 times
        try:
            async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
                if not cookies or attempt > 0:  # Re-login on retry
                    login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                        "jsonrpc": "2.0", "method": "call", "id": 1,
                        "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
                    })
                    cookies = dict(login_r.cookies)
                r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                    "jsonrpc": "2.0", "method": "call", "id": 2,
                    "params": {
                        "model": model, "method": "search_read",
                        "args": [domain],
                        "kwargs": {"fields": fields, "limit": limit, "order": order, "offset": offset}
                    }
                }, cookies=cookies)
                data = r.json()
                if data.get("error"):
                    last_error = data["error"].get("message", str(data["error"]))
                    if attempt < 2:
                        await asyncio.sleep(1)
                        continue
                    return json.dumps({"error": last_error})
                return json.dumps(data.get("result", []), default=str, ensure_ascii=False)
        except Exception as e:
            last_error = str(e)
            if attempt < 2:
                await asyncio.sleep(1)
            continue
    return json.dumps({"error": f"Failed after 3 attempts: {last_error}"})

async def odoo_list_fields(model):
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            cookies = login_r.cookies
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {
                    "model": model, "method": "fields_get",
                    "args": [],
                    "kwargs": {"attributes": ["string", "type"]}
                }
            }, cookies=cookies)
            data = r.json()
            fields = {k: {"label": v.get("string", ""), "type": v.get("type", "")}
                      for k, v in data.get("result", {}).items()
                      if v.get("type") in ["char", "integer", "float", "monetary", "date", "datetime", "boolean", "many2one", "selection"]}
            return json.dumps(fields, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ─────────────────────────────────────────────
# Report helpers (unchanged)
# ─────────────────────────────────────────────

async def fetch_moves(move_type, date_from, date_to):
    result = await odoo_query(
        "account.move",
        [["move_type","=",move_type],["state","=","posted"],
         ["invoice_date",">=",date_from],["invoice_date","<=",date_to],
         ["company_id","=",1],["payment_state","in",VALID_STATES]],
        ["name", "invoice_partner_display_name", "partner_id", "invoice_user_id",
         "invoice_date", "invoice_origin", "amount_untaxed", "amount_untaxed_signed",
         "amount_tax", "amount_tax_signed", "amount_total", "amount_total_signed", "move_type",
         "payment_state", "ref", "source_id", "x_payment_method", "tag_ids"],
        limit=2000
    )
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records:
        return [], records["error"]
    return records, None

async def fetch_credits(date_from, date_to):
    """Fetch credit notes with same payment_state filter as invoices (Commission Deduct logic)."""
    result = await odoo_query(
        "account.move",
        [["move_type","=","out_refund"],["state","=","posted"],
         ["invoice_date",">=",date_from],["invoice_date","<=",date_to],
         ["company_id","=",1],["payment_state","in",VALID_STATES]],
        ["name", "invoice_partner_display_name", "partner_id", "invoice_user_id",
         "invoice_date", "invoice_origin", "amount_untaxed", "amount_untaxed_signed",
         "amount_tax", "amount_tax_signed", "amount_total", "amount_total_signed", "move_type",
         "payment_state", "ref", "source_id", "x_payment_method", "tag_ids"],
        limit=2000
    )
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records:
        return [], records["error"]
    return records, None

def summarize_moves(records, is_credit=False):
    by_state = {"paid":0,"in_payment":0,"reversed":0}
    total_untaxed = total_tax = total_amount = 0
    sign = -1 if is_credit else 1
    for r in records:
        state = r.get("payment_state","")
        if state in by_state: by_state[state] += 1
        # amount_untaxed_signed: reliable in Odoo 17 (negative for credits)
        total_untaxed += r.get("amount_untaxed_signed", 0)
        # amount_tax and amount_total: use manual sign (Odoo 17 _signed tax fields may have unexpected signs for credits)
        total_tax    += r.get("amount_tax", 0) * sign
        total_amount += r.get("amount_total", 0) * sign
    if is_credit and records:
        # Debug: log first credit note's values to verify signs
        r0 = records[0]
        print(f"CREDIT DEBUG: name={r0.get('name')} untaxed_signed={r0.get('amount_untaxed_signed')} tax={r0.get('amount_tax')} tax_signed={r0.get('amount_tax_signed')} total={r0.get('amount_total')} total_signed={r0.get('amount_total_signed')}")
    return {"count":len(records),"by_payment_state":by_state,
            "total_untaxed":round(total_untaxed,2),"total_tax":round(total_tax,2),"total_amount":round(total_amount,2)}

@app.get("/report/monthly-tax")
async def monthly_tax(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day}"
    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits,  err2 = await fetch_credits(date_from, date_to)
    if err1 or err2: return {"error": err1 or err2}
    inv = summarize_moves(invoices)
    crd = summarize_moves(credits, is_credit=True)
    return {"period":f"{year}-{month:02d}","report_type":"Monthly Tax Report","invoices":inv,"credit_notes":crd,
            "net":{"count":inv["count"]-crd["count"],
                   "total_untaxed":round(inv["total_untaxed"]+crd["total_untaxed"],2),
                   "total_tax":round(inv["total_tax"]+crd["total_tax"],2),
                   "total_amount":round(inv["total_amount"]+crd["total_amount"],2)}}

@app.get("/report/quarterly-tax")
async def quarterly_tax(year: int, quarter: int):
    if quarter not in [1,2,3,4]: return {"error":"Quarter must be 1-4"}
    start_month = (quarter-1)*3+1
    end_month   = start_month+2
    last_day    = calendar.monthrange(year, end_month)[1]
    date_from   = f"{year}-{start_month:02d}-01"
    date_to     = f"{year}-{end_month:02d}-{last_day}"
    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits,  err2 = await fetch_credits(date_from, date_to)
    if err1 or err2: return {"error": err1 or err2}
    inv = summarize_moves(invoices)
    crd = summarize_moves(credits, is_credit=True)
    monthly = []
    for m in range(start_month, end_month+1):
        ld = calendar.monthrange(year,m)[1]
        inv_m,_ = await fetch_moves("out_invoice",f"{year}-{m:02d}-01",f"{year}-{m:02d}-{ld}")
        crd_m,_ = await fetch_credits(f"{year}-{m:02d}-01",f"{year}-{m:02d}-{ld}")
        inv_s = summarize_moves(inv_m); crd_s = summarize_moves(crd_m, is_credit=True)
        monthly.append({"month":f"{year}-{m:02d}","invoice_tax":inv_s["total_tax"],
                        "credit_note_tax":crd_s["total_tax"],"net_tax":round(inv_s["total_tax"]-crd_s["total_tax"],2),
                        "invoice_count":inv_s["count"],"credit_note_count":crd_s["count"]})
    return {"period":f"Q{quarter} {year}","report_type":"Quarterly Tax Report",
            "date_range":f"{date_from} to {date_to}","invoices":inv,"credit_notes":crd,
            "net":{"total_untaxed":round(inv["total_untaxed"]+crd["total_untaxed"],2),
                   "total_tax":round(inv["total_tax"]+crd["total_tax"],2),
                   "total_amount":round(inv["total_amount"]+crd["total_amount"],2)},
            "monthly_breakdown":monthly}

@app.get("/report/monthly-sales")
async def monthly_sales(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day}"
    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits,  err2 = await fetch_credits(date_from, date_to)
    if err1 or err2: return {"error": err1 or err2}

    def get_salesperson(r):
        user = r.get("invoice_user_id")
        if user and isinstance(user, (list, tuple)) and len(user) > 1:
            return user[1]
        return "Unassigned"

    def format_row(r):
        """Format one invoice/credit note row matching Odoo SALE COMMISSION NEW template."""
        source = r.get("source_id")
        tags   = r.get("tag_ids", [])
        return {
            "Invoice Partner Display Name": r.get("invoice_partner_display_name") or (r["partner_id"][1] if r.get("partner_id") else ""),
            "Invoice/Bill Date":            r.get("invoice_date", ""),
            "Number":                       r.get("name", ""),
            "Origin":                       r.get("invoice_origin", ""),
            "Untaxed Amount Signed":        r.get("amount_untaxed_signed", r.get("amount_untaxed", 0)),
            "Reference":                    r.get("ref", ""),
            "Source":                       source[1] if source and isinstance(source, (list,tuple)) and len(source)>1 else "",
            "Payment Method":               r.get("x_payment_method", ""),
            "Tags":                         ", ".join(str(t) for t in tags) if tags else "",
            "Salesperson":                  get_salesperson(r),
            "Payment Status":               r.get("payment_state", ""),
        }

    # Group by salesperson for summary
    def group_by_salesperson(records, is_credit=False):
        by_person = {}
        sign = -1 if is_credit else 1
        for r in records:
            name = get_salesperson(r)
            if name not in by_person:
                by_person[name] = {"salesperson": name, "count": 0,
                                   "amount_untaxed": 0, "amount_tax": 0, "amount_total": 0}
            by_person[name]["count"] += 1
            by_person[name]["amount_untaxed"] += r.get("amount_untaxed_signed", 0)
            by_person[name]["amount_tax"]     += r.get("amount_tax", 0) * sign
            by_person[name]["amount_total"]   += r.get("amount_total", 0) * sign
        for p in by_person.values():
            p["amount_untaxed"] = round(p["amount_untaxed"], 2)
            p["amount_tax"]     = round(p["amount_tax"], 2)
            p["amount_total"]   = round(p["amount_total"], 2)
        return sorted(by_person.values(), key=lambda x: x["amount_untaxed"], reverse=True)

    inv_by_person = group_by_salesperson(invoices, is_credit=False)
    crd_by_person = group_by_salesperson(credits, is_credit=True)

    inv_dict = {p["salesperson"]: p for p in inv_by_person}
    crd_dict = {p["salesperson"]: p for p in crd_by_person}
    all_names = sorted(set(inv_dict) | set(crd_dict))
    net_by_person = []
    for name in all_names:
        inv_p = inv_dict.get(name, {"count":0,"amount_untaxed":0,"amount_tax":0,"amount_total":0})
        crd_p = crd_dict.get(name, {"count":0,"amount_untaxed":0,"amount_tax":0,"amount_total":0})
        # amount_untaxed is already signed (negative for credits), so just add
        net_by_person.append({
            "salesperson":        name,
            "invoice_count":      inv_p["count"],
            "credit_note_count":  crd_p["count"],
            "invoice_amount":     round(inv_p["amount_untaxed"], 2),
            "credit_amount":      round(crd_p["amount_untaxed"], 2),
            "net_amount_untaxed": round(inv_p["amount_untaxed"] + crd_p["amount_untaxed"], 2),
            "net_amount_tax":     round(inv_p["amount_tax"]     + crd_p["amount_tax"],     2),
            "net_amount_total":   round(inv_p["amount_total"]   + crd_p["amount_total"],   2),
        })
    net_by_person.sort(key=lambda x: x["net_amount_untaxed"], reverse=True)

    inv_total = summarize_moves(invoices)
    crd_total = summarize_moves(credits, is_credit=True)

    # All rows formatted per Odoo template (invoices + credit notes combined, sorted by salesperson then date)
    all_rows = (
        [format_row(r) for r in invoices] +
        [format_row(r) for r in credits]
    )
    all_rows.sort(key=lambda x: (x["Salesperson"], x["Invoice/Bill Date"]))

    return {
        "period":      f"{year}-{month:02d}",
        "report_type": "Monthly Sales Report (Commission Base)",
        "note":        "Includes paid, in_payment, reversed. Format matches Odoo SALE COMMISSION NEW template.",
        "by_salesperson": net_by_person,
        "commission_base": {
            "net_sales_excl_tax": round(inv_total["total_untaxed"] + crd_total["total_untaxed"], 2),
            "net_sales_incl_tax": round(inv_total["total_amount"]  + crd_total["total_amount"],  2),
            "net_tax":            round(inv_total["total_tax"]     + crd_total["total_tax"],     2),
            "invoice_count":      inv_total["count"],
            "credit_note_count":  crd_total["count"],
        },
        "detail_rows": all_rows,
    }

@app.get("/report/missing-tax")
async def missing_tax(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    result = await odoo_query("account.move",
        [["move_type","=","out_invoice"],["state","=","posted"],
         ["invoice_date",">=",f"{year}-{month:02d}-01"],["invoice_date","<=",f"{year}-{month:02d}-{last_day}"],
         ["company_id","=",1],["amount_tax","=",0],["partner_shipping_id.state_id","in",[CA_STATE_ID]]],
        ["name","partner_id","partner_shipping_id","invoice_date","amount_untaxed","amount_tax","amount_total","payment_state"],
        limit=2000)
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records: return records
    return {"period":f"{year}-{month:02d}","report_type":"Missing Tax Detection - CA Invoices",
            "total_found":len(records),"note":"These CA invoices have $0 tax - please review",
            "invoices":[{"name":r["name"],"customer":r["partner_id"][1] if r.get("partner_id") else "N/A",
                "date":r.get("invoice_date",""),"amount":r.get("amount_untaxed",0),
                "tax":r.get("amount_tax",0),"total":r.get("amount_total",0),
                "payment_state":r.get("payment_state","")} for r in records]}

# ─────────────────────────────────────────────
# Tools
# ─────────────────────────────────────────────

TOOLS = [
    {
        "name": "odoo_search",
        "description": "Search Odoo data. Common models: sale.order, product.product, res.partner, account.move, repair.order, stock.quant, purchase.order, account.payment. For stock always add [\"location_id.usage\",\"=\",\"internal\"]. For products use ilike on both name and default_code with OR logic.",
        "input_schema": {"type":"object","properties":{"model":{"type":"string"},"domain":{"type":"array"},"fields":{"type":"array","items":{"type":"string"}},"limit":{"type":"integer","default":2000},"order":{"type":"string","default":"id desc"}},"required":["model","domain","fields"]}
    },
    {
        "name": "odoo_fields",
        "description": "List available fields for any Odoo model.",
        "input_schema": {"type":"object","properties":{"model":{"type":"string"}},"required":["model"]}
    },
    {
        "name": "get_monthly_tax",
        "description": "Get accurate monthly tax report.",
        "input_schema": {"type":"object","properties":{"year":{"type":"integer"},"month":{"type":"integer"}},"required":["year","month"]}
    },
    {
        "name": "get_quarterly_tax",
        "description": "Get accurate quarterly tax report with monthly breakdown.",
        "input_schema": {"type":"object","properties":{"year":{"type":"integer"},"quarter":{"type":"integer"}},"required":["year","quarter"]}
    },
    {
        "name": "get_monthly_sales",
        "description": "Get monthly sales report grouped by salesperson for commission calculation. Returns each salesperson's invoice count, credit note count, and net sales amount. Always use for sales/commission queries.",
        "input_schema": {"type":"object","properties":{"year":{"type":"integer"},"month":{"type":"integer"}},"required":["year","month"]}
    },
    {
        "name": "get_missing_tax",
        "description": "Find CA invoices with zero tax amount.",
        "input_schema": {"type":"object","properties":{"year":{"type":"integer"},"month":{"type":"integer"}},"required":["year","month"]}
    },
    {
        "name": "search_knowledge",
        "description": "Search the internal knowledge base — includes websites AND uploaded documents (service manuals, spec sheets, product manuals, warranty docs, employee handbook). ALWAYS use this first for ANY product question, maintenance, repair, troubleshooting, error codes, procedures, or specs. The results contain the ACTUAL TEXT from the documents — read and use this content directly in your answer. Use doc_name to filter by a specific document when you know which file to look in.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Search query — use specific terms like model number, symptom, part name, procedure"},"top_k":{"type":"integer","default":10,"description":"Number of chunks to retrieve, default 10, max 20"},"doc_name":{"type":"string","description":"Optional: filter by document name (partial match). E.g. 'Gas Fryer' or 'FLM-F3' or 'warranty'"},"category":{"type":"string","description":"Optional: service_manual, product_manual, spec_sheet, employee_handbook, after_sales, warranty, general"}},"required":["query"]}
    },
    {
        "name": "list_documents",
        "description": "List all uploaded documents in the knowledge base with their names, categories, and descriptions. Use this when: (1) user asks what documents/manuals are available, (2) search_knowledge returns no results and you want to check if a relevant document exists under a different name, (3) user asks for a file download. Returns document names and download links.",
        "input_schema": {"type":"object","properties":{"category":{"type":"string","description":"Optional filter: service_manual, product_manual, spec_sheet, employee_handbook, after_sales, warranty, general"}},"required":[]}
    },
    {
        "name": "web_search",
        "description": "Search the live internet for up-to-date information. Use when: (1) user explicitly asks to search online/Google it/看网上, (2) question needs current info not in knowledge base (news, recent prices, competitor info, latest model releases), (3) you don't have enough info from search_knowledge to answer troubleshooting/repair questions. Prefer search_knowledge FIRST for Chumart/Polarman/Flamaster/ChefAsst brand questions. Returns structured results optimized for LLMs.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Search query in natural language (English recommended for better results)"},"max_results":{"type":"integer","default":5,"description":"Number of results, 3-10"}},"required":["query"]}
    },
    {
        "name": "search_documents",
        "description": "Search for specific internal documents by name or category. Use when user asks to find or download a specific file like a service manual, employee handbook, or procedure document. Returns document name, category, and download link.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Document name or keywords"},"category":{"type":"string","description":"Optional: service_manual, product_manual, spec_sheet, employee_handbook, after_sales, warranty, general"}},"required":["query"]}
    },
    {
        "name": "odoo_create_record",
        "description": "Create a new record in Odoo after user confirms. Use for: purchase.order (PO), sale.order (SO), res.partner (contact). Always search first to verify product/partner IDs, show a confirmation summary, and only call this after user explicitly says 'confirm' or '确认'. For purchase.order: vals must include partner_id. Order lines are added separately via odoo_add_order_line.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string", "description": "Odoo model: purchase.order, sale.order, res.partner etc"},
                "vals": {"type": "object", "description": "Field values to set on the new record"}
            },
            "required": ["model", "vals"]
        }
    },
    {
        "name": "odoo_add_order_line",
        "description": "Add a product line to an existing purchase.order or sale.order. ALWAYS include the sku field — it is used to verify/correct the product_id.",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_type": {"type": "string", "description": "purchase or sale"},
                "order_id": {"type": "integer", "description": "The ID of the order"},
                "product_id": {"type": "integer", "description": "Product ID from Odoo"},
                "sku": {"type": "string", "description": "Product SKU/default_code — REQUIRED for ID verification"},
                "quantity": {"type": "number", "description": "Quantity to order"},
                "price_unit": {"type": "number", "description": "Unit price (optional, will use product default if 0)"}
            },
            "required": ["order_type", "order_id", "product_id", "quantity"]
        }
    },
    {
        "name": "odoo_confirm_order",
        "description": "Confirm a draft purchase or sale order (changes state from draft to confirmed). Only call after user explicitly requests to confirm/submit the order.",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_type": {"type": "string", "description": "purchase or sale"},
                "order_id": {"type": "integer", "description": "The order ID to confirm"}
            },
            "required": ["order_type", "order_id"]
        }
    },
    {
        "name": "odoo_update_record",
        "description": "Update fields on an existing Odoo record. Use for modifying existing orders, contacts, etc. Requires explicit user confirmation before calling.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "record_id": {"type": "integer"},
                "vals": {"type": "object", "description": "Fields to update"}
            },
            "required": ["model", "record_id", "vals"]
        }
    },
    {
        "name": "get_po_with_so_links",
        "description": "Get a Purchase Order's product lines AND find all related Sales Orders containing those products. Use this for ANY question like 'show me PO X and its related SOs', 'which customers bought products from PO X', 'PO to SO analysis'. This tool handles all the table joins correctly — do NOT use odoo_search manually for this type of query. When the user asks about a specific salesperson's related SOs or 'what sold' from a PO, pass only_with_so=true to hide product lines that had no matching SO (cleaner output).",
        "input_schema": {
            "type": "object",
            "properties": {
                "po_name": {"type": "string", "description": "PO number exactly as shown (e.g. 'P00461')"},
                "days_back": {"type": "integer", "default": 30, "description": "How many days back to look for related SOs (default 30)"},
                "include_all_so": {"type": "boolean", "default": False, "description": "If true, find all SOs ever, no date filter"},
                "salesperson": {"type": "string", "default": "", "description": "Filter SOs by salesperson name (e.g. 'Alex'). Empty = all salespeople."},
                "only_with_so": {"type": "boolean", "default": False, "description": "If true, only return product lines that have at least one matching SO. Set to true when the user is asking about what actually sold, or about a specific salesperson's related SOs — this avoids cluttering the output with products that had no sales activity."}
            },
            "required": ["po_name"]
        }
    },
    {
        "name": "odoo_search_products_by_sku",
        "description": "Search multiple products by SKU (default_code) in one batch call. Returns product ID, name, SKU, price for each. Use when user provides a list of SKUs to build purchase orders.",
        "input_schema": {
            "type": "object",
            "properties": {
                "skus": {"type": "array", "items": {"type": "string"}, "description": "List of SKU codes"}
            },
            "required": ["skus"]
        }
    },
    {
        "name": "odoo_get_product_vendors",
        "description": "Get all vendors for a list of products from product.supplierinfo. Returns each product with ALL its vendors including supplierinfo_id (the record ID in product.supplierinfo), vendor_id, vendor_name, price, min_qty. If a product has multiple vendors, AI must ask user to choose. Use before creating any purchase order, or before updating vendor prices.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_ids": {"type": "array", "items": {"type": "integer"}, "description": "List of product IDs"}
            },
            "required": ["product_ids"]
        }
    },
    {
        "name": "odoo_update_vendor_price",
        "description": "Update vendor pricelist (product.supplierinfo) prices for a batch of SKUs under ONE vendor. ALWAYS use this tool — NOT odoo_update_record — for any vendor price update request. This tool resolves product IDs from SKUs, finds or creates the correct supplierinfo record for the given vendor, and writes the new price. AI MUST NOT pass any record_id — the backend looks it up. If no existing vendor record exists for a product, a new one is created automatically. Requires explicit user confirmation before calling.",
        "input_schema": {
            "type": "object",
            "properties": {
                "vendor_name": {"type": "string", "description": "Vendor display name as shown in Odoo res.partner (e.g. 'Thunder Group')"},
                "updates": {
                    "type": "array",
                    "description": "List of {sku, new_price} updates",
                    "items": {
                        "type": "object",
                        "properties": {
                            "sku": {"type": "string", "description": "Product SKU / default_code (e.g. 'ALFN001')"},
                            "new_price": {"type": "number", "description": "New price (must be >= 0)"}
                        },
                        "required": ["sku", "new_price"]
                    }
                }
            },
            "required": ["vendor_name", "updates"]
        }
    },
    {
        "name": "odoo_find_recent_purchases_by_skus",
        "description": "BATCH TOOL — USE THIS for any question about 'which of these SKUs have we purchased recently / at what prices / from which vendors'. Takes up to 500 SKUs and a date window, returns per-SKU purchase stats in ONE tool call (replaces the old pattern of chaining 5-8 odoo_search calls). Returned per-SKU info: purchase_count, total_qty, vendors, min/max/last price, last_po, last_date, all_po_names. Plus an overall summary: total_skus_with_purchases, total_pos_involved, total_amount. Cancelled POs excluded by default. CALL THIS INSTEAD OF manually searching purchase.order and purchase.order.line for SKU-based purchase history questions.",
        "input_schema": {
            "type": "object",
            "properties": {
                "skus": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "List of SKUs / default_codes to look up (1-500)"
                },
                "days_back": {
                    "type": "integer",
                    "description": "How many days back from today to search (default 120). Ignored if since_date is provided."
                },
                "since_date": {
                    "type": "string",
                    "description": "Explicit cutoff date YYYY-MM-DD (e.g. '2025-12-31'). Overrides days_back if given."
                },
                "include_cancelled": {
                    "type": "boolean",
                    "description": "Include cancelled POs. Default false — cancelled POs are normally excluded."
                }
            },
            "required": ["skus"]
        }
    },
    {
        "name": "odoo_match_payment_to_customer",
        "description": "MATCH A RECEIVED PAYMENT to customer invoices/SOs. USE THIS for any question like '我收到一笔 $X 的钱 不知道是哪个客户 / 哪个发票对应', 'which customer sent $X', 'find invoice for this payment', 'who paid $X on [date]'. Multi-phase search: (1) account.payment + in_payment/partial invoices, (2) in_payment invoices checking total/residual/paid-portion, (3) uninvoiced SOs (customer paid before invoice created, or shipping supplement), (4) paid invoices (possible mis-reconciliation). Amount tolerance $20 covering tax fluctuations. Returns ranked candidates with match_score. ALWAYS use this INSTEAD OF multiple manual odoo_search calls. Read-only.",
        "input_schema": {
            "type": "object",
            "properties": {
                "amount": {"type": "number", "description": "The USD amount received (e.g. 3034.98). Required."},
                "date": {"type": "string", "description": "Date money was received, YYYY-MM-DD. Optional but highly recommended (narrows search)."},
                "date_window_days": {"type": "integer", "description": "Search ±N days around date. Default 14. Use 7 for tight matching, 30 for loose."},
                "customer_hint": {"type": "string", "description": "Customer name from bank memo / Zelle note, if available (e.g. 'CARLOS RODRIGUEZ' or 'Anna Cafe'). Improves match accuracy significantly."},
                "max_candidates": {"type": "integer", "description": "Max candidates to return. Default 10."}
            },
            "required": ["amount"]
        }
    },
    {
        "name": "odoo_create_bulk_po",
        "description": "Create multiple purchase orders at once, one per vendor. Only call after user has confirmed the full plan. Each PO has one vendor and multiple product lines. CRITICAL: partner_id MUST come from odoo_get_product_vendors' vendor_id field (never user ID, never partner_name string). product_id MUST come from odoo_search_products_by_sku's product_id field (never invented, never product_tmpl_id).",
        "input_schema": {
            "type": "object",
            "properties": {
                "purchase_orders": {
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "partner_id": {"type": "integer", "description": "Vendor partner ID — MUST be vendor_id from odoo_get_product_vendors"},
                            "partner_name": {"type": "string"},
                            "lines": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "sku": {"type": "string", "description": "SKU (default_code) — STRONGLY PREFERRED. Copy from user's request or odoo_search_products_by_sku. Backend will resolve product_id from this if provided."},
                                        "product_id": {"type": "integer", "description": "Product ID — MUST be product_id returned by odoo_search_products_by_sku. If unsure, provide sku instead."},
                                        "product_name": {"type": "string", "description": "Product name from odoo_search_products_by_sku result"},
                                        "quantity": {"type": "number"},
                                        "price_unit": {"type": "number"}
                                    }
                                }
                            }
                        }
                    },
                    "description": "List of POs to create, one per vendor"
                }
            },
            "required": ["purchase_orders"]
        }
    },
    {
        "name": "odoo_restock_analysis",
        "description": "Analyze stock moves (outgoing) over a period and compare with current inventory to determine which products need restocking. Groups results by brand with urgency levels based on brand-specific lead times. Use this for ANY question like '哪些产品需要补货', 'restock analysis', '补货分析', 'what needs reordering', '库存预警'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "days_back": {"type": "integer", "default": 30, "description": "Number of days of outgoing history to analyze (default 30)"},
                "brand_filter": {"type": "string", "default": "", "description": "Optional: filter to a specific brand name (e.g. 'Polarman', 'Flamaster'). Empty = all brands."},
                "urgency_filter": {"type": "string", "default": "", "enum": ["", "out_of_stock", "urgent", "reorder", "ok", "no_movement"], "description": "Optional: filter results to a specific urgency level only"}
            },
            "required": []
        }
    },
    {
        "name": "odoo_create_invoice_from_so",
        "description": "Create an invoice from a Sales Order. USE THIS when user says 'release S04100', 'create invoice for CMT12345', '给这个订单开票'. For Shopify (#CMT) and Amazon (AMZ) orders, the SO existing in Odoo means payment is confirmed — can proceed directly. For normal orders (S-prefix), require explicit payment confirmation first. Returns the created invoice ID and name.",
        "input_schema": {
            "type": "object",
            "properties": {
                "so_name": {"type": "string", "description": "Sales Order name/number (e.g. 'S04100', '#CMT12345', 'AMZ-12345')"},
                "payment_method": {"type": "string", "description": "Payment method selection value for x_payment_method field. Common values: Cash, Stripe, Zelle, Shopify Payment, Amazon Payment, Square, Combo(Cash+Zelle), etc. For Shopify orders default to 'Shopify Payment'. For Amazon default to 'Amazon Payment'."},
            },
            "required": ["so_name"]
        }
    },
    {
        "name": "odoo_register_payment",
        "description": "Register payment on an invoice (marks it as paid). Call AFTER odoo_create_invoice_from_so. Requires the invoice_id returned from that tool. Journal mapping: Cash payments → 'Cash' journal, Amazon → 'Amazon PLAT BUS CHECKING' journal, everything else (Stripe/Zelle/Square/Shopify) → 'Revenue and COGS' journal. Stripe invoices may already be auto-registered — check payment_state first.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_id": {"type": "integer", "description": "The account.move ID of the invoice to register payment on"},
                "journal_name": {"type": "string", "description": "Journal name: 'Cash', 'Revenue and COGS', or 'Amazon PLAT BUS CHECKING'"},
                "amount": {"type": "number", "description": "Payment amount. Usually the invoice total. Optional — if omitted, pays the full invoice amount."},
                "payment_date": {"type": "string", "description": "Payment date YYYY-MM-DD. Optional — defaults to today."}
            },
            "required": ["invoice_id", "journal_name"]
        }
    },
    {
        "name": "odoo_export_invoice_pdf",
        "description": "Export an invoice as PDF. Returns a download URL. Use after creating and registering payment on an invoice. The PDF can be downloaded or sent to a printer.",
        "input_schema": {
            "type": "object",
            "properties": {
                "invoice_id": {"type": "integer", "description": "The account.move ID to export as PDF"}
            },
            "required": ["invoice_id"]
        }
    },
    {
        "name": "check_so_payment_status",
        "description": (
            "Check whether a Sales Order has received payment(s) and whether it can be released (invoiced). "
            "Use this when user asks 'has SO X been paid?', 'did we receive payment for X?', or before releasing a SO. "
            "Returns: SO state (must be 'sale' or 'done' to release), list of received payments from Stripe/Square/Zelle, "
            "total received amount, SO total amount, whether already invoiced, and whether eligible for release."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "so_name": {"type": "string", "description": "Sales Order name like S04301 or CMT12345"}
            },
            "required": ["so_name"]
        }
    },
    {
        "name": "release_so",
        "description": (
            "Release a Sales Order = create invoice + register payment + export PDF. "
            "USE THIS ONLY AFTER user explicitly confirms (e.g., '确认', 'yes', 'release', '开票'). "
            "Pre-conditions checked automatically: (1) SO state must be 'sale' or 'done' (confirmed); "
            "(2) Total received payments must be >= SO amount; (3) SO must not already have a posted/paid invoice. "
            "If preconditions fail, returns error explaining what's wrong. "
            "Payment method auto-detected from received_payments: single channel = 'Stripe'/'Zelle'/'POS Machine'; "
            "multiple = 'Combo(Stripe+Zelle)'. Stripe-only payments skip register_payment (Odoo handles automatically). "
            "ALWAYS call check_so_payment_status first and show user the details, then ask for confirmation before calling this."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "so_name": {"type": "string", "description": "Sales Order name to release"}
            },
            "required": ["so_name"]
        }
    },
    {
        "name": "create_reminder",
        "description": "Schedule a reminder that notifies the user via email/SMS/voice call at a specified time. Use when user says '提醒我...', 'remind me...', '下个月X号提醒我'. fire_at must be ISO datetime (naive = LA time). Default channel is email. Add 'sms' for text, 'call' for phone call. CRITICAL: Always compute the date from the 'today' value in your system prompt — do NOT guess. '明天/tomorrow' = today's date + 1 day (use the explicit 'tomorrow' value in your system prompt). When user mentions a weekday like '下周一', count forward from today's weekday. Always confirm the resolved date in your reply (e.g. '已设置 4月28日(周一) 09:00 的提醒')",
        "input_schema": {
            "type": "object",
            "properties": {
                "content": {"type": "string", "description": "What to remind about"},
                "fire_at": {"type": "string", "description": "When to fire, ISO format like '2026-05-24T09:00:00'. Naive = LA time. If user only gives date, default time to 09:00."},
                "channels": {"type": "array", "items": {"type": "string", "enum": ["email", "sms", "call"]}, "description": "Notification channels. Default ['email']. Add 'sms' if user says '短信', 'call' if user says '打电话'/'重要'."}
            },
            "required": ["content", "fire_at"]
        }
    },
    {
        "name": "list_reminders",
        "description": "List the user's scheduled reminders. Use when user asks 'what reminders do I have' / '我有什么提醒'.",
        "input_schema": {
            "type": "object",
            "properties": {
                "include_fired": {"type": "boolean", "default": False, "description": "If true, include past fired reminders too."}
            },
            "required": []
        }
    },
    {
        "name": "cancel_reminder",
        "description": "Cancel a pending reminder by id. Get id from list_reminders first. To cancel by content (e.g. 'cancel my X reminder'), first call list_reminders to find the matching id.",
        "input_schema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"]
        }
    },
    {
        "name": "update_reminder",
        "description": (
            "Update an existing reminder's time and/or content. Use when user says '改一下我的X提醒到Y时间', "
            "'change my reminder to ...', 'reschedule the X reminder'. "
            "ALWAYS call list_reminders first to get the correct id (don't guess). "
            "If multiple pending reminders match the user's description, ask which one. "
            "DO NOT use create_reminder + cancel_reminder as a workaround — use this tool to atomically update."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "id": {"type": "integer", "description": "Reminder id from list_reminders"},
                "fire_at": {"type": "string", "description": "New time, ISO datetime (naive = LA time). Optional — only pass if changing time."},
                "content": {"type": "string", "description": "New content. Optional — only pass if changing content."}
            },
            "required": ["id"]
        }
    },
    {
        "name": "create_event",
        "description": "Record a calendar event/行程. Use when user says '帮我记一下下周三开会', 'I have a meeting on Friday'. This does NOT send a reminder — call create_reminder separately if user wants notification.",
        "input_schema": {
            "type": "object",
            "properties": {
                "title": {"type": "string"},
                "start_at": {"type": "string", "description": "ISO datetime, naive = LA time"},
                "end_at": {"type": "string", "description": "Optional end time"},
                "notes": {"type": "string"},
                "location": {"type": "string"}
            },
            "required": ["title", "start_at"]
        }
    },
    {
        "name": "list_events",
        "description": "List user's calendar events in a date range. Use when user asks '我明天有什么安排', 'what's on my schedule'. Defaults to next 30 days.",
        "input_schema": {
            "type": "object",
            "properties": {
                "date_from": {"type": "string", "description": "ISO date, start of range"},
                "date_to": {"type": "string", "description": "ISO date, end of range"}
            },
            "required": []
        }
    },
    {
        "name": "delete_event",
        "description": "Delete a calendar event by id. Get id from list_events first.",
        "input_schema": {
            "type": "object",
            "properties": {"id": {"type": "integer"}},
            "required": ["id"]
        }
    },
    {
        "name": "set_my_contact",
        "description": "Save user's preferred email or phone for receiving reminders. Phone must be E.164 format (e.g. +13105551234). Falls back to Odoo login email and partner mobile if not set.",
        "input_schema": {
            "type": "object",
            "properties": {
                "email": {"type": "string"},
                "phone": {"type": "string", "description": "E.164 format, e.g. +13105551234"}
            },
            "required": []
        }
    }
]

def get_system_prompt(role: str = "guest", user_name: str = "", user_id: int = 0, free_mode: bool = False, memories: list = []):
    now_dt = datetime.datetime.now(LA_TZ)
    today_la = now_dt.date()
    today = today_la.strftime("%Y年%m月%d日")
    now_la = now_dt.strftime("%Y-%m-%d %H:%M %Z")
    weekday_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    weekday_cn = ["周一", "周二", "周三", "周四", "周五", "周六", "周日"]
    dow_en = weekday_names[now_dt.weekday()]
    dow_cn = weekday_cn[now_dt.weekday()]
    # Pre-compute tomorrow & day-after explicitly so AI doesn't have to guess
    tomorrow_la = today_la + datetime.timedelta(days=1)
    tomorrow_str = tomorrow_la.strftime("%Y年%m月%d日")
    tomorrow_dow_cn = weekday_cn[tomorrow_la.weekday()]
    tomorrow_dow_en = weekday_names[tomorrow_la.weekday()]
    perms = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["guest"])

    # Build permission-specific rules
    finance_rules = ""
    if perms["can_see_finance"]:
        finance_rules = f"""
FINANCIAL REPORT RULES (you have access):
- Monthly tax -> get_monthly_tax
- Quarterly tax -> get_quarterly_tax
- Monthly sales / commission base -> get_monthly_sales
- CA invoices missing tax -> get_missing_tax
- Can query account.move, account.payment with full access

COMMISSION REPORT RULES (IMPORTANT — follow this exactly):
When user mentions "commission", "提成", "销售提成", "佣金", or any combination like "X月commission", "commission统计":
1. Extract year and month from the request (e.g. "26年3月" = 2026-03, "3月" = current year March)
2. Check if user is asking for a SPECIFIC salesperson (e.g. "Gene的4月commission", "查下Alex的提成")
3. Call get_monthly_sales with the correct year and month
4. Present results based on whether it's for ALL salespeople or ONE specific person:

=== ALL SALESPEOPLE (default) ===

PART A — 销售员销售统计（按销售员）:
| 销售员 | 发票数 | 退款数 | 发票金额 | 退款金额 | 净销售额(税前) |
Use the by_salesperson data from the tool result. Show ALL salespeople. Add a total row at the bottom.
The "合计" row MUST use commission_base values directly — NEVER sum up individual rows yourself (rounding errors).
Use $ with commas for all amounts. Copy numbers directly from tool response.

PART B — Commission Base 汇总:
Show commission_base values: net_sales_excl_tax (净销售额税前), net_tax (销售税), net_sales_incl_tax (净销售额税后), invoice_count (发票总数), credit_note_count (退款单总数).

Do NOT output any download links or export buttons in your response — the frontend automatically generates a Download Excel button when it detects commission data. Just end with the data.

=== SPECIFIC SALESPERSON (e.g. "Gene的4月commission") ===

IMPORTANT: Still call get_monthly_sales(year, month) — do NOT query sale.order or account.move directly.
Then filter the by_salesperson array from the result to find that person's data.

PART A — 该销售员汇总:
Show only that salesperson's row: 发票数, 退款数, 发票金额, 退款金额, 净销售额(税前)

Do NOT show the detail transaction list — it can be very long and cause lag.
Instead, tell the user how many invoices/credit notes there are, and suggest they can download the full list via the Excel button below.
Do NOT output any download links — the frontend auto-generates them.
Do NOT use odoo_search to query sale.order or account.move for commission data — always use get_monthly_sales."""
    else:
        finance_rules = """
FINANCIAL RULES (NO ACCESS):
- You do NOT have permission to view financial reports, tax data, invoices, or payment information
- If asked about finance/tax/invoices, politely say you don't have access and suggest contacting the finance team
- Do NOT call get_monthly_tax, get_quarterly_tax, get_monthly_sales, get_missing_tax"""

    sales_rules = ""
    if perms["can_see_all_sales"]:
        sales_rules = """
SALES RULES (full access):
- Can view all sales orders and invoices for all salespeople"""
    else:
        own_filter = f'["invoice_user_id","=",{user_id}]' if user_id else '["invoice_user_id","=",false]'
        sales_rules = f"""
SALES RULES (own data only):
- Current user: {user_name} (uid={user_id})
- Can ONLY view own sales orders: always add filter ["user_id","=",{user_id}] to sale.order queries
- Can ONLY view own invoices: always add filter {own_filter} to account.move queries
- Cannot view other salespeople's data
- Cannot view commission reports for others
- CANNOT query account.payment at all — payment/收款 data is restricted to finance
- CANNOT do payment matching / 对账 / reconciliation — if asked, reply: "抱歉，我没有权限查询付款和对账信息，请联系财务部门。"
- CANNOT query purchase.order or purchase.order.line — purchasing data is restricted
- If the user asks about ANY topic you don't have permission for, reply briefly: "抱歉，我没有权限查询该信息。" Do NOT try to work around the restriction with odoo_search."""

    cost_rules = ""
    if not perms["can_see_cost"]:
        cost_rules = """
COST/MARGIN RULES (NO ACCESS):
- NEVER show standard_price, cost, or margin fields
- If asked about cost or margin, say this information is restricted
- Only show sales price (list_price), not cost price (standard_price)"""
    else:
        cost_rules = """COST/MARGIN RULES (can view cost data):
- NEVER proactively mention profit/margin/利润 unless the user explicitly asks
- EXCEPTION: If you notice cost price is HIGHER than selling price (negative margin), proactively warn the user — this indicates a pricing problem
- When user asks about cost or margin, you may show it
- Do NOT calculate or display profit margins in PO-to-SO analysis unless asked"""

    inventory_rules = ""
    if perms["can_see_inventory"]:
        inventory_rules = "INVENTORY: Can query stock.quant and view inventory levels."
    else:
        inventory_rules = "INVENTORY: No access to inventory data."

    # Build memory block
    memory_block = ""
    if memories:
        memory_block = "\n\n【关于你的记忆】\n" + "\n".join(f"- {m}" for m in memories)

    # Free mode: ONLY when admin explicitly toggles it on
    if role == "admin" and free_mode:
        return f"""今天是{today}（{dow_cn}/{dow_en}），当前时间(洛杉矶): {now_la}。
明天是{tomorrow_str}（{tomorrow_dow_cn}/{tomorrow_dow_en}）。
你是 Chumart AI，Chumart 管理员的专属私人助手。
你支持中英文，用用户的语言回复。{memory_block}

【你的能力没有限制】
- 你既是企业AI助手（连接Odoo ERP和产品知识库），也是管理员的私人助手
- 可以回答任何话题：商业分析、Odoo数据查询、产品知识、娱乐、算命、星座、趣味问答、创意写作、日常建议等等
- 遇到算命/星座/塔罗等娱乐话题，以轻松有趣的方式回应，加入一些幽默感
- 不需要把每个问题都往Odoo上靠，自然对话即可

【Odoo 工具使用规则】
- 涉及产品/规格/价格/维修/故障排除时：先调用 search_knowledge（包含网站内容和上传的文档如service manual）
- 如果用户想找或下载某个文件：用 search_documents 工具
- 涉及财务报表：get_monthly_tax / get_quarterly_tax / get_monthly_sales / get_missing_tax
- 涉及库存/客户/订单：用 odoo_search，company_id=1，stock加 location_id.usage=internal
- 涉及产品搜索：name 和 default_code 都用 ilike，OR 逻辑

【数据展示】
- 财务数字用 $ 加千位分隔符
- 表格数据用 Markdown 表格格式 | col | col |
- 计算规则：财务汇总永远直接引用工具返回的预计算值，不要自己重新加总"""

    # Admin in work mode = same as finance (full access, enterprise assistant)
    if role == "admin":
        role = "finance"  # treat admin as finance for work mode prompt

    # Build memory block for work mode
    memory_block = ""
    if memories:
        memory_block = f"\n\nUSER MEMORY (personalization context):\n" + "\n".join(f"- {m}" for m in memories)

    return f"""今天是{today}（{dow_cn}/{dow_en}），当前时间(洛杉矶): {now_la}。
明天是{tomorrow_str}（{tomorrow_dow_cn}/{tomorrow_dow_en}）。
You are Chumart Assistant, an enterprise AI assistant.
You support both English and Chinese - reply in the same language the user uses.

CURRENT USER: {user_name} | ROLE: {perms['label']} | UID: {user_id}{memory_block}

KNOWLEDGE BASE RULES (MOST IMPORTANT):
- For ANY product question, spec, price, installation, maintenance, repair, troubleshooting, error codes, or company policy: ALWAYS call search_knowledge first
- The knowledge base contains: website content (chumartusa.com, polarmanusa.com etc.) AND uploaded internal documents (service manuals, employee handbook, after-sales procedures, warranty docs)
- If user asks to find or download a specific document, use search_documents tool
- Never answer product/maintenance/repair questions from memory — always search first

{finance_rules}

{sales_rules}

{cost_rules}

{inventory_rules}

GENERAL ODOO RULES:
- Always include date filters when user mentions a time period
- For account.move, sale.order, purchase.order, account.payment, res.partner, crm.lead, repair.order, stock.picking: filtered by company_id=1
- For product.product, stock.quant: do NOT add company_id filter
- For stock queries always add ["location_id.usage","=","internal"]
- For product search use ilike on both name and default_code with OR logic
- CONTEXT LIMIT: Never set limit > 200 on any query. If you need more data, break into multiple smaller queries (e.g. by date range or by product batch). Large result sets will crash the system.
- When comparing a PDF/file with Odoo data: extract the specific SKUs from the file FIRST, then query Odoo using only those SKUs. Do NOT pull all records from a vendor and try to match — that will exceed context limits.

STANDARD QUERY PATTERNS (follow these exactly):

"Show PO X details" / "查下PO X的清单" / "PO X 的产品":
→ Step 1 ONLY: Use odoo_search to query PO header + PO lines + product details (SKU, name, qty, price)
→ Show the product list in a clean table
→ At the end, ASK the user: "需要查一下这些产品最近的相关销售订单吗？"
→ Do NOT automatically query SOs — wait for user to ask

"PO X related SOs" / "查下这些产品的SO" / "which customers bought from PO X" / user says yes to SO query:
→ NOW use get_po_with_so_links(po_name="P00461") — this handles all table joins correctly
→ Adjust days_back if user specifies a time range (e.g. "last 7 days" → days_back=7)
→ Use include_all_so=true if user wants all historical SOs
→ IMPORTANT — use only_with_so=true when:
    - User mentions a specific salesperson ("Alex相关的 SO", "what Alex sold", "Mike 卖过的")
    - User asks "what sold from this PO" / "哪些卖出去了" / "实际成交的"
    - User is doing PO-to-SO conversion analysis
  When only_with_so=true is used, the tool hides products that had zero matching SOs (reduces noise). Display only the returned rows — do NOT separately list the products that were filtered out. The summary line already tells you how many were hidden.
→ Leave only_with_so=false (default) ONLY when user explicitly wants the full PO breakdown including unsold items.

CRITICAL: Odoo 17 does NOT reliably support 2-level relational filters like "order_id.date_order" or "order_id.company_id" on order lines — these may silently return wrong/empty results. Always filter dates at the order level in a separate query.
CRITICAL: NEVER use "order_id.id" as a filter — use "order_id" directly. Example: [["order_id","in",[453,447]]] NOT [["order_id.id","in",[453,447]]]

"Which SOs contain SKU X in the last N days?"
→ Step 1: Get product_id: odoo_search(product.product, [["default_code","=","SKU"]], ["id","name"])
→ Step 2: odoo_search(sale.order.line, [["product_id","in",[pid]]], ["order_id","product_id","product_uom_qty","price_unit"], limit=500)
→ Step 3: Extract order_ids from step 2, then filter by date:
   odoo_search(sale.order, [["id","in",[order_ids]],["date_order",">=","DATE"],["company_id","=",1]], ["name","partner_id","date_order","state","amount_total"])
→ If step 3 returns empty → say "no SOs found in this period" — NEVER invent results

"Which POs contain SKU X?"
→ Step 1: odoo_search(product.product, [["default_code","=","SKU"]], ["id"])
→ Step 2: odoo_search(purchase.order.line, [["product_id","in",[pid]]], ["order_id","product_id","product_qty","price_unit"])
→ Step 3: odoo_search(purchase.order, [["id","in",[order_ids]],["company_id","=",1]], ["name","partner_id","date_order","state"])

"PO from vendor X → what products → which SOs bought them recently?"
→ Step 1: odoo_search(purchase.order, [["partner_id.name","ilike","Thunder Group"],["date_order",">=","DATE"],["company_id","=",1]], ["name","partner_id","date_order","id"])
→ Step 2: odoo_search(purchase.order.line, [["order_id","in",[po_ids]]], ["product_id","product_qty","order_id"])
→ Step 3: Collect all product_ids from step 2
→ Step 4: odoo_search(sale.order.line, [["product_id","in",[pid_list]]], ["order_id","product_id","product_uom_qty"], limit=500)
→ Step 5: odoo_search(sale.order, [["id","in",[so_ids]],["date_order",">=","DATE"],["company_id","=",1]], ["name","partner_id","date_order","state","amount_total"])
→ If steps 4/5 return empty → "no matching SOs found" — do NOT fabricate data

When showing financial data: use $ with commas, be precise.
When helping sales: be specific, cite model numbers, give concrete talking points.
Only use markdown tables when data is genuinely tabular (multi-row comparisons, reports, lists with multiple columns). Do NOT use tables for single items, simple answers, or narrative responses.
CALCULATION RULES: When summing financial data from tool results, always use the exact numbers returned by the tool. Never recalculate totals yourself — use the pre-calculated values from the data (commission_base.net_sales_excl_tax etc). If showing a summary, copy the numbers directly from the tool response.
- For commission reports: the "合计" row MUST use commission_base values directly, NOT sum up the by_salesperson rows yourself. Your arithmetic may have rounding errors.
- credit_amount and net_amount_untaxed come from the tool — display them as-is, never recalculate.

DATA ACCURACY RULES (CRITICAL — violations damage user trust):
- NEVER invent, guess, or fill in ANY field value — customer names, order numbers, SKUs, prices, addresses, phone numbers, MODEL NUMBERS, etc.
- NEVER supplement tool results with product data from training knowledge. If search_knowledge returns 5 Polarman models, output exactly those 5 — not 8 with 3 "from memory".
- If a tool returns an error → tell the user "查询出错" and show the error message. NEVER fabricate data to fill in for a failed query.
- If a field is empty or null in the tool result → say "not found" or "not set in Odoo", do NOT substitute a plausible-sounding value
- Customer names MUST come from the actual tool query result (partner_id field in sale.order/purchase.order). Never use a company name you "think" is the customer.
- If the first query doesn't return the expected field, run another query with the correct fields — do NOT guess
- When showing order details: ONLY show fields that were actually returned by the tool. If you didn't query for a field, don't show it.
- CRITICAL — id vs name: The database "id" (integer like 461) is NOT the same as the order "name" (like P00461 or S04270). NEVER assume id=461 means the order is named "P00461". Always query the "name" field explicitly and use that. When user says "P00461", search by [["name","=","P00461"]] not by id.

WRITE OPERATION RULES:
- Roles that CAN write to Odoo: admin, purchase
- NEVER execute write operations without explicit confirmation ("confirm", "确认", "yes", "go ahead")
- After creating, always show Odoo direct link from tool result
- If user role lacks can_write_odoo, politely decline

BULK PURCHASE ORDER WORKFLOW (when user gives a list of SKUs):
Follow these steps in order:

STEP 0 — If user provides SKUs WITHOUT quantities:
  After Step 1 and Step 2 complete, list all found products WITH their SKUs and ask user to fill in quantities.
  Format it so user only needs to type numbers:
  "请为以下产品填写采购数量：
  1. SLTHUT507 — 12" Coiled Spring Utility Tong → 数量: __
  2. PLMC064CL — 105g Scrubber Ball → 数量: __
  ..."
  NEVER show an empty template like "SKU1: qty". Always pre-fill the actual SKUs you already found.
  If user then replies with just numbers (e.g. "4, 5, 6"), match them to the SKUs in the order you listed.

STEP 1 — Search all products at once:
  Call odoo_search_products_by_sku with all SKUs in one call.
  IMPORTANT: Always use the product_id returned from odoo_search_products_by_sku — never use product_tmpl_id or any other ID.
  If any SKU not found, tell user immediately and ask how to proceed.

STEP 2 — Get all vendors:
  Call odoo_get_product_vendors with all found product IDs in one call.
  Check each product:
  - no_vendor=true → tell user this product has no vendor configured, skip or ask
  - has_multiple_vendors=true → LIST all vendors with their prices, ask user to choose ONE for that product
  - Only one vendor → auto-assign, no need to ask

STEP 3 — Show grouped PO plan:
  Group products by their chosen vendor. Show a clear table:
  "📋 PO Plan — X orders will be created:

  **PO #1 → [Vendor A]**
  | SKU | Product | Qty | Unit Price |
  |-----|---------|-----|------------|
  | ... | ...     | ... | ...        |

  **PO #2 → [Vendor B]**
  | SKU | Product | Qty | Unit Price |
  ...

  Total: X POs, Y line items
  Reply '确认' to create all, or tell me what to change."

STEP 4 — Execute only after confirmation:
  Call odoo_create_bulk_po with the full plan.
  IMPORTANT: partner_id in purchase_orders must be the res.partner ID from vendor info (from odoo_get_product_vendors vendor_id field), NOT a user ID or product ID.
  Report results: PO names (e.g. P00442) + Odoo links for each created PO.
  If any line_errors exist, tell user which products failed and why.

ORDER/INVOICE STATE FILTERING (APPLIES TO ALL QUERIES):
When searching or aggregating purchase orders, sale orders, invoices, or stock pickings, ALWAYS exclude CANCELLED records by default. Cancelled records do not represent real business activity and should not pollute user-facing counts, totals, or lists.

Note: draft, sent (quotation/RFQ), and in-progress states ARE normal business activity and should be INCLUDED by default. Only cancelled records are filtered out.

DEFAULT STATE FILTERS — add these to the domain of every search, UNLESS the user explicitly asks to include cancelled ("包含取消" / "including cancelled" / "所有状态" / "all states including cancelled"):

  purchase.order:
    ["state", "!=", "cancel"]
    (includes: draft, sent/RFQ, to approve, purchase, done — excludes only cancel)

  sale.order:
    ["state", "!=", "cancel"]
    (includes: draft, sent/quotation, sale, done — excludes only cancel)

  stock.picking:
    ["state", "!=", "cancel"]
    (includes: draft, waiting, confirmed, assigned, done — excludes only cancel)

  account.move (invoices/bills):
    [["state", "=", "posted"]]
    (Invoices are special: only "posted" = real financial activity. "draft" is an unposted draft and "cancel" is cancelled; neither hits the books.)
    Note: for existing finance report tools (get_monthly_tax, get_quarterly_tax, get_monthly_sales, get_missing_tax), the state filter is already applied server-side — do NOT re-add it.

EXAMPLES:
  User: "查下25年12月31日以来采购过的产品" (purchase history)
  → Correct domain: [["date_order", ">=", "2025-12-31"], ["company_id", "=", 1], ["state", "!=", "cancel"]]
  → Excludes cancelled POs but includes drafts, RFQs, confirmed, and done orders.

  User: "列出本月所有销售单，包括取消的" (explicit "including cancelled")
  → Correct domain: [["date_order", ">=", ...], ["company_id", "=", 1]]  — NO state filter at all (user wants cancelled too)

  User: "有多少取消的采购订单" (specifically asking about cancelled)
  → Correct domain: [..., ["state", "=", "cancel"]] — filter TO cancelled only

  User: "有哪些RFQ待审批" (looking for drafts/RFQs specifically)
  → Correct domain: [..., ["state", "in", ["draft", "sent", "to approve"]]] — filter TO drafts only

WHEN TO OMIT THE STATE FILTER:
  - User explicitly says "包含取消" / "including cancelled" / "all states including cancel"
  - The query is specifically about cancelled records ("取消的订单")

TELL THE USER WHAT YOU EXCLUDED:
In your summary, briefly note that cancelled records are excluded, so users don't wonder why counts differ from Odoo's unfiltered list view. Examples:
"（不含取消订单）"
"(excluding cancelled orders)"

WHY: Cancelled orders did not actually happen. A user asking "我们采购过什么" almost never means "including the order we cancelled." Default to real business activity. But drafts and quotations ARE real activity (we're working on them) — they should be included.

VENDOR PRICELIST UPDATES (CRITICAL WORKFLOW):
When the user wants to update vendor prices / 供应商价格 / vendor pricelist for a list of SKUs:

RULE #1 — NEVER use odoo_update_record for product.supplierinfo. You will hallucinate record IDs. Use odoo_update_vendor_price instead.
RULE #2 — NEVER guess, derive, or invent supplierinfo record_ids. The only way to write to product.supplierinfo is through odoo_update_vendor_price (which looks up the correct record internally) OR by first calling odoo_get_product_vendors and using the exact supplierinfo_id it returns.
RULE #3 — odoo_update_vendor_price handles the "vendor doesn't have a record for this product yet" case automatically by creating a new supplierinfo row. Do NOT refuse the task just because a product has no existing vendor record.

Workflow:
  STEP 1 — Extract from user input: (a) the vendor name (ask user if unclear), (b) a list of {{sku, new_price}} pairs. Count the items programmatically: len(updates).
  STEP 2 — Show the user a BRIEF preview (NOT the full table — use summary mode):
    "准备更新 **[Vendor Name]** 的供应商价格，共 **N** 个 SKU：
    | SKU | 新价格 |
    (show first 5 rows + "... 还有 X 个")
    回复 '确认' 即可执行。"
    ⚠️ Do NOT show current prices in the preview — you don't have them yet. odoo_update_vendor_price will return old vs new.
    ⚠️ Do NOT count SKUs by eyeballing a table — state the count from len(updates) in your code/logic.
  STEP 3 — After user confirms, call odoo_update_vendor_price ONCE with {{vendor_name, updates: [...]}}.
    ⚠️ CRITICAL: Do NOT call odoo_search_products_by_sku, odoo_get_product_vendors, or any other lookup tool before this step. The tool resolves all products and supplierinfo internally. Extra lookups waste 2+ minutes and add zero value.
  STEP 4 — Report results from the returned summary + per-SKU results. For each SKU show: status (updated / created / unchanged / not_found / error), old_price → new_price.

SKU PURCHASE HISTORY QUERIES (CRITICAL PERFORMANCE RULE):
When the user asks something like "have we purchased any of these SKUs recently", "what did we pay for these products", "compare PDF prices with our actual PO prices", "these SKUs in our purchase history", or uploads a price sheet / product list and asks about purchases — ALWAYS use odoo_find_recent_purchases_by_skus INSTEAD OF manually chaining odoo_search calls.

RULE — When the question involves "SKUs + purchase history":
  ✅ CORRECT: odoo_find_recent_purchases_by_skus({{"skus": [...], "since_date": "..."}})     — one call, fast
  ❌ WRONG:   odoo_search(purchase.order, ...) + odoo_search(purchase.order.line, ...)   — 5-8 calls, slow, fragile

The batch tool returns everything you need:
  - Which SKUs were bought vs not
  - How many times each was bought
  - From which vendors
  - Price statistics (min/max/last)
  - Last PO name + date for each SKU
  - Overall totals (PO count, $ amount)

USE IT WHEN:
  - User uploads a file with SKUs and asks about purchases/prices
  - User asks "X 个 SKU 最近有没有采购过"
  - User asks "我们最近采购了哪些 [category]"
  - Cross-reference: file price vs Odoo PO price for multiple SKUs
  - "Which vendors sold us [list of SKUs]"

DATE WINDOW:
  - If user specifies a date ("25年12月31日以来", "since Dec 31 2025"): pass since_date: "2025-12-31"
  - If user says "最近" / "recent" without a date: pass days_back: 120 (default, 4 months)
  - If user says "本年" / "this year": since_date: "2026-01-01"
  - If user says "去年" / "last year": since_date: "2025-01-01"

DO NOT chain this with odoo_search for the same purpose. Call it once, use the result. If you need PO line details for ONE specific PO, then use odoo_search, but for SKU-centric questions this tool is always the answer.

PAYMENT MATCHING QUERIES (CRITICAL PERFORMANCE RULE):
When the user asks any of the following patterns — ALWAYS use odoo_match_payment_to_customer INSTEAD OF chaining odoo_search calls on account.payment / account.move:
  - "我收到一笔 $X 的钱 不知道是哪个客户 / 哪个发票 / 哪个 SO"
  - "上个月收到 $X 查下对应哪张发票"
  - "这笔 $X 来自哪个客户"
  - "Zelle 里 $X 是谁付的"
  - "who paid $X", "which invoice matches this $X payment"
  - Any screenshot of a bank statement + question about matching the amount

RULE — When the question is "received money → which invoice/customer/SO":
  ✅ CORRECT: odoo_match_payment_to_customer(amount=X, date="YYYY-MM-DD", customer_hint="NAME")
  ❌ WRONG:   odoo_search(account.payment, ...) + odoo_search(account.move, ...) + ...

The tool searches in multiple phases (mirrors real reconciliation workflow):
  Phase 1: account.payment + account.move (not_paid/partial/in_payment invoices by total AND residual)
  Phase 2A: in_payment invoices — checks if total, residual, or already-paid portion matches the received amount
  Phase 2B: uninvoiced SOs — customer may have paid before invoice was created (e.g. shipping supplement)
  Phase 2C: paid invoices — possible earlier mis-reconciliation (amount close but already marked paid)
Amount tolerance is $20 to cover tax rate fluctuations and small shipping additions. Candidates are ranked by match_score considering amount closeness, date proximity, and customer name match.

EXTRACTING INPUTS FROM USER MESSAGE:
  - amount (required): the USD amount mentioned
  - date: if user says "上个月", pass the middle of last month or user-screenshot date; if user gives a date, use it exactly
  - customer_hint: if user uploaded a bank screenshot with a name (like Zelle memo "CARLOS RODRIGUEZ PINTO") or mentioned a customer name, pass it — it dramatically improves accuracy
  - date_window_days: default 14 is fine; use 30 if user is vague about timing

INTERPRETING RESULTS:
  - best_candidate.match_score >= 80 → confident: lead with "最可能是 [partner] 的 [invoice]"
  - 50 <= score < 80 → medium: present top 2-3 candidates and let user pick
  - score < 50 → low confidence: say "未找到高置信度匹配，建议提供更多信息"
  - 0 candidates → say "未在 Odoo 中找到金额 $X 附近、日期 [range] 的记录"

DO NOT fabricate matches. If no candidates returned, say so clearly. Do not invent invoice numbers or customer names.

WHEN 0 CANDIDATES RETURNED (total_candidates_found = 0):
This is a meaningful answer, not a failure. Tell the user something like:
  "在 Odoo 中没有找到金额 $X 附近、日期 ±14 天内的客户付款单或发票记录。这笔钱可能：
   1) 还没有被录入 Odoo (通常 Zelle/Wire 手工入账需要财务补录)
   2) 客户还没开发票
   3) 日期或金额和实际有偏差"
Then ask: "你能提供更多信息吗？比如客户名、准确的金额或日期。"

DO NOT say "我找到一条银行对账记录" as an answer — the user is usually asking this FROM the bank line view, so that would just echo their input.

REMINDER MANAGEMENT (提醒管理) — STRICT RULES:
- "改一下我的X提醒到Y" / "reschedule X to Y" / "把X提醒改成Y时间" → MUST use update_reminder. 
  STEPS: (1) call list_reminders, (2) find the matching reminder's id, (3) call update_reminder with id + new fire_at.
  DO NOT call create_reminder + cancel_reminder as a workaround — that creates duplicates.
- "取消我的X提醒" / "cancel my X reminder" / "取消第N个" / "delete reminder N" → 
  MANDATORY STEPS:
  (1) call list_reminders to get current id mapping
  (2) call cancel_reminder(id=X) — YOU MUST ACTUALLY CALL THIS TOOL
  (3) call list_reminders AGAIN to verify it's gone
  (4) Only then reply to user with the result
- "我有什么提醒" / "list my reminders" → list_reminders.

⚠️ CRITICAL ANTI-HALLUCINATION RULE ⚠️
DO NOT respond with "已取消" / "cancelled" / "已删除" / "已更新" UNLESS:
  (a) You actually called cancel_reminder/update_reminder in THIS turn, AND
  (b) The tool returned {"ok": true}
If you only THINK you cancelled something, you DIDN'T. Tool calls are the only way to make changes.
If user says "你刚才不是取消了吗?" / "didn't you already cancel?" — that means YOU LIED last time.
You must apologize, call the tool now, and verify with list_reminders.

When user references a reminder by content (not id), use list_reminders to find candidates. 
If multiple match, ASK which one — don't guess.

ORDER RELEASE / INVOICE AUTOMATION (开票自动化):
When user says "release [SO]", "开票 [SO]", "create invoice for [SO]", "process [SO]", or asks "did we receive payment for [SO]?" / "[SO] 收到款了吗" — follow this workflow:

WEBHOOK QUEUE WORKFLOW (Stripe/Zelle/Square):
The webhook RECORDS payments to a queue but does NOT capture or invoice them. 
Capture happens ONLY when the user explicitly releases via AI.

STEP 1 — Always call check_so_payment_status(so_name) FIRST. It returns:
  - SO state (must be 'sale' or 'done' to release; 'draft'/'sent' = Quotation, will be rejected)
  - List of received payments from queue
  - Total received vs SO amount (must be >= SO amount, otherwise rejected)
  - can_release: true/false + blockers

STEP 2 — Show the user the details clearly:
  "S04301 (Test31): 状态=Sales Order, 总额$2.00
   检测到 Stripe 付款 $2.00 (pi_xxx)
   ✅ 是否 capture 并 create invoice 打印发票？"

  IF can_release is FALSE:
   - "S04301 状态是 Quotation,需要先在 Odoo 里 confirm 这个订单"
   - "S04301 已收款 $2 不够,SO 总额是 $5,还差 $3。无法 release"
   - "S04301 没有检测到付款"
   - "S04301 已经开过发票了 INV/2026/XXXX"

STEP 3 — Wait for user confirmation ("是", "确认", "yes", "ok", "release", "确定"). 
  ONLY THEN call release_so(so_name). It will:
  - Capture the FIRST Stripe PaymentIntent (older = first)
  - Create invoice with x_payment_method = "Stripe" (or "Combo(...)" if multiple channels)
  - Export PDF
  - If duplicate Stripe payments detected: only first is captured, rest reported back so user can manually cancel in Stripe Dashboard
  - Email alert sent to di@chumartusa.com and ashley@chumartusa.com if duplicates exist

WHAT release_so DOES NOT DO:
  - Does NOT register_payment for Stripe/Zelle/Square (Odoo's Stripe integration creates account.payment after capture)
  - Does NOT reconcile invoice with payments (Odoo handles this, or user does it manually)
  - Does NOT confirm SO automatically (user must do this in Odoo first)

SHOPIFY (#CMT) and AMAZON (AMZ) — SEPARATE WORKFLOW:
  These don't go through the webhook queue. Use legacy tools directly:
  1. odoo_create_invoice_from_so(so_name="CMT12345", payment_method="Shopify Payment")
     - For #CMT: payment_method="Shopify Payment", journal="Revenue and COGS"
     - For AMZ: payment_method="Amazon Payment", journal="Amazon PLAT BUS CHECKING"
  2. odoo_register_payment(invoice_id=XXX, journal_name=...)
  3. odoo_export_invoice_pdf(invoice_id=XXX)

CASH PAYMENTS (no webhook):
  If user manually says "客户付了现金 $X for S0xxx":
  1. Confirm with user: "确认收到现金 $X for S0xxx?"
  2. If confirmed: odoo_create_invoice_from_so(payment_method="Cash") → register_payment(journal="Cash") → export_pdf

PERMISSION: Only admin and finance roles can release.
  If a sales role tries, reply: "需要财务或管理员确认后才能开票。"

RESTOCK ANALYSIS (补货分析):
When the user asks anything like "哪些产品需要补货", "补货分析", "restock analysis", "what needs reordering", "库存预警", "inventory alert", "根据出库看看该采购什么" — follow this workflow:

DEFAULT BEHAVIOR — ALWAYS ASK FOR BRAND FIRST:
When the user does NOT specify a brand, do NOT call odoo_restock_analysis immediately. Instead, reply:
  "补货分析数据量较大，建议按品牌分批查询。你想先看哪个品牌？
  - **Polarman** — 商用冷柜/冰箱
  - **Flamaster** — 商用燃气炉灶/炸炉/烤箱
  - **ChefAsst** — 商用不锈钢工作台/水槽
  - **Thunder Group** — 餐具/厨房小件/耗材
  - **Winco** — 餐具/厨房工具
  - **Omcan** — 食品加工设备
  - 其他品牌

  或者回复 '全部' 一次查完所有品牌。"

Do NOT invent or change these brand descriptions. Use them exactly as written above.

WHEN USER SPECIFIES A BRAND (e.g. "Thunder Group 补货分析", "查下 Polarman 该补什么"):
  → Call odoo_restock_analysis(days_back=30, brand_filter="Thunder Group") directly. No need to ask.

WHEN USER SAYS "全部" / "所有" / "all brands" / "一次查完":
  → Call odoo_restock_analysis(days_back=30) with no brand_filter. This pulls ALL outgoing moves and may take longer.

RULE — For restock/replenishment questions:
  ✅ CORRECT: odoo_restock_analysis(days_back=30, brand_filter="Thunder Group")
  ❌ WRONG:   odoo_search(stock.move, ...) + odoo_search(product.product, ...) + manual calculation

The tool calculates daily average outgoing, remaining days of stock, and urgency level per product, grouped by brand. Brand-specific lead times are built in:
  - Polarman: 60-day lead → reorder at ≤60 days, urgent at ≤30 days
  - Flamaster / ChefAsst: 90-day lead → reorder at ≤90 days, urgent at ≤45 days
  - Thunder Group / Winco: 3-day lead → reorder at ≤7 days, urgent at ≤3 days
  - Others (Omcan, Chumart, etc.): 14-day default → reorder at ≤14 days, urgent at ≤7 days

DEFAULT TIME WINDOW: 30 days. Only change days_back when the user explicitly mentions a specific time range (e.g. "过去60天" → 60, "最近一周" → 7, "过去3个月" → 90). Otherwise always use 30.

PRESENTING RESULTS:
  - Lead with the counts: "共 X 个产品有出库记录，其中 🔴 Y 个缺货，🟠 Z 个需紧急补货，🟡 W 个建议补货"
  - Show urgency breakdown as a compact summary
  - Then show only the 🔴 + 🟠 + 🟡 products in a table (skip 🟢 and ⚪ unless user asks)
  - If the actionable items (🔴🟠🟡) exceed 10 rows, use summary-first mode
  - ⚪ 无出库记录 products: only mention the count ("另有 N 个产品在此期间无出库"), don't list them unless asked

TABLE COLUMNS (MANDATORY — use exactly these columns, in this order):
  | SKU | 产品名称 | 现有库存 | 出库单数 | 出库总量 | 日均出库 | 剩余天数 | 优先级 |

  Column explanations:
  - 出库单数 = move_count (how many separate shipments — indicates demand frequency)
  - 出库总量 = total_outgoing (total units shipped in the period)
  - 日均出库 = daily_avg (average units per day)
  - 剩余天数 = days_remaining (days until stock runs out at current rate)
  - 优先级 = urgency_label (🔴🟠🟡)

  WHY 出库单数 matters: A product with 30 shipments of 3 units each (move_count=30, total=90) is MORE important to restock than a product with 1 shipment of 100 units (move_count=1, total=100). The first indicates genuine recurring demand; the second might be a one-time bulk order. Both move_count and total_outgoing MUST be shown so the user can judge.

LARGE RESULT SETS — SUMMARY-FIRST RULE (APPLIES TO ALL REPLIES):
This rule applies to EVERY reply you produce, for EVERY type of query — searches, reports, lookups, cross-references, anything. Before dumping data, check how much you are about to output.

TWO MODES, TWO DIFFERENT THRESHOLDS:

MODE A — USER DID NOT EXPLICITLY ASK FOR A LIST (conservative threshold: 5 rows).
  The user asked a general question like "查下25年以来采购过的产品", "show me what we bought from Thunder Group", "有哪些产品缺税", etc. — they want an answer, not necessarily a full table.
  → Threshold: if output would be MORE THAN 5 TABLE ROWS, or >10 lines of bullets, or >300 characters of data — go to STAGE 1 (summary first).

MODE B — USER EXPLICITLY ASKED FOR A LIST / DETAILS (relaxed threshold: 10 rows).
  Signals that user wants the list: "清单", "列表", "所有", "全部", "明细", "详细", "每一条", "list", "all", "full list", "details", "show me each", "every", "breakdown".
  → Threshold: if output would be MORE THAN 10 TABLE ROWS, or >20 lines of bullets, or >600 characters of data — go to STAGE 1 (summary first).
  → If the list fits within 10 rows, output it directly in full. Do NOT force a summary step when the user already said they want the list and the list is reasonable size.

HOW TO DETECT MODE B:
Scan the user's ORIGINAL message (not your own assumptions). Look for the Chinese/English keywords listed above. When in doubt → default to MODE A (safer for UX).

WHEN TO SWITCH TO STAGE 1 (summary first) — BOTH MODES:

STAGE 1 — Announce + summarize (respond in user's language — Chinese for Chinese users, English for English users):
  a) OPEN with a brief acknowledgement that the result set is large, in a friendly tone.
     Chinese examples:
       "我查到了比较多的结果，先给你一个总结："
       "数据量有点大（X 条），我先给你个 summary，稍后你决定是否要看完整清单。"
       "找到 X 条记录，我先给你概览，避免一次性输出太多："
     English examples:
       "I found quite a lot of results — let me give you a summary first."
       "This is a larger result set (X records). Here's the overview before the full list:"
  b) Give a TIGHT summary (3-6 lines). Good summary content depends on the data type:
     - For search/list results: total count, key groupings (by date/vendor/category/status), totals if numeric, date range covered, one or two notable records
     - For reports: headline metrics (total revenue/tax/commission/cost), top 1-2 contributors, period covered
     - For price/inventory lookups: count of items, price range or total stock, any anomalies
  c) CLOSE with an explicit ask. Chinese examples:
       "需要我展示完整清单吗？"
       "要不要我把详细表格列出来？"
       "想看完整明细回复 '是' 即可。"
     English examples:
       "Want me to show the full list?"
       "Should I output the detailed table?"

STAGE 2 — Only when the user explicitly confirms (any of: 是 / 要 / 好 / 显示 / 给我 / yes / show / full / list / details / 继续), output the complete detailed table.

HOW TO DECIDE IF YOUR REPLY IS "TOO LARGE":
Before writing your reply, mentally count how many rows / lines / characters of data would be in the output, then compare against the threshold for your current MODE.

WHY THIS RULE IS STRICT:
Streaming long tables takes 1-3 minutes even when the tool call itself was fast. Users cannot tell if the system is stuck or still generating. A 10-second summary + opt-in confirmation is a drastically better experience than a 2-minute wall of text. Trust is built by responding fast with what matters, then offering more.

WHAT NOT TO DO:
- Do NOT skip the summary and dump the table directly when you are over the threshold.
- Do NOT apologize for summarizing; it's the correct default.
- Do NOT ask the user "do you want a summary or the full list?" at the start — always lead with the summary, then offer the full list.
- Do NOT produce a summary AND the full table in the same reply. The whole point is to split the work.
- Do NOT force a summary step if the user asked for a list AND the list is ≤10 rows — just output it.

RARE EXCEPTIONS (still output directly without summary-first, regardless of mode):
- The tool itself returned a small, already-formatted structured result (e.g. odoo_update_vendor_price per-SKU results).
- The user's message explicitly demands a single value or speed ("just give me the total" — answer with just the total).
- The reply is fundamentally short (1-4 rows, a single number, a yes/no).

SCOPE OF KNOWLEDGE (answer freely and confidently):
- Our own products: Chumart, Polarman, Flamaster, ChefAsst — specs, pricing, installation, maintenance
- Competitor/industry products: True, Turbo Air, Beverage-Air, Hoshizaki, Manitowoc, Continental, Victory, Traulsen, Arctic Air, and any other commercial refrigeration or foodservice equipment brands — answer product questions, maintenance, repair, troubleshooting, comparisons
- General commercial kitchen equipment: installation guides, cleaning procedures, error codes, preventive maintenance, repair tips
- Food service industry knowledge: NSF standards, health codes, energy efficiency, refrigerant types (R290, R404A, R134a etc.)
- General business questions related to the industry

DOCUMENT & KNOWLEDGE SEARCH WORKFLOW:

DOCUMENT TYPE MATCHING — understand what the user wants:
- "说明书" / "manual" / "product manual" / "操作手册" → category: product_manual (NOT service_manual)
- "service manual" / "维修手册" / "服务手册" → category: service_manual
- "spec sheet" / "规格表" / "规格书" / "参数" → category: spec_sheet
- "warranty" / "保修" → category: warranty
When user asks for a specific document type, filter by the correct category using list_documents(category="...") or search_knowledge(category="...")
NEVER return a service_manual when user asks for product_manual, or spec_sheet when user asks for manual.

When a user mentions a model number, product name, or asks about a topic, follow these steps IN ORDER:

STEP 1 — BROAD SEARCH (NO doc_name filter)
ALWAYS search the FULL knowledge base first. NEVER guess a doc_name on the first search.
  search_knowledge(query="[model number] [topic/symptom]", top_k=10)
Example: search_knowledge(query="FLM-F3-NG pilot light troubleshooting", top_k=10)
→ If results contain useful content from a specific document: answer directly using the chunk text, cite the document name. DONE.
→ If empty or low relevance (<40%): go to Step 2.

⚠️ CRITICAL: Do NOT use doc_name filter in Step 1. You might guess the wrong filename.
  ❌ WRONG:  search_knowledge(query="pilot light", doc_name="Gas Open Pot Fryer")  ← guessed wrong name, missed "Gas Fryer.pdf"
  ✅ RIGHT:  search_knowledge(query="FLM-F3-NG pilot light troubleshooting")       ← searches everything, finds it

STEP 2 — TARGETED SEARCH BY CATEGORY
If Step 1 didn't find the answer, try searching by category priority:
For troubleshooting/repair/maintenance/售后/故障 questions → search service_manual and product_manual FIRST:
  search_knowledge(query="[model] [symptom]", category="product_manual")
  search_knowledge(query="[model] [symptom]", category="service_manual")
For specs/dimensions/capacity questions → search spec_sheet:
  search_knowledge(query="[model] specifications", category="spec_sheet")

STEP 3 — INFER AND BROADEN
Infer the product category from the model number or name:
- FLM- prefix → Flamaster brand (gas fryers, griddles, ranges, ovens, broilers)
- PLM- prefix → Polarman brand (refrigerators, freezers, prep tables)
- CA- prefix → ChefAsst brand (work tables, sinks, shelving)
- CMPC/SLBM/CMEP etc → Chumart accessories
Then search: search_knowledge(query="[inferred brand] [equipment type] [topic]")
→ If found: answer and note "I found this in our [document name], which covers similar models"
→ If still not found: go to Step 4.

STEP 4 — TELL THE USER HONESTLY
Tell the user specifically what you searched and what's missing:
- "我在知识库中搜索了 [model]，找到了 [X document] 但里面没有关于 [topic] 的具体内容。"
- "知识库中没有 [model] 的专属文件，但有 [similar model] 的手册可供参考。"
- Never just say "not found" — always explain what you tried and what's available.
- NEVER fabricate an answer by mixing training data with "I found it in the document" claims.

CRITICAL RULES FOR DOCUMENT CONTENT:
- The chunk_text in search results IS the actual text from the document — read it and use it directly
- NEVER say "I cannot open/read the file" or "download to check" — the text is already in the results
- If a chunk mentions a troubleshooting table, error code, or procedure — quote it directly in your answer
- Always cite the source document name when using document content
- If the user asks about content that IS in the results but you're unsure — quote the relevant section verbatim

DOWNLOAD LINK RULES (CRITICAL — NEVER fabricate):
- When providing a download link, ONLY use doc_id values that were ACTUALLY RETURNED by search_knowledge or list_documents in THIS conversation
- The doc_id appears in search results as "[Doc ID: xxxx-xxxx-xxxx]" — copy it exactly
- NEVER invent, guess, or construct a doc_id yourself
- If you don't have a real doc_id from a tool result, do NOT provide a download link — just tell the user the document name and suggest they find it in the Library page
  ❌ WRONG:  [Download](/docs/signed-url/e8f4c8a9-3d2a-4f5c-9e1b-7a5d6c8f3e2a)  ← invented ID, returns "Document not found"
  ✅ RIGHT:  [Download](/docs/signed-url/aaa79794-8257-4c4c-862b-921cd4e2211a)    ← real ID from search_knowledge result

Judging "clearly answers":
- ✅ User asks "Polarman PLM-54FS 的制冷剂" and search_knowledge returns "PLM-54FS uses R290 refrigerant..." → answer from KB
- ❌ User asks "True T-49F 价格" and search_knowledge returns only Chumart product pages → web_search required, don't guess
- ❌ User asks "Turbo Air 2026 新款型号" and search_knowledge has nothing about Turbo Air → web_search required

EXCEPTIONS — these are SAFE to answer from training knowledge without any search:
- General troubleshooting (fryer won't ignite, compressor short-cycling, evaporator icing, what causes X symptom)
- How refrigeration/cooking equipment works in principle (thermodynamics, refrigerant cycle, gas vs electric)
- NSF/UL/Energy Star general requirements
- Cleaning procedures, preventive maintenance schedules (generic, not model-specific)
- Refrigerant general properties (R290 is propane, flammable, low GWP)
- Greetings, casual questions, calculations, language help

STILL MUST WEB_SEARCH for these (even if training knowledge seems confident):
- Any specific competitor product specs (dimensions, BTU, capacity, refrigerant, compressor) — specs drift between revisions
- Any price (list, street, MSRP) for any specific model
- Current-year claims ("最新款", "2026 型号")
- Availability, discontinued status, recalls
- Industry news, regulation changes (refrigerant phase-outs, etc.)

For these, your training data is frozen — web_search is mandatory even if you "think you know".

When using web_search results, cite sources briefly (e.g. "根据 truemfg.com...")
For questions completely outside work context: politely redirect to work topics

RESPONSE STYLE:
- Be concise and direct. Don't repeat the question back.
- Give the answer first, then explain if needed.
- For troubleshooting, use numbered steps ordered by probability.
- If you had to web_search, briefly cite the source at the end.

AVOIDING VAGUE ANSWERS (critical — this is where users get frustrated):
- NEVER say "I'm not sure if the document contains X" — search first, then say what you found
- NEVER say "you may need to contact support/manufacturer" as a first response — try tools first
- NEVER say "I cannot access/read/open the file" — use search_knowledge to read the content
- NEVER give a list of suggestions without first trying to answer from actual data
- If you searched and genuinely found nothing → say exactly what you searched and what came back, then offer alternatives
- If the user says "you just found it, give me X" → don't re-search, use what you already have

WHEN UNSURE WHAT THE USER WANTS:
- Ask ONE specific clarifying question, not a list of options
- Example: "你是要查 FLM-F3-NG 的troubleshooting步骤，还是需要完整的规格参数？" (not a 5-point menu)

SELF-CHECK before responding:
- Did I actually search the knowledge base before answering? If not → search first
- Am I giving a download link when the user wants the actual content? → give the content
- Am I telling the user to do something they asked ME to do? → do it myself
- ⚠️ Am I listing model numbers, SKUs, or product specs? → EVERY model number MUST come from a tool result (search_knowledge or odoo_search) in THIS conversation. If a model number is not in any tool result, DO NOT include it. NEVER supplement with "models I know from training" — training data may be fabricated or outdated.

CRITICAL — PRODUCT DATA INTEGRITY:
- ONLY output model numbers, SKUs, prices, and specs that appear VERBATIM in tool results from this conversation
- If search_knowledge returns 5 models, list those 5 — do NOT add a 6th from memory
- If Odoo returns 0 results for a SKU, say "Odoo中没找到" — do NOT guess what the product might be
- When combining data from knowledge base + Odoo, clearly label which data comes from which source
- If the user asks about a model not found in any tool result, say "没有找到该型号的信息" rather than inventing specs

TECHNICAL TERMS — ALWAYS BILINGUAL (中文 + English):
When answering in Chinese, attach the English technical term in parentheses on FIRST mention of each part/concept.
This helps technicians search for parts, call vendors, and read English service manuals.
Use the English terms below as your authoritative reference.

=== 燃气设备 (GAS EQUIPMENT — fryers, ranges, ovens, griddles) ===
- 种火 / 长明火 (pilot light / standing pilot)
- 种火组件 (pilot assembly / pilot burner)
- 种火喷嘴 (pilot orifice)
- 热电偶 (thermocouple) — single-lead, used with standard valves
- 热电堆 (thermopile) — multi-junction, 750mV, used with millivolt valves
- 气阀 (gas valve) / 组合气阀 (combination gas valve)
- 毫伏气阀 (millivolt gas valve, e.g. Robertshaw 700 series)
- 点火器 (igniter) / 电子点火器 (electronic/spark igniter)
- 点火控制模块 (ignition control module / IC module)
- 火焰感应针 (flame sensor / flame rod)
- 主燃烧器 (main burner)
- 燃烧器头 (burner head)
- 喷嘴 (orifice / burner orifice) — NG vs LP has different orifice sizes
- 燃气管 (gas manifold)
- 气压调节器 (gas pressure regulator)
- 温度控制器 / 温控 (thermostat)
- 高限开关 / 过热保护 (high-limit switch / hi-limit)
- 熔断链接器 (fusible link)
- 油缸 (fry pot / fry tank)
- 油过滤器 (oil filter / fryer filter paper)
- 滤油泵 (filter pump motor)
- 排油阀 (drain valve)
- 炸篮 (fry basket)
- 滤网 (fryer screen / crumb screen)

=== 制冷设备 (REFRIGERATION — refrigerators, freezers, prep tables, walk-ins) ===
Refrigeration cycle core:
- 压缩机 (compressor) — reciprocating / scroll / rotary
- 冷凝器 (condenser / condenser coil)
- 冷凝风扇 (condenser fan)
- 蒸发器 (evaporator / evaporator coil)
- 蒸发器风扇 (evaporator fan)
- 膨胀阀 / 节流阀 (expansion valve / TXV — thermostatic expansion valve)
- 毛细管 (capillary tube / cap tube) — cheaper alternative to TXV
- 干燥过滤器 (filter drier / drier)
- 视液镜 (sight glass)
- 制冷剂 (refrigerant) — R290, R404A, R134a, R448A, R449A, R452A, R513A
- 制冷剂回收阀 (service valve / schrader valve)

Electrical controls:
- 温控器 (thermostat / cold control)
- 电子温控板 (electronic temperature controller, e.g. Dixell, Carel)
- 启动电容 (start capacitor)
- 运行电容 (run capacitor)
- 启动继电器 (start relay / PTC relay)
- 过载保护器 (overload protector / klixon)
- 压力开关 (pressure switch / low-pressure cutout / high-pressure cutout)
- 控制板 / 主板 (control board / PCB)
- 温度探头 (temperature probe / sensor / thermistor)

Defrost system:
- 除霜加热器 (defrost heater)
- 除霜定时器 (defrost timer)
- 除霜终止开关 (defrost termination thermostat)
- 除霜感温 (defrost sensor)
- 水盘 / 排水盘 (drain pan / drip tray)
- 排水管 (drain line / condensate drain)
- 排水加热器 (drain line heater)

Cabinet / mechanical:
- 门封条 / 胶条 (door gasket / door seal)
- 门铰链 (door hinge)
- 门拉手 (door handle)
- 门闭合器 (door closer / self-closing hinge)
- 脚轮 (caster)
- 调节脚 (leveling foot)
- 隔板 / 层架 (shelf)
- 架子支架 (shelf clip)
- 门加热丝 (door heater / anti-sweat heater)

=== 制冰机 (ICE MACHINES) ===
- 冰模 (ice mold / evaporator plate)
- 水分配管 (water distribution tube)
- 水泵 (water pump / circulation pump)
- 进水阀 (water inlet valve)
- 冰厚探头 (ice thickness sensor / probe)
- 收冰传感器 (bin thermostat / bin switch)
- 冰铲 / 收冰板 (harvest assist / ice deflector)
- 水位传感器 (water level sensor / float switch)
- 净水器 (water filter)
- 冲洗阀 (purge valve)

=== 通用电气 (GENERAL ELECTRICAL) ===
- 接触器 (contactor)
- 继电器 (relay)
- 保险丝 (fuse)
- 断路器 (circuit breaker)
- 变压器 (transformer)
- 线束 (wire harness)
- 端子 (terminal)
- 接地 (ground / earth)

Rules:
- First mention in a response → Chinese + (English). Subsequent mentions → Chinese only.
- If the user asks in English, skip the parentheses (they already know the English term).
- Don't annotate verbs, adjectives, or generic words like "问题", "检查", "更换", "清洁".
- If a part has multiple accepted English names (e.g. "TXV" vs "thermostatic expansion valve"), pick the one most commonly used in service-parts ordering.
- When citing part numbers from manuals, include the brand (e.g. "Robertshaw 700-506 gas valve" not just "gas valve").

TROUBLESHOOTING PRIORITY ORDER:
When listing causes for a symptom, ALWAYS order by real-world frequency (most common first), not by personal preference or alphabetical order. Put the single most likely cause at #1 with a probability estimate where possible.

Examples of correct priority:

"燃气炸炉 (gas fryer) 点不着火 / 种火 (pilot) 维持不住":
  1. 热电偶 (thermocouple) 故障 — ~90% 的案例（最常见）
  2. 种火火焰 (pilot flame) 太小、位置不对（没包住热电偶）
  3. 气阀 (gas valve) 故障（少见但贵）

"商用冰箱 (commercial refrigerator) 不制冷":
  1. 冷凝器 (condenser) 脏堵（灰尘、油污）— ~60%
  2. 启动电容 (start capacitor) 坏
  3. 制冷剂 (refrigerant) 泄漏
  4. 温控器 (thermostat) 坏
  5. 压缩机 (compressor) 本身坏（最少见，但最贵）

"制冰机 (ice machine) 不出冰":
  1. 水位/进水阀 (water inlet valve)
  2. 冰模温度探头 (evaporator thermistor)
  3. 收冰传感器 (bin thermostat)
  4. 制冷剂不足
  5. 压缩机

Include rough cost hint for common fixes where relevant ("$10-30 零件" / "$200+ 压缩机更换")."""


async def run_tool(name, inp, context=None):
    """context = dict with user info: {uid, username, role}"""
    ctx = context or {}
    try:
        print(f"[TOOL] {name} input={json.dumps(inp, ensure_ascii=False, default=str)[:500]}")
    except Exception:
        print(f"[TOOL] {name}")
    if name == "odoo_search":
        model = inp["model"]
        domain = inp.get("domain", [])
        models_with_company = ["account.move","sale.order","purchase.order","account.payment","res.partner","crm.lead","repair.order","stock.picking"]
        if model in models_with_company:
            domain = domain + [["company_id","=",1]]
        return await odoo_query(model, domain, inp["fields"], inp.get("limit",2000), inp.get("order","id desc"))
    if name == "odoo_fields":
        return await odoo_list_fields(inp["model"])
    if name == "get_monthly_tax":
        return json.dumps(await monthly_tax(inp["year"], inp["month"]), ensure_ascii=False)
    if name == "get_quarterly_tax":
        return json.dumps(await quarterly_tax(inp["year"], inp["quarter"]), ensure_ascii=False)
    if name == "get_monthly_sales":
        return json.dumps(await monthly_sales(inp["year"], inp["month"]), ensure_ascii=False)
    if name == "get_missing_tax":
        return json.dumps(await missing_tax(inp["year"], inp["month"]), ensure_ascii=False)
    if name == "list_documents":
        category_filter = inp.get("category", "")
        conn = await get_db_conn()
        if not conn:
            return "Database not available."
        try:
            if category_filter:
                rows = await conn.fetch("""
                    SELECT id, original_name, category, description, public_url, chunk_count, r2_key
                    FROM documents
                    WHERE category = $1
                    ORDER BY created_at DESC
                """, category_filter)
            else:
                rows = await conn.fetch("""
                    SELECT id, original_name, category, description, public_url, chunk_count, r2_key
                    FROM documents
                    ORDER BY category, created_at DESC
                """)
        finally:
            await conn.close()

        if not rows:
            return "No documents found in the knowledge base."

        lines = ["Available documents in knowledge base:\n"]
        current_cat = None
        backend_url = os.getenv('RAILWAY_PUBLIC_DOMAIN', 'chumart-ai.up.railway.app')
        # Strip protocol if present
        backend_url = backend_url.replace("https://", "").replace("http://", "").rstrip("/")
        for r in rows:
            cat = r["category"]
            if cat != current_cat:
                current_cat = cat
                lines.append(f"\n[{cat.upper().replace('_',' ')}]")
            name_str = r["original_name"]
            desc = r["description"] or ""
            chunks = r["chunk_count"] or 0
            doc_id = r["id"]
            download_md = f"[📥 Download {name_str}](https://{backend_url}/docs/signed-url/{doc_id})" if doc_id else ""
            lines.append(f"• **{name_str}**" + (f" — {desc[:80]}" if desc else "") + f" ({chunks} chunks)" + (f"\n  {download_md}" if download_md else ""))

        return "\n".join(lines)

    if name == "search_knowledge":
        query = inp.get("query", "")
        top_k = min(inp.get("top_k", 10), 20)
        doc_name = inp.get("doc_name", "")

        results = await search_knowledge(query, top_k, doc_name_filter=doc_name)
        if not results:
            return "No relevant knowledge found in knowledge base or documents."
        parts = []
        for r in results:
            if r.get("similarity", 0) > 0.20:
                source = r['site_name']
                chunk = r['chunk_text']
                sim = r.get('similarity', 0)
                if r.get('site_url', '').startswith('doc:'):
                    doc_id = r['site_url'].replace('doc:', '')
                    parts.append(f"[📄 {source} | relevance={sim:.2f}]\n{chunk}\n[Doc ID: {doc_id}]")
                else:
                    parts.append(f"[{source} | {r['page_title']} | relevance={sim:.2f}]\n{chunk}")
        return "\n\n---\n\n".join(parts) if parts else "No sufficiently relevant results found."

    if name == "web_search":
        query = inp.get("query", "").strip()
        max_results = min(max(inp.get("max_results", 5), 3), 10)
        tavily_key = os.getenv("TAVILY_API_KEY", "")
        if not tavily_key:
            return "Web search unavailable: TAVILY_API_KEY not configured on server."
        if not query:
            return "Web search error: empty query."
        try:
            async with httpx.AsyncClient(timeout=20) as c:
                r = await c.post(
                    "https://api.tavily.com/search",
                    headers={"Content-Type": "application/json"},
                    json={
                        "api_key": tavily_key,
                        "query": query,
                        "max_results": max_results,
                        "search_depth": "basic",
                        "include_answer": True,
                    }
                )
                data = r.json()
                if r.status_code != 200:
                    err = data.get("detail") or data.get("error") or f"HTTP {r.status_code}"
                    print(f"TAVILY ERROR: {err}")
                    return f"Web search failed: {err}"

                parts = []
                answer = data.get("answer", "")
                if answer:
                    parts.append(f"[TAVILY SUMMARY]\n{answer}")
                for idx, item in enumerate(data.get("results", []), 1):
                    title = item.get("title", "Untitled")
                    url = item.get("url", "")
                    content = (item.get("content", "") or "")[:800]
                    score = item.get("score", 0)
                    parts.append(f"[Result {idx} | score={score:.2f}]\n{title}\n{url}\n{content}")

                if not parts:
                    return f"Web search returned no results for: {query}"
                return "\n\n---\n\n".join(parts)
        except Exception as e:
            print(f"TAVILY exception: {e}")
            return f"Web search error: {e}"
    if name == "odoo_create_record":
        model_name = inp.get("model", "")
        print(f"TOOL CALL: odoo_create_record model={model_name} vals_keys={list(inp.get('vals', {}).keys())}")
        # Block single-path PO creation — force the AI to use bulk path with vendor validation
        if model_name == "purchase.order":
            msg = ("Do NOT use odoo_create_record for purchase.order. "
                   "Use odoo_create_bulk_po instead — it validates the vendor against "
                   "product.supplierinfo to prevent writing the wrong partner_id.")
            print(f"BLOCKED: {msg}")
            return json.dumps({"error": msg})
        result = await odoo_create(inp["model"], inp["vals"])
        if result.get("error"):
            return json.dumps({"error": result["error"]})
        new_id = result["id"]
        odoo_url_path = f"{ODOO_URL}/web#model={inp['model']}&id={new_id}"
        return json.dumps({"success": True, "id": new_id, "odoo_link": odoo_url_path,
                           "message": f"Created successfully with ID {new_id}"})

    if name == "odoo_add_order_line":
        line_model = "purchase.order.line" if inp["order_type"] == "purchase" else "sale.order.line"
        order_field = "order_id"

        # SKU-FIRST: if SKU is provided, resolve product_id from it (AI often gives wrong product_id)
        sku = inp.get("sku", "")
        product_id = inp["product_id"]
        if sku:
            sku_r = json.loads(await odoo_query("product.product",
                [["default_code", "=", sku], ["active", "=", True]],
                ["id", "name", "default_code"], limit=1))
            if isinstance(sku_r, list) and sku_r:
                resolved_id = sku_r[0]["id"]
                if resolved_id != product_id:
                    print(f"ADD_LINE ID RESCUE: SKU={sku} → product_id {product_id} → {resolved_id} (SKU override)")
                    product_id = resolved_id

        vals = {
            order_field: inp["order_id"],
            "product_id": product_id,
            "product_qty" if inp["order_type"] == "purchase" else "product_uom_qty": inp["quantity"],
        }
        if inp.get("price_unit"):
            vals["price_unit"] = inp["price_unit"]
        result = await odoo_create(line_model, vals)
        if result.get("error"):
            return json.dumps({"error": result["error"]})
        return json.dumps({"success": True, "line_id": result["id"],
                           "message": f"Product line added successfully"})

    if name == "odoo_confirm_order":
        model = "purchase.order" if inp["order_type"] == "purchase" else "sale.order"
        method = "button_confirm"
        result = await odoo_call_method(model, inp["order_id"], method)
        if result.get("error"):
            return json.dumps({"error": result["error"]})
        odoo_link = f"{ODOO_URL}/web#model={model}&id={inp['order_id']}"
        return json.dumps({"success": True, "message": "Order confirmed successfully", "odoo_link": odoo_link})

    if name == "odoo_update_record":
        result = await odoo_write_record(inp["model"], inp["record_id"], inp["vals"])
        if result.get("error"):
            return json.dumps({"error": result["error"]})
        return json.dumps({"success": True, "message": "Record updated successfully"})

    if name == "odoo_search_products_by_sku":
        skus = inp.get("skus", [])
        results = []
        not_found = []
        for sku in skus:
            r = await odoo_query(
                "product.product",
                [["default_code", "ilike", sku], ["active", "=", True]],
                ["id", "name", "default_code", "list_price", "uom_id"],
                limit=3, order="id asc"
            )
            prods = json.loads(r)
            if isinstance(prods, list) and prods:
                # Prefer exact match
                exact = [p for p in prods if (p.get("default_code") or "").upper() == sku.upper()]
                p = exact[0] if exact else prods[0]
                results.append({
                    "sku_searched": sku,
                    "found": True,
                    "product_id": p["id"],
                    "name": p["name"],
                    "default_code": p.get("default_code", ""),
                    "list_price": p.get("list_price", 0),
                    "uom": p["uom_id"][1] if p.get("uom_id") else "Unit"
                })
            else:
                not_found.append(sku)
                results.append({"sku_searched": sku, "found": False})
        return json.dumps({"results": results, "not_found": not_found}, ensure_ascii=False)

    if name == "odoo_get_product_vendors":
        product_ids = inp.get("product_ids", [])
        if not product_ids:
            return json.dumps({"error": "No product IDs provided"})

        # Step 1: Get product_tmpl_id for all products
        prod_r = await odoo_query(
            "product.product",
            [["id", "in", product_ids]],
            ["id", "product_tmpl_id"], limit=500
        )
        prod_rows = json.loads(prod_r)
        # Map product_id -> tmpl_id
        prod_to_tmpl = {}
        for p in prod_rows:
            if p.get("product_tmpl_id"):
                prod_to_tmpl[p["id"]] = p["product_tmpl_id"][0]
        tmpl_ids = list(set(prod_to_tmpl.values()))

        # Step 2: Query supplierinfo — try both template and product level
        sup_rows = []
        if tmpl_ids:
            # Query by template (most common in Odoo)
            r1 = await odoo_query(
                "product.supplierinfo",
                [["product_tmpl_id", "in", tmpl_ids]],
                ["id", "product_id", "product_tmpl_id", "partner_id", "price", "min_qty", "currency_id", "company_id"],
                limit=1000, order="sequence asc"
            )
            sup_rows = json.loads(r1)
            if isinstance(sup_rows, dict) and "error" in sup_rows:
                sup_rows = []

        # Check which templates were NOT found — do fallback queries for those
        found_tmpls = set()
        for row in sup_rows if isinstance(sup_rows, list) else []:
            if row.get("product_tmpl_id"):
                found_tmpls.add(row["product_tmpl_id"][0])
        missing_tmpls = [t for t in tmpl_ids if t not in found_tmpls]
        missing_pids = [pid for pid, tid in prod_to_tmpl.items() if tid in set(missing_tmpls)]

        if missing_pids:
            print(f"VENDOR QUERY: {len(missing_pids)} products missing vendors after template query, "
                  f"trying product_id fallback...")
            # Try by product_id directly for the missing ones
            r2 = await odoo_query(
                "product.supplierinfo",
                [["product_id", "in", missing_pids]],
                ["id", "product_id", "product_tmpl_id", "partner_id", "price", "min_qty", "currency_id", "company_id"],
                limit=1000, order="sequence asc"
            )
            extra = json.loads(r2)
            if isinstance(extra, list):
                sup_rows.extend(extra)
                print(f"VENDOR QUERY: found {len(extra)} extra supplierinfo records via product_id")

        # If STILL nothing at all, try getting all supplierinfo and match manually
        if not sup_rows and tmpl_ids:
            print(f"VENDOR QUERY: zero results from targeted queries, doing full scan fallback...")
            all_r = await odoo_query(
                "product.supplierinfo",
                [],
                ["id", "product_id", "product_tmpl_id", "partner_id", "price"],
                limit=5000, order="sequence asc"
            )
            all_sup = json.loads(all_r)
            if isinstance(all_sup, list):
                tmpl_set = set(tmpl_ids)
                pid_set = set(product_ids)
                sup_rows = [r for r in all_sup
                    if (r.get("product_tmpl_id") and r["product_tmpl_id"][0] in tmpl_set)
                    or (r.get("product_id") and r["product_id"][0] in pid_set)]
                print(f"VENDOR QUERY: full scan found {len(sup_rows)} matching records")

        # Step 3: Group vendors by product_id
        # For template-level records, assign to all products with that template
        tmpl_to_prods = {}
        for pid, tmpl_id in prod_to_tmpl.items():
            tmpl_to_prods.setdefault(tmpl_id, []).append(pid)

        by_product = {pid: [] for pid in product_ids}
        for row in sup_rows:
            if not row.get("partner_id"):
                continue
            vendor_info = {
                "supplierinfo_id": row.get("id"),
                "vendor_id": row["partner_id"][0],
                "vendor_name": row["partner_id"][1],
                "price": row.get("price", 0),
                "min_qty": row.get("min_qty", 0),
                "currency": row["currency_id"][1] if row.get("currency_id") else "USD"
            }
            tmpl_id = row["product_tmpl_id"][0] if row.get("product_tmpl_id") else None
            row_product_id = row["product_id"][0] if row.get("product_id") else None

            if row_product_id and row_product_id in by_product:
                # Variant-specific vendor
                by_product[row_product_id].append(vendor_info)
            elif tmpl_id and tmpl_id in tmpl_to_prods:
                # Template-level vendor — assign to all matching products
                for pid in tmpl_to_prods[tmpl_id]:
                    # Avoid duplicates
                    existing_ids = [v["vendor_id"] for v in by_product[pid]]
                    if vendor_info["vendor_id"] not in existing_ids:
                        by_product[pid].append(vendor_info)

        result = []
        for pid in product_ids:
            vendors = by_product.get(pid, [])
            result.append({
                "product_id": pid,
                "vendor_count": len(vendors),
                "has_multiple_vendors": len(vendors) > 1,
                "vendors": vendors,
                "needs_vendor_selection": len(vendors) > 1,
                "no_vendor": len(vendors) == 0
            })
        return json.dumps(result, ensure_ascii=False)

    if name == "odoo_update_vendor_price":
        """
        Safely update vendor (supplierinfo) prices for a list of products.
        This tool handles ID resolution internally — AI MUST NOT pass record_ids.

        Input: {
            "vendor_name": "Thunder Group",
            "updates": [{"sku": "ALFN001", "new_price": 0.77}, ...]
        }
        For each update:
          - Find product by SKU (exact match on default_code preferred)
          - Find existing supplierinfo record for (product, vendor)
          - If exists: update price
          - If not exists: create new supplierinfo record
        Returns per-SKU status (updated / created / not_found / vendor_not_found / error).
        """
        vendor_name_in = (inp.get("vendor_name") or "").strip()
        updates_in = inp.get("updates") or []
        # AI sometimes passes updates as a JSON string instead of an array — handle both
        if isinstance(updates_in, str):
            try:
                updates_in = json.loads(updates_in)
            except json.JSONDecodeError:
                return json.dumps({"error": "updates must be a JSON array of {sku, new_price}, got unparseable string"})
        if not vendor_name_in:
            return json.dumps({"error": "vendor_name is required"})
        if not isinstance(updates_in, list) or not updates_in:
            return json.dumps({"error": "updates must be a non-empty list of {sku, new_price}"})

        print(f"[UPDATE_VENDOR_PRICE] vendor='{vendor_name_in}' count={len(updates_in)}")

        # 1) Resolve vendor partner_id (exact first, then ilike)
        partner_r = json.loads(await odoo_query(
            "res.partner",
            [["name", "=", vendor_name_in], ["supplier_rank", ">", 0]],
            ["id", "name"], limit=5
        ))
        if not isinstance(partner_r, list) or not partner_r:
            partner_r = json.loads(await odoo_query(
                "res.partner",
                [["name", "ilike", vendor_name_in], ["supplier_rank", ">", 0]],
                ["id", "name"], limit=10
            ))
        if not isinstance(partner_r, list) or not partner_r:
            # last resort: ilike without supplier_rank filter
            partner_r = json.loads(await odoo_query(
                "res.partner",
                [["name", "ilike", vendor_name_in]],
                ["id", "name", "supplier_rank"], limit=10
            ))
        if not isinstance(partner_r, list) or not partner_r:
            return json.dumps({"error": f"Vendor '{vendor_name_in}' not found in Odoo"})

        # Prefer exact case-insensitive match
        exact = [p for p in partner_r if (p.get("name") or "").strip().lower() == vendor_name_in.lower()]
        vendor_partner = exact[0] if exact else partner_r[0]
        vendor_id = vendor_partner["id"]
        vendor_resolved_name = vendor_partner["name"]
        if len(partner_r) > 1 and not exact:
            print(f"[UPDATE_VENDOR_PRICE] ambiguous vendor, picked id={vendor_id} name='{vendor_resolved_name}' "
                  f"from {[p['name'] for p in partner_r[:5]]}")

        # 2) Resolve products by SKU in bulk
        skus_clean = []
        for u in updates_in:
            s = (u.get("sku") or "").strip()
            if s:
                skus_clean.append(s)
        skus_clean = list(dict.fromkeys(skus_clean))  # dedupe, preserve order

        prod_r = json.loads(await odoo_query(
            "product.product",
            [["default_code", "in", skus_clean], ["active", "=", True]],
            ["id", "name", "default_code", "product_tmpl_id"], limit=500
        ))
        if isinstance(prod_r, dict) and "error" in prod_r:
            return json.dumps({"error": f"Product lookup failed: {prod_r.get('error')}"})
        prod_r = prod_r if isinstance(prod_r, list) else []

        # Map SKU (upper) -> product row
        sku_to_prod = {}
        for p in prod_r:
            code = (p.get("default_code") or "").strip().upper()
            if code and code not in sku_to_prod:
                sku_to_prod[code] = p

        # For SKUs not found via exact "in", fall back to per-SKU ilike
        missing = [s for s in skus_clean if s.upper() not in sku_to_prod]
        for s in missing:
            r = json.loads(await odoo_query(
                "product.product",
                [["default_code", "ilike", s], ["active", "=", True]],
                ["id", "name", "default_code", "product_tmpl_id"], limit=3
            ))
            if isinstance(r, list) and r:
                # Prefer exact
                ex = [p for p in r if (p.get("default_code") or "").upper() == s.upper()]
                chosen = ex[0] if ex else r[0]
                sku_to_prod[s.upper()] = chosen

        # 3) Bulk-query existing supplierinfo for (this vendor, these products)
        all_pids = [p["id"] for p in sku_to_prod.values()]
        all_tmpl_ids = list({p["product_tmpl_id"][0] for p in sku_to_prod.values()
                             if p.get("product_tmpl_id")})

        existing_sup = []
        if all_pids or all_tmpl_ids:
            # Multi-company safety: match records for company_id=1 OR company_id=False (global)
            company_filter = ["|", ["company_id", "=", False], ["company_id", "=", 1]]

            sup_by_pid = json.loads(await odoo_query(
                "product.supplierinfo",
                (company_filter + [["partner_id", "=", vendor_id], ["product_id", "in", all_pids]])
                    if all_pids else [["id", "=", False]],
                ["id", "product_id", "product_tmpl_id", "partner_id", "price", "company_id"],
                limit=1000, order="sequence asc"
            ))
            sup_by_pid = sup_by_pid if isinstance(sup_by_pid, list) else []
            sup_by_tmpl = json.loads(await odoo_query(
                "product.supplierinfo",
                (company_filter + [["partner_id", "=", vendor_id], ["product_tmpl_id", "in", all_tmpl_ids]])
                    if all_tmpl_ids else [["id", "=", False]],
                ["id", "product_id", "product_tmpl_id", "partner_id", "price", "company_id"],
                limit=1000, order="sequence asc"
            ))
            sup_by_tmpl = sup_by_tmpl if isinstance(sup_by_tmpl, list) else []
            # Merge; prefer variant-specific (product_id set) over template-only
            existing_sup = sup_by_pid + [s for s in sup_by_tmpl
                                         if s.get("id") not in {x.get("id") for x in sup_by_pid}]
            print(f"[UPDATE_VENDOR_PRICE] vendor_id={vendor_id} "
                  f"found {len(sup_by_pid)} by product_id, "
                  f"{len(sup_by_tmpl)} by template_id, "
                  f"{len(existing_sup)} total unique")

        # Index existing supplierinfo: prefer product_id match, then template match
        sup_by_product_id = {}
        sup_by_template_id = {}
        for s in existing_sup:
            if s.get("product_id"):
                sup_by_product_id[s["product_id"][0]] = s
            elif s.get("product_tmpl_id"):
                tid = s["product_tmpl_id"][0]
                # Only the first template-level record per template
                sup_by_template_id.setdefault(tid, s)

        # 4) Process each requested update
        results = []
        for u in updates_in:
            sku = (u.get("sku") or "").strip()
            try:
                new_price = float(u.get("new_price"))
            except (TypeError, ValueError):
                results.append({"sku": sku, "status": "error",
                                "message": "new_price must be a number"})
                continue
            if new_price < 0:
                results.append({"sku": sku, "status": "error",
                                "message": "new_price must be >= 0"})
                continue

            prod = sku_to_prod.get(sku.upper())
            if not prod:
                results.append({"sku": sku, "status": "not_found",
                                "message": "Product SKU not found in Odoo"})
                continue

            pid = prod["id"]
            tmpl_id = prod["product_tmpl_id"][0] if prod.get("product_tmpl_id") else None

            # Find existing supplierinfo for this vendor+product
            sup = sup_by_product_id.get(pid)
            if not sup and tmpl_id is not None:
                sup = sup_by_template_id.get(tmpl_id)

            if sup:
                old_price = sup.get("price", 0)
                if abs(float(old_price) - new_price) < 1e-9:
                    results.append({
                        "sku": sku, "product_id": pid, "product_name": prod.get("name"),
                        "supplierinfo_id": sup["id"],
                        "status": "unchanged", "old_price": old_price, "new_price": new_price,
                        "message": "Price already matches, no update needed"
                    })
                    continue
                w = await odoo_write_record("product.supplierinfo", sup["id"],
                                            {"price": new_price})
                # Audit this write regardless of success or failure
                audit_extra = {
                    "sku": sku, "vendor_name": vendor_resolved_name,
                    "vendor_id": vendor_id, "product_id": pid,
                    "product_name": prod.get("name"),
                }
                if w.get("error"):
                    await audit_odoo_write(
                        who_uid=(context or {}).get("uid", 0),
                        who_name=(context or {}).get("username", ""),
                        tool_name="odoo_update_vendor_price",
                        model="product.supplierinfo",
                        record_id=sup["id"],
                        operation="update",
                        old_values={"price": old_price},
                        new_values={"price": new_price},
                        extra_info=audit_extra,
                        status=f"error: {w['error']}",
                    )
                    results.append({
                        "sku": sku, "product_id": pid, "product_name": prod.get("name"),
                        "supplierinfo_id": sup["id"],
                        "status": "error", "old_price": old_price, "new_price": new_price,
                        "message": f"Write failed: {w['error']}"
                    })
                else:
                    await audit_odoo_write(
                        who_uid=(context or {}).get("uid", 0),
                        who_name=(context or {}).get("username", ""),
                        tool_name="odoo_update_vendor_price",
                        model="product.supplierinfo",
                        record_id=sup["id"],
                        operation="update",
                        old_values={"price": old_price},
                        new_values={"price": new_price},
                        extra_info=audit_extra,
                        status="success",
                    )
                    results.append({
                        "sku": sku, "product_id": pid, "product_name": prod.get("name"),
                        "supplierinfo_id": sup["id"],
                        "status": "updated", "old_price": old_price, "new_price": new_price
                    })
            else:
                # No existing record → create one
                create_vals = {
                    "partner_id": vendor_id,
                    "price": new_price,
                    "min_qty": 0,
                }
                if tmpl_id is not None:
                    create_vals["product_tmpl_id"] = tmpl_id
                # Also pin to the specific variant when available
                create_vals["product_id"] = pid
                c = await odoo_create("product.supplierinfo", create_vals)
                audit_extra = {
                    "sku": sku, "vendor_name": vendor_resolved_name,
                    "vendor_id": vendor_id, "product_id": pid,
                    "product_name": prod.get("name"),
                }
                if c.get("error"):
                    await audit_odoo_write(
                        who_uid=(context or {}).get("uid", 0),
                        who_name=(context or {}).get("username", ""),
                        tool_name="odoo_update_vendor_price",
                        model="product.supplierinfo",
                        record_id=None,
                        operation="create",
                        old_values=None,
                        new_values=create_vals,
                        extra_info=audit_extra,
                        status=f"error: {c['error']}",
                    )
                    results.append({
                        "sku": sku, "product_id": pid, "product_name": prod.get("name"),
                        "status": "error", "new_price": new_price,
                        "message": f"Create failed: {c['error']}"
                    })
                else:
                    await audit_odoo_write(
                        who_uid=(context or {}).get("uid", 0),
                        who_name=(context or {}).get("username", ""),
                        tool_name="odoo_update_vendor_price",
                        model="product.supplierinfo",
                        record_id=c.get("id"),
                        operation="create",
                        old_values=None,
                        new_values=create_vals,
                        extra_info=audit_extra,
                        status="success",
                    )
                    results.append({
                        "sku": sku, "product_id": pid, "product_name": prod.get("name"),
                        "supplierinfo_id": c.get("id"),
                        "status": "created", "new_price": new_price,
                        "message": "No prior vendor record; created new one"
                    })

        summary = {
            "updated": sum(1 for r in results if r["status"] == "updated"),
            "created": sum(1 for r in results if r["status"] == "created"),
            "unchanged": sum(1 for r in results if r["status"] == "unchanged"),
            "not_found": sum(1 for r in results if r["status"] == "not_found"),
            "errors": sum(1 for r in results if r["status"] == "error"),
        }
        print(f"[UPDATE_VENDOR_PRICE] done vendor='{vendor_resolved_name}' summary={summary}")
        return json.dumps({
            "vendor_id": vendor_id,
            "vendor_name": vendor_resolved_name,
            "summary": summary,
            "results": results
        }, ensure_ascii=False)

    if name == "odoo_find_recent_purchases_by_skus":
        """
        Batch: given a list of SKUs and a date window, find which were purchased
        recently. Returns per-SKU purchase stats (count, total_qty, vendors,
        min/max/last price, last PO name, last date) AND an overall summary.

        Replaces a pattern where the AI would otherwise need 5-8 chained
        odoo_search calls (PO list → PO lines → filter → group). Here we do
        exactly 2 XML-RPC calls: one for PO headers, one for PO lines.

        Input:
          {
            "skus": ["CMEC072", "ALFN001", ...],     # required, 1-500 items
            "days_back": 120,                         # optional, default 120
            "since_date": "2025-12-31",               # optional; if provided, overrides days_back
            "include_cancelled": false                # optional, default false
          }

        Output JSON:
          {
            "since_date": "2025-12-31",
            "total_skus_requested": 50,
            "total_skus_with_purchases": 18,
            "total_pos_involved": 9,
            "total_amount": 12430.50,
            "results": [
              {
                "sku": "CMEC072",
                "product_id": 8397,
                "product_name": "74\" Post with Leveling Foot, Green Epoxy",
                "purchase_count": 3,        # number of POs that contain this SKU
                "total_qty": 48,            # sum of qty across those POs
                "vendors": ["Thunder Group"],
                "min_price": 4.50,
                "max_price": 9.27,
                "last_price": 9.27,         # price on most recent PO
                "last_po": "P00463",
                "last_po_id": 468,
                "last_date": "2026-04-23",
                "all_po_names": ["P00463", "P00435", "P00430"]
              },
              ...
              {"sku": "XYZ999", "product_id": null, "status": "sku_not_found"},
              ...
            ]
          }
        """
        import datetime as dt

        skus_in = inp.get("skus") or []
        if not isinstance(skus_in, list) or not skus_in:
            return json.dumps({"error": "skus must be a non-empty list of SKU strings"})
        if len(skus_in) > 500:
            return json.dumps({"error": f"Too many SKUs ({len(skus_in)}); max 500 per call"})

        # Normalize SKU input
        skus_clean = []
        for s in skus_in:
            if isinstance(s, str) and s.strip():
                skus_clean.append(s.strip())
        skus_clean = list(dict.fromkeys(skus_clean))  # dedupe, preserve order

        # Determine date cutoff
        since_date = (inp.get("since_date") or "").strip()
        if not since_date:
            days_back = int(inp.get("days_back") or 120)
            cutoff = dt.datetime.now() - dt.timedelta(days=days_back)
            since_date = cutoff.strftime("%Y-%m-%d")

        include_cancelled = bool(inp.get("include_cancelled", False))

        print(f"[FIND_PURCHASES] skus={len(skus_clean)} since={since_date} "
              f"include_cancelled={include_cancelled}")

        # --- Step 1: resolve SKUs → product IDs (1 XML-RPC call, bulk 'in') ---
        prod_r = json.loads(await odoo_query(
            "product.product",
            [["default_code", "in", skus_clean], ["active", "=", True]],
            ["id", "name", "default_code"],
            limit=2000
        ))
        prod_r = prod_r if isinstance(prod_r, list) else []

        # Map SKU (uppercase) → product row; keep first match for each
        sku_to_prod = {}
        for p in prod_r:
            code = (p.get("default_code") or "").strip().upper()
            if code and code not in sku_to_prod:
                sku_to_prod[code] = p

        # SKUs not found
        not_found_skus = [s for s in skus_clean if s.upper() not in sku_to_prod]

        product_ids = [p["id"] for p in sku_to_prod.values()]
        if not product_ids:
            return json.dumps({
                "since_date": since_date,
                "total_skus_requested": len(skus_clean),
                "total_skus_with_purchases": 0,
                "total_pos_involved": 0,
                "total_amount": 0,
                "results": [{"sku": s, "status": "sku_not_found"} for s in not_found_skus]
            }, ensure_ascii=False)

        # --- Step 2: fetch PO lines for these products since cutoff (1 XML-RPC call) ---
        # We query purchase.order.line directly, using its related fields:
        #   order_id.date_order, order_id.state, order_id.name, order_id.partner_id
        # This is 1 request that returns exactly what we need.
        po_state_filter = []
        if not include_cancelled:
            po_state_filter = [["order_id.state", "!=", "cancel"]]

        pol_domain = (
            [["product_id", "in", product_ids]]
            + [["order_id.date_order", ">=", since_date]]
            + [["order_id.company_id", "=", 1]]
            + po_state_filter
        )
        pol_fields = [
            "id", "order_id", "product_id", "product_qty", "price_unit",
            "price_subtotal", "date_planned"
        ]
        pol_r = json.loads(await odoo_query(
            "purchase.order.line",
            pol_domain,
            pol_fields,
            limit=5000,
            order="id desc"
        ))
        pol_r = pol_r if isinstance(pol_r, list) else []
        print(f"[FIND_PURCHASES] got {len(pol_r)} PO lines")

        # --- Step 3: enrich with PO header info (1 more XML-RPC call) ---
        order_ids = list({ln["order_id"][0] for ln in pol_r if ln.get("order_id")})
        po_headers = {}
        if order_ids:
            po_r = json.loads(await odoo_query(
                "purchase.order",
                [["id", "in", order_ids]],
                ["id", "name", "date_order", "state", "partner_id"],
                limit=len(order_ids) + 10
            ))
            po_r = po_r if isinstance(po_r, list) else []
            for po in po_r:
                po_headers[po["id"]] = po

        # --- Step 4: group PO lines by product_id, compute stats ---
        by_product = {}  # product_id → { po_names: set, qtys: list, prices: list, vendors: set, last_{date,price,po}: ... }
        for ln in pol_r:
            pid = ln["product_id"][0] if ln.get("product_id") else None
            oid = ln["order_id"][0] if ln.get("order_id") else None
            if not pid or not oid:
                continue
            po = po_headers.get(oid)
            if not po:
                continue
            entry = by_product.setdefault(pid, {
                "po_names": [],
                "po_ids": [],
                "qtys": [],
                "prices": [],
                "subtotals": [],
                "vendors": set(),
                "last_date": None,
                "last_price": None,
                "last_po": None,
                "last_po_id": None,
            })
            po_name = po.get("name") or ""
            if po_name not in entry["po_names"]:
                entry["po_names"].append(po_name)
                entry["po_ids"].append(oid)
            entry["qtys"].append(float(ln.get("product_qty") or 0))
            entry["prices"].append(float(ln.get("price_unit") or 0))
            entry["subtotals"].append(float(ln.get("price_subtotal") or 0))
            if po.get("partner_id"):
                entry["vendors"].add(po["partner_id"][1])
            po_date = (po.get("date_order") or "")[:10]
            if po_date and (entry["last_date"] is None or po_date > entry["last_date"]):
                entry["last_date"] = po_date
                entry["last_price"] = float(ln.get("price_unit") or 0)
                entry["last_po"] = po_name
                entry["last_po_id"] = oid

        # --- Step 5: build per-SKU result rows ---
        results = []
        all_po_ids = set()
        total_amount = 0.0
        skus_with_purchases = 0
        for sku in skus_clean:
            prod = sku_to_prod.get(sku.upper())
            if not prod:
                results.append({"sku": sku, "status": "sku_not_found"})
                continue
            pid = prod["id"]
            entry = by_product.get(pid)
            if not entry:
                results.append({
                    "sku": sku,
                    "product_id": pid,
                    "product_name": prod.get("name"),
                    "status": "no_purchases_in_window",
                    "purchase_count": 0,
                    "total_qty": 0,
                    "vendors": [],
                    "min_price": None,
                    "max_price": None,
                    "last_price": None,
                    "last_po": None,
                    "last_po_id": None,
                    "last_date": None,
                    "all_po_names": []
                })
                continue
            skus_with_purchases += 1
            all_po_ids.update(entry["po_ids"])
            total_amount += sum(entry["subtotals"])
            results.append({
                "sku": sku,
                "product_id": pid,
                "product_name": prod.get("name"),
                "status": "found",
                "purchase_count": len(entry["po_names"]),
                "total_qty": sum(entry["qtys"]),
                "vendors": sorted(entry["vendors"]),
                "min_price": min(entry["prices"]) if entry["prices"] else None,
                "max_price": max(entry["prices"]) if entry["prices"] else None,
                "last_price": entry["last_price"],
                "last_po": entry["last_po"],
                "last_po_id": entry["last_po_id"],
                "last_date": entry["last_date"],
                "all_po_names": entry["po_names"]
            })

        output = {
            "since_date": since_date,
            "include_cancelled": include_cancelled,
            "total_skus_requested": len(skus_clean),
            "total_skus_with_purchases": skus_with_purchases,
            "total_skus_not_found_in_odoo": len(not_found_skus),
            "total_pos_involved": len(all_po_ids),
            "total_amount": round(total_amount, 2),
            "results": results
        }
        print(f"[FIND_PURCHASES] done: {skus_with_purchases}/{len(skus_clean)} skus "
              f"have purchases in {len(all_po_ids)} POs, total ${round(total_amount,2)}")
        return json.dumps(output, ensure_ascii=False)

    if name == "odoo_match_payment_to_customer":
        """
        Match a received payment amount to customer invoices / SOs / payment records.

        USE THIS TOOL WHEN:
          - User says "我收到一笔 $X 的钱，不知道是哪个客户/发票"
          - User uploads a bank screenshot and asks which invoice it matches
          - User asks "哪个 SO 对应这笔 $X"
          - Any "which customer sent this money" question

        DOES NOT DO WRITES. This is read-only discovery. If user then wants to
        actually reconcile/apply the payment, they do it in Odoo UI.

        Input:
          {
            "amount":            3034.98,         # required, USD amount received
            "date":              "2026-03-24",    # optional, when money arrived
            "date_window_days":  14,              # optional, default 14
            "customer_hint":     "CARLOS RODRIGUEZ",  # optional, name from bank memo
            "max_candidates":    10               # optional, default 10
          }

        Tolerance for amount matching:
          $20 absolute — covers tax rate fluctuations and small shipping additions.

        Search priority (mirrors real reconciliation workflow):
          Phase 1: account.payment + account.move (in_payment/not_paid/partial invoices)
          Phase 2A: in_payment invoices — check total/residual/paid-portion against amount
          Phase 2B: uninvoiced SOs — customer may have paid before invoice was created
          Phase 2C: paid invoices — possible mis-reconciliation from earlier

        Returns candidates sorted by match_score (high → low), each with type
        (payment/invoice_total/invoice_residual/invoice_in_payment/uninvoiced_so/
        invoice_paid_check), amount, date, partner, linked SO if any, and
        match_reasons. Plus a best_candidate and summary.
        """
        import datetime as dt

        # ── Parse inputs ──────────────────────────────────────────
        try:
            amount = float(inp.get("amount"))
        except (TypeError, ValueError):
            return json.dumps({"error": "amount is required and must be a number"})
        if amount <= 0:
            return json.dumps({"error": "amount must be positive"})

        received_date_str = (inp.get("date") or "").strip()
        received_date = None
        if received_date_str:
            try:
                received_date = dt.datetime.strptime(received_date_str, "%Y-%m-%d").date()
            except ValueError:
                return json.dumps({"error": "date must be YYYY-MM-DD"})

        date_window = int(inp.get("date_window_days") or 14)
        if date_window < 1: date_window = 1
        if date_window > 180: date_window = 180

        customer_hint = (inp.get("customer_hint") or "").strip()
        max_candidates = int(inp.get("max_candidates") or 10)

        # Amount tolerance: $20 to cover tax rate fluctuations.
        # Real-world scenario: invoice is $165 but customer paid $155 or $175
        # because tax was calculated differently, or customer added shipping.
        tolerance = 20.0
        amount_low = round(amount - tolerance, 2)
        amount_high = round(amount + tolerance, 2)

        # Scoring bands for amount closeness
        near_exact_tol = 1.0  # within $1 = very strong match

        print(f"[MATCH_PAYMENT] amount={amount} tolerance=±${tolerance:.2f} "
              f"window=±{date_window}d hint='{customer_hint}' date={received_date_str}")

        # ── Build date window for searches ─────────────────────
        date_from = date_to = None
        if received_date:
            date_from = (received_date - dt.timedelta(days=date_window)).strftime("%Y-%m-%d")
            date_to = (received_date + dt.timedelta(days=date_window)).strftime("%Y-%m-%d")
        else:
            # No date given → search last 90 days as a reasonable default.
            date_from = (dt.datetime.now() - dt.timedelta(days=90)).strftime("%Y-%m-%d")
            date_to = dt.datetime.now().strftime("%Y-%m-%d")

        # ── 1) Resolve customer_hint → partner_ids (if given) ──
        hint_partner_ids = []
        hint_partner_names = {}
        if customer_hint:
            # Try several matching strategies
            hint_clean = customer_hint.strip()
            # Strategy 1: exact (case-insensitive)
            p_r = json.loads(await odoo_query(
                "res.partner",
                ["|", ["name", "=ilike", hint_clean],
                      ["name", "ilike", hint_clean]],
                ["id", "name"], limit=20
            ))
            p_r = p_r if isinstance(p_r, list) else []
            # Strategy 2: first word only (e.g. "CARLOS" from "CARLOS RODRIGUEZ PINTO")
            if not p_r:
                first_word = hint_clean.split()[0] if hint_clean.split() else hint_clean
                p_r = json.loads(await odoo_query(
                    "res.partner",
                    [["name", "ilike", first_word]],
                    ["id", "name"], limit=20
                ))
                p_r = p_r if isinstance(p_r, list) else []
            for p in p_r:
                hint_partner_ids.append(p["id"])
                hint_partner_names[p["id"]] = p["name"]
            print(f"[MATCH_PAYMENT] customer_hint resolved to {len(hint_partner_ids)} partners: "
                  f"{list(hint_partner_names.values())[:5]}")

        # ── 2) Search account.payment ──────────────────────────
        # Inbound customer payments in the amount window + date window.
        pay_domain = [
            ["amount", ">=", amount_low],
            ["amount", "<=", amount_high],
            ["date", ">=", date_from],
            ["date", "<=", date_to],
            ["company_id", "=", 1],
            ["partner_type", "=", "customer"],  # only inbound customer payments
        ]
        pay_r = json.loads(await odoo_query(
            "account.payment",
            pay_domain,
            ["id", "name", "amount", "date", "partner_id", "state",
             "ref", "reconciled_invoice_ids", "payment_type", "journal_id"],
            limit=100, order="date desc"
        ))
        pay_r = pay_r if isinstance(pay_r, list) else []
        print(f"[MATCH_PAYMENT] account.payment: {len(pay_r)} candidates")

        # ── 3) Search account.move (invoices) ──────────────────
        # By amount_total AND by amount_residual — customer might pay full or partial.
        inv_total_domain = [
            ["amount_total", ">=", amount_low],
            ["amount_total", "<=", amount_high],
            ["invoice_date", ">=", date_from],
            ["invoice_date", "<=", date_to],
            ["company_id", "=", 1],
            ["state", "=", "posted"],
            ["move_type", "in", ["out_invoice", "out_refund"]],
        ]
        inv_r_total = json.loads(await odoo_query(
            "account.move",
            inv_total_domain,
            ["id", "name", "amount_total", "amount_residual", "invoice_date",
             "partner_id", "payment_state", "invoice_origin", "ref", "move_type", "x_payment_method"],
            limit=100, order="invoice_date desc"
        ))
        inv_r_total = inv_r_total if isinstance(inv_r_total, list) else []

        inv_residual_domain = [
            ["amount_residual", ">=", amount_low],
            ["amount_residual", "<=", amount_high],
            ["invoice_date", ">=", date_from],
            ["invoice_date", "<=", date_to],
            ["company_id", "=", 1],
            ["state", "=", "posted"],
            ["move_type", "in", ["out_invoice"]],
            ["payment_state", "in", ["not_paid", "partial", "in_payment"]],
        ]
        inv_r_residual = json.loads(await odoo_query(
            "account.move",
            inv_residual_domain,
            ["id", "name", "amount_total", "amount_residual", "invoice_date",
             "partner_id", "payment_state", "invoice_origin", "ref", "move_type", "x_payment_method"],
            limit=100, order="invoice_date desc"
        ))
        inv_r_residual = inv_r_residual if isinstance(inv_r_residual, list) else []
        print(f"[MATCH_PAYMENT] account.move: {len(inv_r_total)} by total, "
              f"{len(inv_r_residual)} by residual")

        # NOTE: We intentionally do NOT search account.bank.statement.line.
        # The user is typically asking this question WHILE LOOKING AT a bank
        # statement line — matching a bank line against itself just echoes
        # their input back as the "answer". The real question is "which
        # invoice / SO / payment_record does this money correspond to" —
        # so we only search account.payment and account.move.
        print(f"[MATCH_PAYMENT] (bank.statement.line search intentionally skipped)")

        # ── 4a) Phase 2: in_payment invoices — check their linked payments ──
        # User's real workflow: "我收到一笔钱，先看 in_payment 的发票里有没有对应的"
        # We query in_payment invoices (broader date range), then check if their
        # associated payment amounts match.
        inv_inpayment_domain = [
            ["payment_state", "=", "in_payment"],
            ["company_id", "=", 1],
            ["state", "=", "posted"],
            ["move_type", "in", ["out_invoice"]],
        ]
        if date_from:
            inv_inpayment_domain.append(["invoice_date", ">=", date_from])
        if date_to:
            inv_inpayment_domain.append(["invoice_date", "<=", date_to])
        inv_inpayment_r = json.loads(await odoo_query(
            "account.move",
            inv_inpayment_domain,
            ["id", "name", "amount_total", "amount_residual", "invoice_date",
             "partner_id", "payment_state", "invoice_origin", "ref", "move_type", "x_payment_method"],
            limit=200, order="invoice_date desc"
        ))
        inv_inpayment_r = inv_inpayment_r if isinstance(inv_inpayment_r, list) else []

        # For in_payment invoices, check if any associated payment amount matches
        # (the payment amount might differ from invoice total due to tax/shipping)
        inpayment_matches = []
        for inv in inv_inpayment_r:
            inv_total = float(inv.get("amount_total") or 0)
            inv_residual = float(inv.get("amount_residual") or 0)
            inv_paid = inv_total - inv_residual  # how much has been paid so far
            # Check: does the received amount match either total, residual, or paid portion?
            for check_amt, label in [
                (inv_total, "发票总额"),
                (inv_residual, "未付余额"),
                (inv_paid, "已付部分"),
            ]:
                diff = abs(check_amt - amount)
                if diff <= tolerance and check_amt > 0:
                    inpayment_matches.append({
                        **inv,
                        "_match_amount": check_amt,
                        "_match_label": label,
                        "_match_diff": diff,
                    })
                    break  # only keep the best match per invoice
        print(f"[MATCH_PAYMENT] in_payment invoices checked: {len(inv_inpayment_r)} total, "
              f"{len(inpayment_matches)} amount-matched")

        # ── 4b) Phase 2: uninvoiced SOs — customer may have paid without invoice ──
        # Scenario: customer paid for shipping supplement, or paid before invoice was created
        so_domain = [
            ["amount_total", ">=", amount_low],
            ["amount_total", "<=", amount_high],
            ["company_id", "=", 1],
            ["state", "in", ["sale", "done"]],
            ["invoice_status", "in", ["no", "to invoice"]],  # no invoice created yet
        ]
        if date_from:
            so_domain.append(["date_order", ">=", date_from])
        if date_to:
            so_domain.append(["date_order", "<=", date_to])
        try:
            so_r = json.loads(await odoo_query(
                "sale.order",
                so_domain,
                ["id", "name", "amount_total", "date_order", "partner_id",
                 "state", "invoice_status", "user_id"],
                limit=100, order="date_order desc"
            ))
            so_r = so_r if isinstance(so_r, list) else []
        except Exception as e:
            print(f"[MATCH_PAYMENT] SO query error: {e}")
            so_r = []
        print(f"[MATCH_PAYMENT] uninvoiced SOs: {len(so_r)} candidates")

        # ── 4c) Phase 2: paid invoices — might be a reconciliation error ──
        # Scenario: this payment was already matched to the wrong invoice.
        # "paid" invoices that are close in amount might be the real match.
        inv_paid_domain = [
            ["payment_state", "=", "paid"],
            ["company_id", "=", 1],
            ["state", "=", "posted"],
            ["move_type", "in", ["out_invoice"]],
            ["amount_total", ">=", amount_low],
            ["amount_total", "<=", amount_high],
        ]
        if date_from:
            inv_paid_domain.append(["invoice_date", ">=", date_from])
        if date_to:
            inv_paid_domain.append(["invoice_date", "<=", date_to])
        inv_paid_r = json.loads(await odoo_query(
            "account.move",
            inv_paid_domain,
            ["id", "name", "amount_total", "amount_residual", "invoice_date",
             "partner_id", "payment_state", "invoice_origin", "ref", "move_type", "x_payment_method"],
            limit=100, order="invoice_date desc"
        ))
        inv_paid_r = inv_paid_r if isinstance(inv_paid_r, list) else []
        # Dedupe: exclude any already found in inv_r_total
        seen_inv = {inv["id"] for inv in inv_r_total} | {inv["id"] for inv in inv_r_residual}
        inv_paid_r = [inv for inv in inv_paid_r if inv["id"] not in seen_inv]
        print(f"[MATCH_PAYMENT] paid invoices (possible mis-reconciliation): {len(inv_paid_r)} candidates")

        # ── 5) Score and build candidates ──────────────────────
        candidates = []

        def amount_score(cand_amount):
            """Score based on how close the candidate amount is to the target.
            $20 tolerance covers tax rate fluctuations and small shipping additions."""
            diff = abs(cand_amount - amount)
            if diff <= near_exact_tol:
                return 60, "金额精确匹配"
            elif diff <= 5.0:
                return 45, f"金额差 ${diff:.2f} (小幅差异)"
            elif diff <= 10.0:
                return 35, f"金额差 ${diff:.2f} (可能是税率差异)"
            elif diff <= tolerance:
                return 20, f"金额差 ${diff:.2f} (在 ${tolerance:.0f} 容差内，可能含运费/税差)"
            return 0, f"金额差 ${diff:.2f}"

        def date_score(cand_date_str):
            if not received_date or not cand_date_str:
                return 0, ""
            try:
                cand_date = dt.datetime.strptime(cand_date_str[:10], "%Y-%m-%d").date()
                diff_days = abs((cand_date - received_date).days)
            except Exception:
                return 0, ""
            if diff_days == 0:
                return 30, "同一天"
            elif diff_days <= 3:
                return 20, f"±{diff_days} 天"
            elif diff_days <= 7:
                return 10, f"±{diff_days} 天"
            elif diff_days <= 14:
                return 5, f"±{diff_days} 天"
            return 0, f"±{diff_days} 天"

        def customer_score(partner_id, partner_name):
            if not customer_hint:
                return 0, ""
            if partner_id in hint_partner_ids:
                hint_lower = customer_hint.strip().lower()
                pname_lower = (partner_name or "").strip().lower()
                if hint_lower == pname_lower:
                    return 25, f"客户名精确匹配 '{partner_name}'"
                elif hint_lower in pname_lower or pname_lower in hint_lower:
                    return 20, f"客户名包含 '{partner_name}'"
                else:
                    return 15, f"客户名模糊匹配 '{partner_name}'"
            # Not in hint list but do a fuzzy check on the name
            hint_lower = customer_hint.strip().lower()
            pname_lower = (partner_name or "").strip().lower()
            if hint_lower and pname_lower:
                hint_first = hint_lower.split()[0] if hint_lower.split() else hint_lower
                if hint_first in pname_lower or pname_lower.startswith(hint_first):
                    return 10, f"客户名部分包含 '{partner_name}'"
            return 0, ""

        def payment_method_score(x_payment_method_val):
            """Score based on invoice's x_payment_method field.
            When user mentions Zelle/Wire/bank transfer, Zelle-method invoices
            get a boost; Cash-method invoices get a penalty."""
            if not x_payment_method_val:
                return 0, ""
            pm = str(x_payment_method_val).lower()
            if "zelle" in pm:
                return 10, "付款方式: Zelle"
            elif "ach" in pm:
                return 8, "付款方式: ACH"
            elif "check" in pm:
                return 5, "付款方式: Check"
            elif "stripe" in pm:
                return 0, "付款方式: Stripe (刷卡)"
            elif "cash" in pm:
                return -10, "付款方式: Cash (非 Zelle/Wire)"
            elif "amazon" in pm or "ebay" in pm or "shopify" in pm or "walmart" in pm:
                return -8, f"付款方式: {x_payment_method_val} (平台收款)"
            return 0, f"付款方式: {x_payment_method_val}"

        odoo_web_base = ODOO_URL.rstrip("/")

        # -- 5a. account.payment candidates --
        for p in pay_r:
            c_amt = float(p.get("amount") or 0)
            c_date = p.get("date") or ""
            partner = p.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue  # out of tolerance, skip
            reasons.append(a_reason)
            d_score, d_reason = date_score(c_date)
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score, pm_reason = payment_method_score(inv.get("x_payment_method"))
            if pm_reason: reasons.append(pm_reason)
            type_bonus = 5  # payment record is the most direct evidence

            # Journal-based scoring: if user mentions "zelle/wire/bank transfer",
            # penalize Cash journal payments and boost Bank journal payments.
            journal = p.get("journal_id")
            journal_name = (journal[1] if journal else "").lower()
            journal_bonus = 0
            if "cash" in journal_name:
                journal_bonus = -15  # Cash payment cannot be Zelle/Wire
                reasons.append(f"⚠ Cash journal (非 Zelle/Wire)")
            elif "bank" in journal_name:
                journal_bonus = 5
                reasons.append(f"Bank journal")
            else:
                reasons.append(f"Journal: {journal[1] if journal else '?'}")

            reasons.append("付款单记录")

            # Get linked invoices
            linked_inv = p.get("reconciled_invoice_ids") or []

            total_score = a_score + d_score + cu_score + pm_score + type_bonus + journal_bonus
            candidates.append({
                "type": "payment",
                "match_score": total_score,
                "record_id": p["id"],
                "name": p.get("name"),
                "amount": round(c_amt, 2),
                "date": c_date,
                "partner_id": partner_id,
                "partner_name": partner_name,
                "state": p.get("state"),
                "journal": journal[1] if journal else "",
                "payment_ref": p.get("ref") or "",
                "linked_invoice_ids": linked_inv,
                "match_reasons": reasons,
                "odoo_link": f"{odoo_web_base}/odoo/payments/{p['id']}",
            })

        # -- 5b. account.move (by amount_total) candidates --
        seen_invoice_ids = set()
        for inv in inv_r_total:
            seen_invoice_ids.add(inv["id"])
            c_amt = float(inv.get("amount_total") or 0)
            c_date = inv.get("invoice_date") or ""
            partner = inv.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue
            reasons.append(a_reason + " (发票总额)")
            d_score, d_reason = date_score(c_date)
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score, pm_reason = payment_method_score(inv.get("x_payment_method"))
            if pm_reason: reasons.append(pm_reason)
            type_bonus = 0
            reasons.append(f"发票 ({inv.get('payment_state','?')})")

            total_score = a_score + d_score + cu_score + pm_score + type_bonus
            candidates.append({
                "type": "invoice_total",
                "match_score": total_score,
                "record_id": inv["id"],
                "name": inv.get("name"),
                "amount": round(c_amt, 2),
                "amount_residual": round(float(inv.get("amount_residual") or 0), 2),
                "date": c_date,
                "partner_id": partner_id,
                "partner_name": partner_name,
                "payment_state": inv.get("payment_state"),
                "payment_method": inv.get("x_payment_method") or "",
                "linked_so": inv.get("invoice_origin") or "",
                "ref": inv.get("ref") or "",
                "match_reasons": reasons,
                "odoo_link": f"{odoo_web_base}/odoo/account-move/{inv['id']}",
            })

        # -- 5c. account.move (by amount_residual) candidates --
        for inv in inv_r_residual:
            if inv["id"] in seen_invoice_ids:
                continue  # already covered by amount_total branch
            c_amt = float(inv.get("amount_residual") or 0)
            c_date = inv.get("invoice_date") or ""
            partner = inv.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue
            reasons.append(a_reason + " (未付余额)")
            d_score, d_reason = date_score(c_date)
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score, pm_reason = payment_method_score(inv.get("x_payment_method"))
            if pm_reason: reasons.append(pm_reason)
            type_bonus = 3  # residual-match often indicates partial payment flow
            reasons.append(f"分期付款发票 ({inv.get('payment_state','?')})")

            total_score = a_score + d_score + cu_score + pm_score + type_bonus
            candidates.append({
                "type": "invoice_residual",
                "match_score": total_score,
                "record_id": inv["id"],
                "name": inv.get("name"),
                "amount": round(float(inv.get("amount_total") or 0), 2),
                "amount_residual": round(c_amt, 2),
                "date": c_date,
                "partner_id": partner_id,
                "partner_name": partner_name,
                "payment_state": inv.get("payment_state"),
                "linked_so": inv.get("invoice_origin") or "",
                "ref": inv.get("ref") or "",
                "match_reasons": reasons,
                "odoo_link": f"{odoo_web_base}/odoo/account-move/{inv['id']}",
            })

        # (bank.statement.line branch removed — see note above about not
        # echoing the user's own input as an answer.)

        # -- 5d. in_payment invoice matches (Phase 2A) --
        for inv in inpayment_matches:
            c_amt = float(inv.get("_match_amount") or 0)
            c_date = inv.get("invoice_date") or ""
            partner = inv.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue
            reasons.append(a_reason + f" ({inv['_match_label']})")
            d_score, d_reason = date_score(c_date)
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score, pm_reason = payment_method_score(inv.get("x_payment_method"))
            if pm_reason: reasons.append(pm_reason)
            type_bonus = 8  # in_payment invoices are the PRIMARY search target
            reasons.append(f"in_payment 发票 — 优先匹配")

            total_score = a_score + d_score + cu_score + pm_score + type_bonus
            # Avoid duplicate if same invoice already in candidates
            if inv["id"] not in {c.get("record_id") for c in candidates if c.get("type","").startswith("invoice")}:
                candidates.append({
                    "type": "invoice_in_payment",
                    "match_score": total_score,
                    "record_id": inv["id"],
                    "name": inv.get("name"),
                    "amount": round(float(inv.get("amount_total") or 0), 2),
                    "amount_residual": round(float(inv.get("amount_residual") or 0), 2),
                    "matched_on": inv["_match_label"],
                    "matched_amount": round(c_amt, 2),
                    "date": c_date,
                    "partner_id": partner_id,
                    "partner_name": partner_name,
                    "payment_state": inv.get("payment_state"),
                    "linked_so": inv.get("invoice_origin") or "",
                    "ref": inv.get("ref") or "",
                    "match_reasons": reasons,
                    "odoo_link": f"{odoo_web_base}/odoo/account-move/{inv['id']}",
                })

        # -- 5e. uninvoiced SOs (Phase 2B) --
        for so in so_r:
            c_amt = float(so.get("amount_total") or 0)
            c_date = so.get("date_order") or ""
            partner = so.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue
            reasons.append(a_reason)
            d_score, d_reason = date_score(c_date[:10] if c_date else "")
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score = 0  # SOs don't have x_payment_method
            type_bonus = 2
            salesperson = so["user_id"][1] if so.get("user_id") else ""
            inv_status = so.get("invoice_status") or ""
            reasons.append(f"未开票 SO (invoice_status={inv_status})")
            if salesperson:
                reasons.append(f"销售员: {salesperson}")

            total_score = a_score + d_score + cu_score + pm_score + type_bonus
            candidates.append({
                "type": "uninvoiced_so",
                "match_score": total_score,
                "record_id": so["id"],
                "name": so.get("name"),
                "amount": round(c_amt, 2),
                "date": c_date[:10] if c_date else "",
                "partner_id": partner_id,
                "partner_name": partner_name,
                "so_state": so.get("state"),
                "invoice_status": inv_status,
                "salesperson": salesperson,
                "match_reasons": reasons,
                "odoo_link": f"{odoo_web_base}/odoo/sales/{so['id']}",
            })

        # -- 5f. paid invoices — possible mis-reconciliation (Phase 2C) --
        for inv in inv_paid_r:
            c_amt = float(inv.get("amount_total") or 0)
            c_date = inv.get("invoice_date") or ""
            partner = inv.get("partner_id")
            partner_id = partner[0] if partner else None
            partner_name = partner[1] if partner else ""
            reasons = []
            a_score, a_reason = amount_score(c_amt)
            if a_score == 0: continue
            reasons.append(a_reason)
            d_score, d_reason = date_score(c_date)
            if d_reason: reasons.append(d_reason)
            cu_score, cu_reason = customer_score(partner_id, partner_name)
            if cu_reason: reasons.append(cu_reason)
            pm_score, pm_reason = payment_method_score(inv.get("x_payment_method"))
            if pm_reason: reasons.append(pm_reason)
            type_bonus = -3  # lower priority: already paid, might be wrong reconciliation
            reasons.append("已付清发票 — 可能对账对错")

            total_score = a_score + d_score + cu_score + pm_score + type_bonus
            if total_score > 0:
                candidates.append({
                    "type": "invoice_paid_check",
                    "match_score": total_score,
                    "record_id": inv["id"],
                    "name": inv.get("name"),
                    "amount": round(c_amt, 2),
                    "amount_residual": round(float(inv.get("amount_residual") or 0), 2),
                    "date": c_date,
                    "partner_id": partner_id,
                    "partner_name": partner_name,
                    "payment_state": inv.get("payment_state"),
                    "linked_so": inv.get("invoice_origin") or "",
                    "ref": inv.get("ref") or "",
                    "match_reasons": reasons,
                    "odoo_link": f"{odoo_web_base}/odoo/account-move/{inv['id']}",
                })

        # ── 6) Sort, dedupe, pick top ──────────────────────────
        candidates.sort(key=lambda c: c["match_score"], reverse=True)
        top_candidates = candidates[:max_candidates]

        # ── 7) Enrich top candidates with linked SO info ───────
        # For invoice candidates, invoice_origin is often "SO..." — surface it.
        # Also for top candidate, pull the full SO if we can.
        best = top_candidates[0] if top_candidates else None

        # ── 8) Build human-readable summary line ───────────────
        if not candidates:
            summary_line = (
                f"在 Odoo 中未找到金额 ${amount_low:.2f} ~ ${amount_high:.2f}、"
                f"日期 {date_from} ~ {date_to} 的客户付款单或发票。"
                f"这笔钱可能还没被录入 Odoo（Zelle/Wire 常需财务手工补录），"
                f"或者金额/日期有偏差。建议提供客户名或确认一下金额日期。"
            )
        elif best["match_score"] >= 80:
            summary_line = (f"最可能: {best['partner_name']} 的 {best['name']} "
                            f"(${best['amount']}, {best['date']}, 分数 {best['match_score']})。"
                            f"共 {len(candidates)} 个候选。")
        elif best["match_score"] >= 50:
            summary_line = (f"最可能（置信度中等）: {best['partner_name']} 的 {best['name']} "
                            f"(${best['amount']}, {best['date']}, 分数 {best['match_score']})。"
                            f"共 {len(candidates)} 个候选，建议对比前几条确认。")
        else:
            summary_line = (f"找到 {len(candidates)} 个弱匹配候选，置信度较低。最高分: "
                            f"{best['partner_name']} 的 {best['name']} (${best['amount']}, 分数 {best['match_score']})。"
                            f"建议提供更多信息（客户名、准确日期）缩小范围。")

        output = {
            "amount_searched": amount,
            "tolerance_used": round(tolerance, 2),
            "search_range": [amount_low, amount_high],
            "date_range": [date_from, date_to],
            "customer_hint": customer_hint or None,
            "total_candidates_found": len(candidates),
            "best_candidate": best,
            "candidates": top_candidates,
            "summary": summary_line,
        }
        print(f"[MATCH_PAYMENT] done: {len(candidates)} candidates, "
              f"top_score={best['match_score'] if best else 'N/A'}")
        return json.dumps(output, ensure_ascii=False, default=str)

    if name == "get_po_with_so_links":
        """Get a PO's product lines and find matching SO records within a date range."""
        import datetime as dt
        po_name = inp.get("po_name", "").strip()
        days_back = inp.get("days_back", 30)
        include_all_so = inp.get("include_all_so", False)
        salesperson_filter = inp.get("salesperson", "").strip()  # optional: filter SOs by salesperson name
        only_with_so = bool(inp.get("only_with_so", False))  # hide products that had no matching SO

        if not po_name:
            return json.dumps({"error": "po_name is required (e.g. 'P00461')"})

        try:
            # Step 1: Get PO by NAME (not id) to avoid id/name confusion
            po_r = json.loads(await odoo_query("purchase.order",
                [["name", "=", po_name], ["company_id", "=", 1]],
                ["id", "name", "partner_id", "date_order", "state", "amount_total"],
                limit=1))
            if not po_r or isinstance(po_r, dict):
                return json.dumps({"error": f"PO '{po_name}' not found"})
            po = po_r[0]
            po_id = po["id"]
            print(f"GET_PO_SO: found {po_name} (db_id={po_id})")

            # Step 2: Get PO lines by po_id
            lines_r = json.loads(await odoo_query("purchase.order.line",
                [["order_id", "=", po_id]],
                ["id", "product_id", "product_qty", "price_unit", "product_uom"],
                limit=200))
            if not lines_r or isinstance(lines_r, dict):
                return json.dumps({"error": f"No lines found for {po_name}", "po": po})

            # Step 3: Get product details (id + default_code + name)
            product_ids = list({l["product_id"][0] for l in lines_r if l.get("product_id")})
            prod_r = json.loads(await odoo_query("product.product",
                [["id", "in", product_ids]],
                ["id", "name", "default_code"], limit=200))
            prod_map = {p["id"]: p for p in prod_r} if isinstance(prod_r, list) else {}

            # Step 4: Find SOs containing these products
            date_from = (dt.datetime.now() - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")
            sol_r = json.loads(await odoo_query("sale.order.line",
                [["product_id", "in", product_ids]],
                ["order_id", "product_id", "product_uom_qty", "price_unit"],
                limit=1000))
            so_ids = list({l["order_id"][0] for l in sol_r if l.get("order_id")}) if isinstance(sol_r, list) else []

            # Step 5: Filter SOs by date at sale.order level
            so_domain = [["id", "in", so_ids], ["company_id", "=", 1]]
            if not include_all_so:
                so_domain.append(["date_order", ">=", date_from])
            if salesperson_filter:
                so_domain.append(["user_id.name", "ilike", salesperson_filter])
            so_r = json.loads(await odoo_query("sale.order",
                so_domain,
                ["id", "name", "partner_id", "date_order", "state", "amount_total", "user_id"],
                limit=200, order="date_order desc"))
            so_map = {s["id"]: s for s in so_r} if isinstance(so_r, list) else {}

            # Step 6: Build SOL map for matched SOs
            matched_so_ids = list(so_map.keys())
            sol_filtered = [l for l in (sol_r if isinstance(sol_r, list) else [])
                           if l.get("order_id") and l["order_id"][0] in matched_so_ids]

            # Build result: PO lines with matching SOs per product
            result_lines = []
            for line in lines_r:
                pid = line["product_id"][0] if line.get("product_id") else None
                prod = prod_map.get(pid, {})
                sku = prod.get("default_code", "")
                prod_name = prod.get("name", line["product_id"][1] if line.get("product_id") else "")

                # Find SOs that contain this product
                matching_sos = []
                for sol in sol_filtered:
                    if sol.get("product_id") and sol["product_id"][0] == pid:
                        so_id = sol["order_id"][0]
                        so = so_map.get(so_id, {})
                        if so:
                            matching_sos.append({
                                "so_name": so.get("name"),
                                "customer": so["partner_id"][1] if so.get("partner_id") else "",
                                "salesperson": so["user_id"][1] if so.get("user_id") else "",
                                "date": so.get("date_order", "")[:10],
                                "state": so.get("state"),
                                "qty_sold": sol.get("product_uom_qty"),
                                "price_sold": sol.get("price_unit"),
                            })

                result_lines.append({
                    "sku": sku,
                    "product_name": prod_name,
                    "po_qty": line.get("product_qty"),
                    "po_price": line.get("price_unit"),
                    "so_count": len(matching_sos),
                    "recent_sos": matching_sos[:10],  # max 10 per product
                })

            # Optional: drop products that had no matching SO (when user only cares
            # about "what sold", not "every line of the PO"). Default off to keep
            # backward compatibility.
            total_products_in_po = len(result_lines)
            if only_with_so:
                result_lines = [r for r in result_lines if r.get("so_count", 0) > 0]
            products_shown = len(result_lines)

            vendor = po["partner_id"][1] if po.get("partner_id") else ""
            return json.dumps({
                "po_name": po["name"],
                "po_db_id": po_id,
                "vendor": vendor,
                "po_date": po.get("date_order", "")[:10],
                "po_state": po.get("state"),
                "po_total": po.get("amount_total"),
                "so_date_from": date_from if not include_all_so else "all time",
                "filter_only_with_so": only_with_so,
                "total_products_in_po": total_products_in_po,
                "products_shown": products_shown,
                "lines": result_lines,
                "summary": (
                    f"{po_name}: {products_shown} of {total_products_in_po} products "
                    f"had matching SOs, {len(matched_so_ids)} related SOs found since {date_from}"
                    if only_with_so else
                    f"{po_name}: {len(result_lines)} products, {len(matched_so_ids)} related SOs found since {date_from}"
                )
            }, ensure_ascii=False, default=str)

        except Exception as e:
            print(f"GET_PO_SO error: {e}")
            return json.dumps({"error": str(e)})

    if name == "odoo_create_bulk_po":
        orders = inp.get("purchase_orders", [])
        created = []
        errors = []
        date_planned = (datetime.datetime.now() + datetime.timedelta(days=7)).strftime("%Y-%m-%d %H:%M:%S")
        now_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Get session: prefer logged-in user's cached Odoo session for proper attribution
        ctx_uid = ctx.get("uid")
        user_session = USER_ODOO_SESSIONS.get(ctx_uid) if ctx_uid else None
        if user_session:
            session_age = (datetime.datetime.now() - user_session["time"]).total_seconds()
            if session_age < 7200:  # 2 hours max
                cookies = user_session["cookies"]
                print(f"BULK PO: using user session (uid={ctx_uid}, age={session_age:.0f}s)")
            else:
                cookies = await odoo_get_session()
                print(f"BULK PO: user session expired, using admin session")
        else:
            cookies = await odoo_get_session()
            print(f"BULK PO: no user session cached, using admin session")

        # ─────────────────────────────────────────────────────────────
        # Phase 0: ID rescue — rescue AI-hallucinated product_id / partner_id
        # by falling back to SKU / product_name / partner_name lookup.
        # ─────────────────────────────────────────────────────────────

        # (A) SKU-FIRST product resolution — completely ignore AI's product_id when SKU is available
        # AI hallucinates product_ids that happen to be valid but belong to WRONG products.
        # The only trustworthy identifier is the SKU string.
        sku_cache = {}  # sku -> product_id (batch lookup)

        # Collect all SKUs first for batch query
        all_skus = list({(l.get("sku") or "").strip().upper()
                         for po in orders for l in po.get("lines", [])
                         if (l.get("sku") or "").strip()})
        if all_skus:
            for sku in all_skus:
                r = json.loads(await odoo_query("product.product",
                    [["default_code","=ilike",sku],["active","=",True]],
                    ["id","default_code"], limit=3, cookies=cookies))
                if isinstance(r, list) and r:
                    # Prefer exact match
                    exact = [p for p in r if (p.get("default_code") or "").upper() == sku]
                    p = exact[0] if exact else r[0]
                    sku_cache[sku] = p["id"]

        print(f"ID RESCUE: {len(sku_cache)}/{len(all_skus)} SKUs resolved via batch lookup")

        # (B) Apply: for each line, if SKU available → use SKU-resolved id; else fallback to name
        for po in orders:
            for line in po.get("lines", []):
                sku = (line.get("sku") or "").strip().upper()
                pid = line.get("product_id")
                pname = (line.get("product_name") or "").strip()

                if sku and sku in sku_cache:
                    correct_id = sku_cache[sku]
                    if pid != correct_id:
                        print(f"ID RESCUE: SKU={sku} → product_id {pid} → {correct_id} (SKU override)")
                    line["product_id"] = correct_id
                    continue

                # No SKU or SKU not found — try by name
                if not pid:
                    resolved = None
                    if pname:
                        r = json.loads(await odoo_query("product.product",
                            [["name","ilike",pname.split(",")[0].strip()[:60]],["active","=",True]],
                            ["id","name"], limit=3, cookies=cookies))
                        if isinstance(r, list) and len(r) == 1:
                            resolved = r[0]["id"]
                    if resolved:
                        print(f"ID RESCUE: no SKU, product_id None → {resolved} (by name)")
                        line["product_id"] = resolved
                    else:
                        print(f"ID RESCUE FAIL: no SKU, pid={pid}, name='{pname[:60]}'")

        # (C) Validate partner_ids; rescue via partner_name
        all_partner_ids = list({po.get("partner_id") for po in orders if po.get("partner_id")})
        valid_suppliers = {}  # id -> actual name
        if all_partner_ids:
            r = json.loads(await odoo_query("res.partner",
                [["id","in",all_partner_ids]],
                ["id","name","supplier_rank"], limit=100, cookies=cookies))
            if isinstance(r, list):
                valid_suppliers = {p["id"]: p.get("name", "") for p in r
                                   if (p.get("supplier_rank", 0) or 0) > 0}
        print(f"ID RESCUE: {len(valid_suppliers)}/{len(all_partner_ids)} partner_ids are valid suppliers")

        for po in orders:
            pid = po.get("partner_id")
            pname = (po.get("partner_name") or "").strip()
            # Check: is pid a valid supplier AND does its actual name match what AI claimed?
            if pid in valid_suppliers:
                actual_name = valid_suppliers[pid]
                # If names roughly match, keep it
                if pname and pname.lower() not in actual_name.lower() and actual_name.lower() not in pname.lower():
                    print(f"ID RESCUE: partner_id {pid} is supplier '{actual_name}' but AI said '{pname}' — name mismatch, resolving by name")
                else:
                    continue  # Valid supplier + name matches → keep
            if not pname:
                continue
            resolved = None
            # Exact name match first
            r = json.loads(await odoo_query("res.partner",
                [["name","=",pname],["supplier_rank",">",0]],
                ["id","name"], limit=1, cookies=cookies))
            if isinstance(r, list) and r:
                resolved = r[0]["id"]
            # ilike as last resort, only if unique
            if not resolved:
                r = json.loads(await odoo_query("res.partner",
                    [["name","ilike",pname],["supplier_rank",">",0]],
                    ["id","name"], limit=3, cookies=cookies))
                if isinstance(r, list) and len(r) == 1:
                    resolved = r[0]["id"]
            if resolved:
                print(f"ID RESCUE: partner_id {pid} → {resolved} (by name '{pname}')")
                po["partner_id"] = resolved
            else:
                print(f"ID RESCUE FAIL: partner_id={pid} name='{pname}'")

        # Pre-fetch all product info in one batch per PO
        all_product_ids = list({line["product_id"] for po in orders for line in po.get("lines", [])})
        prod_info = {}
        if all_product_ids:
            prod_r = await odoo_query("product.product",
                [["id","in",all_product_ids]],
                ["id","name","uom_po_id","uom_id"], limit=500, cookies=cookies)
            for p in json.loads(prod_r):
                uom = p.get("uom_po_id") or p.get("uom_id")
                prod_info[p["id"]] = {
                    "name": p["name"],
                    "uom_id": uom[0] if uom else None
                }

        for po in orders:
            partner_id = po["partner_id"]
            po_lines = po.get("lines", [])
            line_pids = [l["product_id"] for l in po_lines if l.get("product_id")]

            # Robust vendor resolution (handles AI passing wrong partner_id)
            partner_id, final_vendor_name, vendor_fix_note = await resolve_po_vendor(
                partner_id, line_pids, cookies=cookies
            )
            if not final_vendor_name:
                final_vendor_name = po.get("partner_name", "")

            # Verify partner exists
            partner_check = await odoo_query(
                "res.partner", [["id","=",partner_id]],
                ["id","name"], limit=1, cookies=cookies)
            partner_data = json.loads(partner_check)
            if not partner_data:
                errors.append({"vendor": final_vendor_name or po.get("partner_name"),
                               "error": f"Partner ID {partner_id} not found."})
                continue
            # Final authoritative name (covers edge cases where resolve_po_vendor didn't set it)
            if not final_vendor_name:
                final_vendor_name = partner_data[0]["name"]

            # Create PO header with verified vendor
            po_vals = {
                "partner_id": partner_id,
                "company_id": 1,
                "date_order": now_str,
            }
            # Set buyer (user_id) to the logged-in user if provided
            ctx_uid = ctx.get("uid")
            if ctx_uid and ctx_uid > 0:
                po_vals["user_id"] = ctx_uid
            po_result = await odoo_create("purchase.order", po_vals, cookies=cookies)
            if po_result.get("error"):
                errors.append({"vendor": po.get("partner_name"), "error": po_result["error"]})
                continue
            po_id = po_result["id"]

            # Fetch the actual PO name from Odoo
            po_name_r = await odoo_query("purchase.order", [["id","=",po_id]], ["name"], limit=1, cookies=cookies)
            po_name_data = json.loads(po_name_r)
            po_name = po_name_data[0]["name"] if po_name_data else f"ID:{po_id}"

            # Use Odoo's load() method — same as Excel import, triggers onchange automatically
            # Only need: order_id (as partner_id equivalent), product_id, product_qty
            line_errors = []
            lines_created = 0

            # Validate and collect all product IDs first
            validated_lines = []
            for line in po.get("lines", []):
                pid = line["product_id"]
                # Auto-fix template IDs
                if pid not in prod_info:
                    chk = await odoo_query("product.product",
                        [["id","=",pid]], ["id","name"], limit=1, cookies=cookies)
                    chk_data = json.loads(chk)
                    if not chk_data:
                        tmpl_chk = await odoo_query("product.product",
                            [["product_tmpl_id","=",pid],["active","=",True]],
                            ["id","name","uom_po_id","uom_id"], limit=1, cookies=cookies)
                        tmpl_data = json.loads(tmpl_chk)
                        if tmpl_data:
                            pid = tmpl_data[0]["id"]
                            prod_info[pid] = {"name": tmpl_data[0]["name"], "uom_id": None}
                        else:
                            line_errors.append(f"{line.get('product_name','?')}: product ID {line['product_id']} not found")
                            continue
                validated_lines.append({"product_id": pid, "quantity": line["quantity"],
                                        "price_unit": line.get("price_unit", 0)})

            # Batch create ALL lines in ONE write() call using One2many (0,0,vals) commands
            if validated_lines:
                order_lines_cmds = []
                for l in validated_lines:
                    pid = l["product_id"]
                    pinfo = prod_info.get(pid, {})
                    line_data = {
                        "product_id": pid,
                        "product_qty": l["quantity"],
                        "name": pinfo.get("name", "Product"),
                        "date_planned": date_planned,
                    }
                    if l.get("price_unit"):
                        line_data["price_unit"] = l["price_unit"]
                    uom_id = pinfo.get("uom_id")
                    if uom_id:
                        line_data["product_uom"] = uom_id
                    order_lines_cmds.append([0, 0, line_data])

                # Single API call to add all lines
                write_result = await odoo_write_record(
                    "purchase.order", po_id,
                    {"order_line": order_lines_cmds},
                    cookies=cookies
                )
                if write_result.get("error"):
                    # Fallback: one by one
                    for l in validated_lines:
                        pinfo = prod_info.get(l["product_id"], {})
                        lr = await odoo_create("purchase.order.line", {
                            "order_id": po_id,
                            "product_id": l["product_id"],
                            "product_qty": l["quantity"],
                            "name": pinfo.get("name", ""),
                            "date_planned": date_planned,
                        }, cookies=cookies)
                        if lr.get("error"):
                            line_errors.append(f"{pinfo.get('name','?')}: {lr['error']}")
                        else:
                            lines_created += 1
                else:
                    lines_created = len(validated_lines)

            created.append({
                "po_id": po_id,
                "po_name": po_name,
                "vendor": final_vendor_name,
                "vendor_id": partner_id,
                "vendor_fix_note": vendor_fix_note,
                "lines_requested": len(po.get("lines", [])),
                "lines_created": lines_created,
                "line_errors": line_errors,
                "odoo_link": f"{ODOO_URL}/web#model=purchase.order&id={po_id}&view_type=form"
            })
        return json.dumps({
            "created": created,
            "errors": errors,
            "summary": f"Created {len(created)} PO(s) with {sum(p['lines_created'] for p in created)} lines total. {len(errors)} PO(s) failed."
        }, ensure_ascii=False)

    if name == "odoo_restock_analysis":
        """
        Restock analysis: query stock.move (outgoing, done) for a time window,
        aggregate by product, compare with current inventory (qty_available),
        and flag products that need restocking based on brand-specific lead times.

        Data flow (optimized — no template queries needed):
          1. Query stock.move (outgoing, done, last N days) — gets product_id + qty
          2. Aggregate total outgoing per product
          3. Query product.product for ONLY moved products — gets qty_available + product_tmpl_id
          4. Query product.template for ONLY those templates — gets x_brand
          5. Calculate daily_avg, days_remaining, urgency per product
          6. Return ONLY actionable products (🔴🟠🟡), capped at 100

        Brand field: x_brand on product.template (many2one → x_brand table).
        Also: x_studio_brand on stock.move.line (related field, same table).
        """
        import datetime as dt
        days_back = inp.get("days_back", 30)
        brand_filter = inp.get("brand_filter", "").strip()
        urgency_filter = inp.get("urgency_filter", "").strip()

        BRAND_LEAD_TIMES = {
            "Polarman":     {"lead": 60, "reorder": 60, "urgent": 30},
            "Flamaster":    {"lead": 90, "reorder": 90, "urgent": 45},
            "ChefAsst":     {"lead": 90, "reorder": 90, "urgent": 45},
            "Thunder Group":{"lead": 3,  "reorder": 7,  "urgent": 3},
            "Winco":        {"lead": 3,  "reorder": 7,  "urgent": 3},
        }
        DEFAULT_LEAD = {"lead": 14, "reorder": 14, "urgent": 7}

        try:
            date_from = (dt.datetime.now() - dt.timedelta(days=days_back)).strftime("%Y-%m-%d")
            print(f"RESTOCK: analyzing {days_back} days since {date_from}, brand='{brand_filter}'")

            # ── Step 1: Query ALL outgoing stock.move in one pass ──
            # stock.move is the source of truth for validated outbound movement.
            # Typically 700-1500 rows for 30 days — manageable in 1-2 batches.
            all_moves = []
            offset = 0
            batch_size = 2000
            while True:
                moves_r = json.loads(await odoo_query(
                    "stock.move",
                    [
                        ["state", "=", "done"],
                        ["picking_type_id.code", "=", "outgoing"],
                        ["date", ">=", date_from],
                        ["company_id", "=", 1],
                    ],
                    ["product_id", "product_qty"],
                    limit=batch_size, offset=offset, order="id asc"
                ))
                if not isinstance(moves_r, list) or not moves_r:
                    break
                all_moves.extend(moves_r)
                if len(moves_r) < batch_size:
                    break
                offset += batch_size
            print(f"RESTOCK: {len(all_moves)} outgoing moves fetched")

            if not all_moves:
                return json.dumps({
                    "summary": {"days_analyzed": days_back, "date_from": date_from,
                                "total_products_analyzed": 0, "actionable_count": 0,
                                "counts": {"out_of_stock":0,"urgent":0,"reorder":0,"ok":0,"no_movement":0,"total":0},
                                "brand_filter": brand_filter or "(all)"},
                    "brand_summary": {},
                    "products": [],
                })

            # ── Step 2: Aggregate total outgoing per product_id ──
            product_stats = {}  # pid -> {total_qty, move_count, name}
            for m in all_moves:
                if not m.get("product_id"):
                    continue
                pid = m["product_id"][0]
                pname = m["product_id"][1]
                if pid not in product_stats:
                    product_stats[pid] = {"total_qty": 0, "move_count": 0, "name": pname}
                product_stats[pid]["total_qty"] += m.get("product_qty", 0)
                product_stats[pid]["move_count"] += 1

            moved_pids = list(product_stats.keys())
            print(f"RESTOCK: {len(moved_pids)} unique products with outgoing moves")

            # ── Step 3: Fetch inventory + template ID for moved products ONLY ──
            inv_map = {}  # pid -> {id, default_code, qty_available, product_tmpl_id}
            for i in range(0, len(moved_pids), 200):
                batch = moved_pids[i:i+200]
                inv_r = json.loads(await odoo_query(
                    "product.product",
                    [["id", "in", batch]],
                    ["id", "default_code", "qty_available", "product_tmpl_id"],
                    limit=200
                ))
                if isinstance(inv_r, list):
                    for p in inv_r:
                        inv_map[p["id"]] = p

            # ── Step 4: Fetch brand (x_brand) from product.template ──
            tmpl_ids = list({inv_map[pid]["product_tmpl_id"][0]
                            for pid in moved_pids
                            if pid in inv_map and inv_map[pid].get("product_tmpl_id")})
            brand_map = {}  # tmpl_id -> brand_name
            if tmpl_ids:
                for i in range(0, len(tmpl_ids), 200):
                    batch = tmpl_ids[i:i+200]
                    tmpl_r = json.loads(await odoo_query(
                        "product.template",
                        [["id", "in", batch]],
                        ["id", "x_brand"], limit=200
                    ))
                    if isinstance(tmpl_r, list):
                        for t in tmpl_r:
                            if t.get("x_brand"):
                                bname = t["x_brand"][1] if isinstance(t["x_brand"], list) else str(t["x_brand"])
                                brand_map[t["id"]] = bname
            print(f"RESTOCK: resolved brands for {len(brand_map)} templates")

            # ── Step 5: Calculate restock status per product ──
            results = []
            for pid, stats in product_stats.items():
                inv = inv_map.get(pid, {})
                sku = inv.get("default_code", "") or ""
                qty_available = inv.get("qty_available", 0)
                tmpl_id = inv["product_tmpl_id"][0] if inv.get("product_tmpl_id") else None
                brand = brand_map.get(tmpl_id, "Other") if tmpl_id else "Other"

                # Brand filter: skip products not matching the requested brand
                if brand_filter:
                    if brand_filter.lower() not in brand.lower():
                        continue

                total_out = stats["total_qty"]
                move_count = stats["move_count"]
                daily_avg = total_out / days_back if days_back > 0 else 0
                days_remaining = (qty_available / daily_avg) if daily_avg > 0 else float("inf")

                # Get lead time config for this brand
                lead_config = DEFAULT_LEAD
                for bname, cfg in BRAND_LEAD_TIMES.items():
                    if bname.lower() in brand.lower():
                        lead_config = cfg
                        break

                # Determine urgency
                if qty_available <= 0:
                    urgency = "out_of_stock"
                    urgency_label = "🔴 已缺货"
                elif days_remaining <= lead_config["urgent"]:
                    urgency = "urgent"
                    urgency_label = "🟠 紧急补货"
                elif days_remaining <= lead_config["reorder"]:
                    urgency = "reorder"
                    urgency_label = "🟡 建议补货"
                else:
                    urgency = "ok"
                    urgency_label = "🟢 库存充足"

                if urgency_filter and urgency != urgency_filter:
                    continue

                results.append({
                    "product_id": pid,
                    "sku": sku,
                    "name": stats["name"],
                    "brand": brand,
                    "qty_available": round(qty_available, 1),
                    "total_outgoing": round(total_out, 1),
                    "move_count": move_count,
                    "daily_avg": round(daily_avg, 2),
                    "days_remaining": round(days_remaining, 1) if days_remaining != float("inf") else None,
                    "urgency": urgency,
                    "urgency_label": urgency_label,
                    "lead_time_days": lead_config["lead"],
                    "reorder_threshold": lead_config["reorder"],
                    "urgent_threshold": lead_config["urgent"],
                })

            # ── Step 6: Sort, summarize, and return only actionable items ──
            urgency_order = {"out_of_stock": 0, "urgent": 1, "reorder": 2, "ok": 3}
            results.sort(key=lambda r: (
                urgency_order.get(r["urgency"], 9),
                -r["move_count"],  # more shipments = higher priority (genuine recurring demand)
                r["days_remaining"] if r["days_remaining"] is not None else 99999
            ))

            # Brand summary (computed from ALL results)
            brand_summary = {}
            for r in results:
                b = r["brand"]
                if b not in brand_summary:
                    brand_summary[b] = {"out_of_stock": 0, "urgent": 0, "reorder": 0, "ok": 0, "total": 0}
                brand_summary[b][r["urgency"]] += 1
                brand_summary[b]["total"] += 1

            counts = {
                "out_of_stock": sum(1 for r in results if r["urgency"] == "out_of_stock"),
                "urgent": sum(1 for r in results if r["urgency"] == "urgent"),
                "reorder": sum(1 for r in results if r["urgency"] == "reorder"),
                "ok": sum(1 for r in results if r["urgency"] == "ok"),
                "total": len(results),
            }

            print(f"RESTOCK: done. OOS={counts['out_of_stock']}, urgent={counts['urgent']}, "
                  f"reorder={counts['reorder']}, ok={counts['ok']}")

            # Only return actionable products (🔴🟠🟡), capped at 100
            actionable = [r for r in results if r["urgency"] in ("out_of_stock", "urgent", "reorder")]
            returned_products = actionable[:100]
            truncated = len(actionable) - len(returned_products)

            return json.dumps({
                "summary": {
                    "days_analyzed": days_back,
                    "date_from": date_from,
                    "total_products_analyzed": len(results),
                    "actionable_count": len(actionable),
                    "returned_count": len(returned_products),
                    "truncated": truncated,
                    "skipped_ok": counts["ok"],
                    "counts": counts,
                    "brand_filter": brand_filter or "(all brands)",
                    "urgency_filter": urgency_filter or "(all levels)",
                },
                "brand_summary": brand_summary,
                "products": returned_products,
            }, ensure_ascii=False)

        except Exception as e:
            print(f"RESTOCK ERROR: {e}")
            import traceback; traceback.print_exc()
            return json.dumps({"error": f"Restock analysis failed: {str(e)}"})

    if name == "search_documents":
        conn = await get_db_conn()
        if not conn:
            return "Database not available."
        try:
            query = inp["query"]
            category = inp.get("category", "")
            # Build fuzzy search terms from query words
            words = [w for w in query.lower().split() if len(w) > 2]
            if not words:
                words = [query.lower()]

            if category:
                rows = await conn.fetch("""
                    SELECT id, original_name, category, description, public_url, chunk_count, created_at
                    FROM documents
                    WHERE category = $2
                    AND (LOWER(original_name) LIKE $1 OR LOWER(description) LIKE $1
                         OR LOWER(REPLACE(original_name, '_', ' ')) LIKE $1)
                    ORDER BY created_at DESC LIMIT 10
                """, f"%{query.lower()}%", category)
            else:
                # Try full query first, then individual words
                rows = await conn.fetch("""
                    SELECT id, original_name, category, description, public_url, chunk_count, created_at
                    FROM documents
                    WHERE LOWER(original_name) LIKE $1
                       OR LOWER(description) LIKE $1
                       OR LOWER(REPLACE(original_name, '_', ' ')) LIKE $1
                    ORDER BY created_at DESC LIMIT 10
                """, f"%{query.lower()}%")
                # If no results, try each word separately
                if not rows and words:
                    for word in words:
                        rows = await conn.fetch("""
                            SELECT id, original_name, category, description, public_url, chunk_count, created_at
                            FROM documents WHERE LOWER(original_name) LIKE $1 OR LOWER(description) LIKE $1
                            ORDER BY created_at DESC LIMIT 10
                        """, f"%{word}%")
                        if rows:
                            break
            if not rows:
                return f"No documents found matching '{query}'. Ask the admin to upload relevant documents."
            results = []
            for r in rows:
                results.append(
                    f"📄 **{r['original_name']}**\n"
                    f"   Category: {r['category']}\n"
                    f"   [📥 Download File](/docs/signed-url/{r['id']})"
                )
            return "\n\n".join(results)
        except Exception as e:
            return f"Search error: {e}"
        finally:
            await conn.close()
    if name == "odoo_create_invoice_from_so":
        """Create invoice from a Sales Order.
        Steps: find SO → call action_create_invoice → set payment method → return invoice info.
        """
        so_name = (inp.get("so_name") or "").strip()
        payment_method = (inp.get("payment_method") or "").strip()
        if not so_name:
            return json.dumps({"error": "so_name is required"})

        try:
            cookies = await odoo_get_session()

            # Step 1: Find the SO
            so_r = json.loads(await odoo_query("sale.order",
                [["name", "=", so_name], ["company_id", "=", 1]],
                ["id", "name", "partner_id", "amount_total", "state", "invoice_status", "invoice_ids"],
                limit=1, cookies=cookies))
            if not isinstance(so_r, list) or not so_r:
                return json.dumps({"error": f"SO '{so_name}' not found"})
            so = so_r[0]

            # Check SO state
            if so.get("state") not in ("sale", "done"):
                return json.dumps({"error": f"SO '{so_name}' is in state '{so.get('state')}', must be confirmed (sale/done) to invoice"})

            # Check if already invoiced
            existing_invoices = so.get("invoice_ids") or []
            if existing_invoices:
                # Check if any posted invoice exists
                inv_check = json.loads(await odoo_query("account.move",
                    [["id", "in", existing_invoices], ["state", "=", "posted"]],
                    ["id", "name", "payment_state", "amount_total"],
                    limit=5, cookies=cookies))
                if isinstance(inv_check, list) and inv_check:
                    inv = inv_check[0]
                    return json.dumps({
                        "already_invoiced": True,
                        "invoice_id": inv["id"],
                        "invoice_name": inv.get("name"),
                        "payment_state": inv.get("payment_state"),
                        "amount_total": inv.get("amount_total"),
                        "message": f"SO {so_name} already has invoice {inv.get('name')} (payment_state: {inv.get('payment_state')})"
                    })

            # Step 2: Create invoice via action_create_invoice wizard
            # In Odoo 17, we call sale.advance.payment.inv wizard
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
                # Create the wizard
                wiz_r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                    "jsonrpc": "2.0", "method": "call", "id": 10,
                    "params": {
                        "model": "sale.advance.payment.inv",
                        "method": "create",
                        "args": [{"advance_payment_method": "delivered"}],
                        "kwargs": {"context": {"active_ids": [so["id"]], "active_model": "sale.order"}}
                    }
                }, cookies=cookies)
                wiz_data = wiz_r.json()
                if wiz_data.get("error"):
                    return json.dumps({"error": f"Invoice wizard create failed: {wiz_data['error'].get('data', {}).get('message', str(wiz_data['error']))}"})
                wiz_id = wiz_data.get("result")

                # Execute the wizard to create invoice
                exec_r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                    "jsonrpc": "2.0", "method": "call", "id": 11,
                    "params": {
                        "model": "sale.advance.payment.inv",
                        "method": "create_invoices",
                        "args": [[wiz_id]],
                        "kwargs": {"context": {"active_ids": [so["id"]], "active_model": "sale.order"}}
                    }
                }, cookies=cookies)
                exec_data = exec_r.json()
                if exec_data.get("error"):
                    return json.dumps({"error": f"Invoice creation failed: {exec_data['error'].get('data', {}).get('message', str(exec_data['error']))}"})

            # Step 3: Find the newly created invoice
            so_refresh = json.loads(await odoo_query("sale.order",
                [["id", "=", so["id"]]],
                ["invoice_ids"], limit=1, cookies=cookies))
            new_inv_ids = so_refresh[0].get("invoice_ids", []) if so_refresh else []
            # The new invoice is the one not in existing_invoices
            created_inv_ids = [i for i in new_inv_ids if i not in existing_invoices]
            if not created_inv_ids:
                created_inv_ids = new_inv_ids  # fallback

            if not created_inv_ids:
                return json.dumps({"error": "Invoice creation seemed to succeed but no new invoice found"})

            inv_id = created_inv_ids[0]

            # Step 4: Post the invoice (draft → posted)
            post_result = await odoo_call_method("account.move", inv_id, "action_post")
            if post_result.get("error"):
                return json.dumps({"error": f"Invoice post failed: {post_result['error']}"})

            # Step 5: Set payment method if provided
            if payment_method:
                await odoo_write_record("account.move", inv_id,
                    {"x_payment_method": payment_method}, cookies=cookies)

            # Get final invoice info
            inv_info = json.loads(await odoo_query("account.move",
                [["id", "=", inv_id]],
                ["id", "name", "amount_total", "payment_state", "x_payment_method"],
                limit=1, cookies=cookies))
            inv = inv_info[0] if isinstance(inv_info, list) and inv_info else {}

            partner_name = so["partner_id"][1] if so.get("partner_id") else ""

            await audit_odoo_write(
                who_uid=ctx.get("uid", 0), who_name=ctx.get("username", ""),
                tool_name="odoo_create_invoice_from_so",
                model="account.move", record_id=inv_id,
                operation="create_invoice",
                new_values={"so": so_name, "payment_method": payment_method},
                extra_info={"partner": partner_name, "amount": so.get("amount_total")},
            )

            return json.dumps({
                "success": True,
                "so_name": so_name,
                "partner": partner_name,
                "invoice_id": inv_id,
                "invoice_name": inv.get("name", ""),
                "amount_total": inv.get("amount_total", so.get("amount_total")),
                "payment_state": inv.get("payment_state", "not_paid"),
                "payment_method": payment_method,
                "odoo_link": f"{ODOO_URL}/odoo/account-move/{inv_id}",
                "message": f"Invoice {inv.get('name', '')} created for {so_name} ({partner_name}), amount ${inv.get('amount_total', 0):,.2f}"
            }, ensure_ascii=False)

        except Exception as e:
            print(f"CREATE_INVOICE error: {e}")
            import traceback; traceback.print_exc()
            return json.dumps({"error": f"Failed to create invoice: {str(e)}"})

    if name == "odoo_register_payment":
        """Register payment on an invoice.
        Uses Odoo's account.payment.register wizard (same as clicking 'Register Payment' in UI).
        """
        invoice_id = inp.get("invoice_id")
        journal_name = (inp.get("journal_name") or "").strip()
        amount = inp.get("amount")
        payment_date = inp.get("payment_date") or datetime.date.today().strftime("%Y-%m-%d")

        if not invoice_id:
            return json.dumps({"error": "invoice_id is required"})
        if not journal_name:
            return json.dumps({"error": "journal_name is required (Cash, Revenue and COGS, or Amazon PLAT BUS CHECKING)"})

        try:
            cookies = await odoo_get_session()

            # Check invoice state first
            inv_r = json.loads(await odoo_query("account.move",
                [["id", "=", invoice_id]],
                ["id", "name", "state", "payment_state", "amount_total", "amount_residual"],
                limit=1, cookies=cookies))
            if not isinstance(inv_r, list) or not inv_r:
                return json.dumps({"error": f"Invoice {invoice_id} not found"})
            inv = inv_r[0]

            if inv.get("state") != "posted":
                return json.dumps({"error": f"Invoice {inv.get('name')} is not posted (state={inv.get('state')}). Post it first."})
            if inv.get("payment_state") in ("paid", "in_payment"):
                return json.dumps({
                    "already_paid": True,
                    "invoice_name": inv.get("name"),
                    "payment_state": inv.get("payment_state"),
                    "message": f"Invoice {inv.get('name')} is already {inv.get('payment_state')}"
                })

            # Find journal by name
            journal_r = json.loads(await odoo_query("account.journal",
                [["name", "ilike", journal_name], ["company_id", "=", 1]],
                ["id", "name", "type"], limit=5, cookies=cookies))
            if not isinstance(journal_r, list) or not journal_r:
                return json.dumps({"error": f"Journal '{journal_name}' not found. Available: Cash, Revenue and COGS, Amazon PLAT BUS CHECKING"})
            # Prefer exact match
            exact = [j for j in journal_r if j["name"].lower() == journal_name.lower()]
            journal = exact[0] if exact else journal_r[0]
            journal_id = journal["id"]

            pay_amount = amount if amount else float(inv.get("amount_residual") or inv.get("amount_total") or 0)

            # Use account.payment.register wizard (same as UI button)
            async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
                # Create wizard
                wiz_vals = {
                    "journal_id": journal_id,
                    "amount": pay_amount,
                    "payment_date": payment_date,
                }
                wiz_r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                    "jsonrpc": "2.0", "method": "call", "id": 20,
                    "params": {
                        "model": "account.payment.register",
                        "method": "create",
                        "args": [wiz_vals],
                        "kwargs": {"context": {
                            "active_model": "account.move",
                            "active_ids": [invoice_id],
                        }}
                    }
                }, cookies=cookies)
                wiz_data = wiz_r.json()
                if wiz_data.get("error"):
                    return json.dumps({"error": f"Payment wizard failed: {wiz_data['error'].get('data', {}).get('message', str(wiz_data['error']))}"})
                wiz_id = wiz_data.get("result")

                # Execute wizard
                exec_r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                    "jsonrpc": "2.0", "method": "call", "id": 21,
                    "params": {
                        "model": "account.payment.register",
                        "method": "action_create_payments",
                        "args": [[wiz_id]],
                        "kwargs": {"context": {
                            "active_model": "account.move",
                            "active_ids": [invoice_id],
                        }}
                    }
                }, cookies=cookies)
                exec_data = exec_r.json()
                if exec_data.get("error"):
                    return json.dumps({"error": f"Payment registration failed: {exec_data['error'].get('data', {}).get('message', str(exec_data['error']))}"})

            # Verify payment state
            inv_after = json.loads(await odoo_query("account.move",
                [["id", "=", invoice_id]],
                ["id", "name", "payment_state", "amount_residual"],
                limit=1, cookies=cookies))
            inv_final = inv_after[0] if isinstance(inv_after, list) and inv_after else {}

            await audit_odoo_write(
                who_uid=ctx.get("uid", 0), who_name=ctx.get("username", ""),
                tool_name="odoo_register_payment",
                model="account.move", record_id=invoice_id,
                operation="register_payment",
                new_values={"journal": journal["name"], "amount": pay_amount, "date": payment_date},
                extra_info={"invoice_name": inv.get("name")},
            )

            return json.dumps({
                "success": True,
                "invoice_id": invoice_id,
                "invoice_name": inv.get("name"),
                "amount_paid": pay_amount,
                "journal": journal["name"],
                "payment_date": payment_date,
                "payment_state_after": inv_final.get("payment_state", "unknown"),
                "amount_residual_after": inv_final.get("amount_residual", 0),
                "message": f"Payment ${pay_amount:,.2f} registered on {inv.get('name')} via {journal['name']}"
            }, ensure_ascii=False)

        except Exception as e:
            print(f"REGISTER_PAYMENT error: {e}")
            import traceback; traceback.print_exc()
            return json.dumps({"error": f"Failed to register payment: {str(e)}"})

    if name == "odoo_export_invoice_pdf":
        """Export invoice as PDF using Odoo's report engine."""
        invoice_id = inp.get("invoice_id")
        if not invoice_id:
            return json.dumps({"error": "invoice_id is required"})

        try:
            cookies = await odoo_get_session()

            # Verify invoice exists
            inv_r = json.loads(await odoo_query("account.move",
                [["id", "=", invoice_id]],
                ["id", "name", "state"],
                limit=1, cookies=cookies))
            if not isinstance(inv_r, list) or not inv_r:
                return json.dumps({"error": f"Invoice {invoice_id} not found"})
            inv = inv_r[0]
            inv_name = inv.get("name", f"INV-{invoice_id}")

            # Call Odoo report endpoint to generate PDF
            # 使用 Chumart 自定义发票模板 oscg_sdcmt_report.report_cmt_invoice2
            async with httpx.AsyncClient(timeout=60, follow_redirects=True) as c:
                report_url = f"{ODOO_URL}/report/pdf/oscg_sdcmt_report.report_cmt_invoice2/{invoice_id}"
                r = await c.get(report_url, cookies=cookies)

                # 如果自定义模板失败,fallback 到默认模板
                if r.status_code != 200:
                    print(f"[EXPORT_PDF] Custom template failed (HTTP {r.status_code}), falling back to default")
                    fallback_url = f"{ODOO_URL}/report/pdf/account.report_invoice/{invoice_id}"
                    r = await c.get(fallback_url, cookies=cookies)
                    if r.status_code != 200:
                        return json.dumps({"error": f"PDF generation failed: HTTP {r.status_code}"})

                pdf_bytes = r.content
                if len(pdf_bytes) < 100:
                    return json.dumps({"error": "PDF generation returned empty or invalid file"})

            # Upload to R2 for download
            safe_name = inv_name.replace("/", "_")
            r2_key = f"invoices/{safe_name}.pdf"
            ok = await r2_upload(pdf_bytes, r2_key, "application/pdf")
            if not ok:
                return json.dumps({"error": "Failed to upload PDF to storage"})

            download_url = f"{R2_PUBLIC_URL}/{r2_key}"

            print(f"EXPORT_PDF: {inv_name} → {len(pdf_bytes)} bytes → {r2_key}")

            return json.dumps({
                "success": True,
                "invoice_id": invoice_id,
                "invoice_name": inv_name,
                "pdf_size_kb": len(pdf_bytes) // 1024,
                "download_url": download_url,
                "message": f"Invoice {inv_name} exported as PDF ({len(pdf_bytes)//1024}KB)"
            })

        except Exception as e:
            print(f"EXPORT_PDF error: {e}")
            return json.dumps({"error": f"Failed to export PDF: {str(e)}"})

    if name == "check_so_payment_status":
        """检查 SO 状态 + 已收款情况，判断是否可以 release。"""
        so_name = (inp.get("so_name") or "").strip()
        if not so_name:
            return json.dumps({"error": "so_name is required"})
        try:
            cookies = await odoo_get_session()
            # 1) 查 SO
            so_r = json.loads(await odoo_query("sale.order",
                [["name", "=", so_name], ["company_id", "=", 1]],
                ["id", "name", "state", "partner_id", "amount_total", "invoice_status", "invoice_ids"],
                limit=1, cookies=cookies))
            if not isinstance(so_r, list) or not so_r:
                return json.dumps({"error": f"SO '{so_name}' not found"})
            so = so_r[0]
            so_state = so.get("state", "")
            so_amount = float(so.get("amount_total") or 0)
            partner_name = (so.get("partner_id") or [0, ""])[1]

            # 2) 检查已有 invoice
            existing_invoice = None
            if so.get("invoice_ids"):
                inv_r = json.loads(await odoo_query("account.move",
                    [["id", "in", so["invoice_ids"]],
                     ["state", "in", ["posted", "draft"]]],
                    ["id", "name", "state", "payment_state", "amount_total"],
                    limit=5, cookies=cookies))
                if isinstance(inv_r, list) and inv_r:
                    # 优先返回 posted 的
                    posted = [i for i in inv_r if i.get("state") == "posted"]
                    existing_invoice = posted[0] if posted else inv_r[0]

            # 3) 查 received_payments 队列
            payments = await _get_received_payments_for_so(so_name, status="received")
            total_received = sum(p["amount"] for p in payments)

            # 4) 判断状态
            can_release = True
            blockers = []
            if so_state not in ("sale", "done"):
                can_release = False
                blockers.append(f"SO is in state '{so_state}', must be 'sale' (confirmed) or 'done'. Please confirm the SO in Odoo first.")
            if existing_invoice and existing_invoice.get("state") == "posted" and existing_invoice.get("payment_state") in ("paid", "in_payment"):
                can_release = False
                blockers.append(f"SO already has paid invoice {existing_invoice.get('name')}. No action needed.")
            if total_received < so_amount - 0.01:
                can_release = False
                blockers.append(f"Insufficient payment: received ${total_received:,.2f} of ${so_amount:,.2f} (short ${so_amount - total_received:,.2f}).")
            if not payments:
                can_release = False
                blockers.append(f"No payment received yet for {so_name}.")

            return json.dumps({
                "so_name": so_name,
                "customer": partner_name,
                "so_state": so_state,
                "so_state_label": {"draft": "Quotation", "sent": "Quotation Sent", "sale": "Sales Order (confirmed)", "done": "Locked", "cancel": "Cancelled"}.get(so_state, so_state),
                "so_amount": so_amount,
                "total_received": total_received,
                "remaining": max(0, so_amount - total_received),
                "received_payments": payments,
                "existing_invoice": existing_invoice,
                "can_release": can_release,
                "blockers": blockers,
                "message": (
                    f"SO {so_name} ({partner_name}): state={so_state}, total=${so_amount:,.2f}, "
                    f"received=${total_received:,.2f} from {len(payments)} payment(s). "
                    + ("✅ Ready to release." if can_release else "❌ Cannot release: " + "; ".join(blockers))
                ),
            }, ensure_ascii=False)
        except Exception as e:
            print(f"CHECK_SO_PAYMENT error: {e}")
            import traceback; traceback.print_exc()
            return json.dumps({"error": f"Failed to check payment status: {str(e)}"})

    if name == "release_so":
        """Release SO: capture Stripe → create invoice → export PDF。
        
        流程:
        1. 检查 SO 状态 (必须 sale/done) + 检查队列里的付款
        2. 重复付款检测 (Stripe 同 SO 多个 PI) → 只 capture 第一个,通知用户去 cancel 其余
        3. Capture Stripe PaymentIntent (这一步 Odoo 会自动创建 account.payment)
        4. 创建 Invoice + 设 x_payment_method
        5. 导出 PDF
        
        注意:
        - 不做 reconcile (Odoo 自动处理或用户手动处理)
        - 不做 register_payment (Cash 走另一个工具)
        - 付款不够全额 → 拒绝
        - SO 是 quotation → 拒绝
        """
        so_name = (inp.get("so_name") or "").strip()
        if not so_name:
            return json.dumps({"error": "so_name is required"})

        try:
            # 先检查 SO 状态 + 队列
            check_result = await run_tool("check_so_payment_status", {"so_name": so_name}, context=ctx)
            check = json.loads(check_result) if isinstance(check_result, str) else check_result
            if check.get("error"):
                return json.dumps(check)
            if not check.get("can_release"):
                return json.dumps({
                    "error": "Cannot release SO",
                    "blockers": check.get("blockers", []),
                    "message": check.get("message", ""),
                })

            payments = check["received_payments"]
            so_amount = check["so_amount"]
            partner_name = check.get("customer", "")
            channels = list(dict.fromkeys(p["channel"] for p in payments))

            # ============================================
            # Step 1: 决定 payment_method 标签 + 预检 Stripe PI 状态 (还不 capture)
            # ============================================
            if len(channels) == 1:
                payment_method_label = channels[0]
            else:
                payment_method_label = f"Combo({'+'.join(channels)})"

            stripe_payments = [p for p in payments if p["channel"] == "Stripe"]
            captured_pis = []
            duplicate_pis_to_cancel = []
            first_pi_id = None

            if stripe_payments:
                import stripe
                stripe.api_key = STRIPE_SECRET_KEY

                # 队列里同一个 SO 的 Stripe 付款按时间排序,第一个 capture,其余拦截
                stripe_payments_sorted = sorted(stripe_payments, key=lambda p: p.get("created_at", ""))
                first_pi_id = stripe_payments_sorted[0]["external_ref"]

                # 预检第一笔 PI 状态 (确保还能 capture)
                try:
                    pi_obj = stripe.PaymentIntent.retrieve(first_pi_id)
                    pi_status = getattr(pi_obj, "status", "")
                    print(f"[RELEASE] Stripe PI {first_pi_id} status={pi_status}")
                    if pi_status not in ("requires_capture", "succeeded"):
                        return json.dumps({
                            "error": f"Stripe PI {first_pi_id} 状态异常: {pi_status},无法 release。",
                        })
                except stripe.error.StripeError as e:
                    return json.dumps({"error": f"Stripe API error: {str(e)}"})

                # 标记多余的 PI (这些不会 capture)
                if len(stripe_payments_sorted) > 1:
                    duplicate_pis_to_cancel = [p["external_ref"] for p in stripe_payments_sorted[1:]]
                    print(f"[RELEASE] ⚠️ 检测到 {len(duplicate_pis_to_cancel)} 笔重复 Stripe 付款,只 capture 第一笔: {duplicate_pis_to_cancel}")

            # ============================================
            # Step 2: 先创建 Invoice (Odoo 允许在 capture 前创建)
            # 这样 capture 后 Odoo 收到 webhook 看到已有 invoice 会自动 reconcile payment
            # ============================================
            ctx_local = {"uid": ctx.get("uid", 0), "username": ctx.get("username", "manual_release"), "role": "admin"}

            r1_str = await run_tool("odoo_create_invoice_from_so", {
                "so_name": so_name,
                "payment_method": payment_method_label,
            }, context=ctx_local)
            r1 = json.loads(r1_str) if isinstance(r1_str, str) else r1_str

            if r1.get("already_invoiced"):
                invoice_id = r1["invoice_id"]
                invoice_name = r1.get("invoice_name", "")
                print(f"[RELEASE] {so_name} already has invoice {invoice_name}")
            elif r1.get("success"):
                invoice_id = r1["invoice_id"]
                invoice_name = r1.get("invoice_name", "")
                print(f"[RELEASE] Created invoice {invoice_name} (id={invoice_id}) for {so_name}")
            else:
                return json.dumps({"error": f"Create invoice failed: {r1.get('error', 'unknown')}"})

            # ============================================
            # Step 3: Capture Stripe (现在 invoice 已经存在,Odoo 会自动 reconcile)
            # ============================================
            if first_pi_id:
                try:
                    # 重新查一遍状态 (创建 invoice 期间可能状态变了)
                    pi_obj = stripe.PaymentIntent.retrieve(first_pi_id)
                    pi_status = getattr(pi_obj, "status", "")

                    if pi_status == "requires_capture":
                        captured = stripe.PaymentIntent.capture(first_pi_id)
                        cap_status = getattr(captured, "status", "")
                        print(f"[RELEASE] Captured {first_pi_id}: {cap_status}")
                        if cap_status != "succeeded":
                            return json.dumps({
                                "error": f"Stripe capture failed for {first_pi_id}: status={cap_status}",
                                "invoice_created": invoice_name,
                                "warning": "Invoice 已创建但 capture 失败,需要人工处理。",
                            })
                        captured_pis.append(first_pi_id)
                    elif pi_status == "succeeded":
                        print(f"[RELEASE] {first_pi_id} already captured")
                        captured_pis.append(first_pi_id)
                except stripe.error.StripeError as e:
                    return json.dumps({
                        "error": f"Stripe capture error: {str(e)}",
                        "invoice_created": invoice_name,
                        "warning": "Invoice 已创建但 capture 失败,需要人工处理。",
                    })

            # ============================================
            # Step 4: Export PDF
            # ============================================
            r3_str = await run_tool("odoo_export_invoice_pdf", {
                "invoice_id": invoice_id,
            }, context=ctx_local)
            r3 = json.loads(r3_str) if isinstance(r3_str, str) else r3_str

            # ============================================
            # Step 5: 标记队列为 released + 通知重复
            # ============================================
            await _mark_received_payments_released(so_name, invoice_name)

            # 如果有重复 Stripe PI 没 capture，通知管理员
            if duplicate_pis_to_cancel:
                await _notify_stripe_duplicate(
                    so_name=so_name,
                    amount=so_amount,
                    keep_pi_id=captured_pis[0] if captured_pis else "",
                    duplicate_pi_ids=duplicate_pis_to_cancel,
                )

            # 构建消息
            duplicate_warning = ""
            if duplicate_pis_to_cancel:
                duplicate_warning = (
                    f"\n\n⚠️ 警告: 检测到 {len(duplicate_pis_to_cancel)} 笔重复 Stripe 付款已拦截:\n"
                    + "\n".join(f"  • {pid} (https://dashboard.stripe.com/payments/{pid})"
                                for pid in duplicate_pis_to_cancel)
                    + "\n请去 Stripe Dashboard 手动 cancel,否则 7 天后会自动释放回客户。\n"
                    + "邮件已发送到 di@chumartusa.com 和 ashley@chumartusa.com。"
                )

            return json.dumps({
                "success": True,
                "so_name": so_name,
                "customer": partner_name,
                "invoice_id": invoice_id,
                "invoice_name": invoice_name,
                "payment_method": payment_method_label,
                "amount": so_amount,
                "captured_stripe_pis": captured_pis,
                "duplicate_stripe_pis": duplicate_pis_to_cancel,
                "pdf_url": r3.get("download_url", ""),
                "message": (
                    f"✅ {so_name} ({partner_name}) 已 release\n"
                    f"发票: {invoice_name} (${so_amount:,.2f})\n"
                    f"付款方式: {payment_method_label}\n"
                    f"PDF: {r3.get('download_url', 'N/A')}"
                    + duplicate_warning
                ),
            }, ensure_ascii=False)

        except Exception as e:
            print(f"RELEASE_SO error: {e}")
            import traceback; traceback.print_exc()
            return json.dumps({"error": f"Failed to release SO: {str(e)}"})

    if name == "create_reminder":
        content = (inp.get("content") or "").strip()
        fire_at_str = (inp.get("fire_at") or "").strip()
        channels = inp.get("channels") or ["email"]
        if isinstance(channels, str):
            channels = [channels]
        channels = [c for c in channels if c in ("email", "sms", "call")]
        if not channels:
            channels = ["email"]
        if not content:
            return json.dumps({"error": "content is required"})
        if not fire_at_str:
            return json.dumps({"error": "fire_at is required (ISO datetime)"})
        try:
            fire_at_utc = _parse_iso_to_utc(fire_at_str)
        except Exception as e:
            return json.dumps({"error": f"could not parse fire_at: {e}"})
        now_utc = datetime.datetime.now(UTC_TZ)
        if fire_at_utc <= now_utc:
            return json.dumps({"error": f"fire_at ({_fmt_la(fire_at_utc)}) is in the past"})
        uid = ctx.get("uid", 0)
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        contact = await _get_user_contact(uid)
        missing = []
        if "email" in channels and not contact["email"]:
            missing.append("email")
        if ("sms" in channels or "call" in channels) and not contact["phone"]:
            missing.append("phone (set via set_my_contact or Odoo partner mobile)")
        if missing:
            return json.dumps({"error": f"missing contact info: {', '.join(missing)}"})
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            row = await conn.fetchrow("""
                INSERT INTO reminders (uid, user_name, content, fire_at, channels, target_email, target_phone)
                VALUES ($1, $2, $3, $4, $5::TEXT[], $6, $7) RETURNING id, fire_at
            """, uid, ctx.get("username", ""), content, fire_at_utc, channels, contact["email"], contact["phone"])
            return json.dumps({"ok": True, "id": row["id"], "content": content,
                "fire_at_la": _fmt_la(row["fire_at"]), "channels": channels,
                "message": f"Reminder set for {_fmt_la(row['fire_at'])}: {content}"}, ensure_ascii=False)
        finally:
            await conn.close()

    if name == "list_reminders":
        uid = ctx.get("uid", 0)
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        include_fired = bool(inp.get("include_fired", False))
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            if include_fired:
                rows = await conn.fetch("SELECT id, content, fire_at, channels, fired, fired_at, error FROM reminders WHERE uid=$1 ORDER BY fire_at DESC LIMIT 50", uid)
            else:
                rows = await conn.fetch("SELECT id, content, fire_at, channels, fired, fired_at, error FROM reminders WHERE uid=$1 AND fired=FALSE ORDER BY fire_at ASC", uid)
            out = [{"id": r["id"], "content": r["content"], "fire_at_la": _fmt_la(r["fire_at"]),
                    "channels": list(r["channels"] or []), "fired": r["fired"],
                    "fired_at_la": _fmt_la(r["fired_at"]) if r["fired_at"] else None,
                    "error": r["error"]} for r in rows]
            return json.dumps({"count": len(out), "reminders": out}, ensure_ascii=False)
        finally:
            await conn.close()

    if name == "cancel_reminder":
        uid = ctx.get("uid", 0)
        rid = inp.get("id")
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        if not rid:
            return json.dumps({"error": "id is required"})
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            result = await conn.execute("DELETE FROM reminders WHERE id=$1 AND uid=$2 AND fired=FALSE", rid, uid)
            if result.endswith(" 1"):
                return json.dumps({"ok": True, "deleted_id": rid})
            return json.dumps({"error": f"reminder {rid} not found or already fired"})
        finally:
            await conn.close()

    if name == "update_reminder":
        uid = ctx.get("uid", 0)
        rid = inp.get("id")
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        if not rid:
            return json.dumps({"error": "id is required (call list_reminders first to find it)"})

        new_fire_at = inp.get("fire_at")
        new_content = inp.get("content")
        if not new_fire_at and not new_content:
            return json.dumps({"error": "must provide at least one of: fire_at, content"})

        # Parse new time if provided
        new_fire_at_utc = None
        if new_fire_at:
            try:
                new_fire_at_utc = _parse_iso_to_utc(new_fire_at)
            except Exception as e:
                return json.dumps({"error": f"could not parse fire_at: {e}"})

        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            # Verify reminder exists and belongs to user
            existing = await conn.fetchrow(
                "SELECT id, content, fire_at, fired FROM reminders WHERE id=$1 AND uid=$2",
                rid, uid
            )
            if not existing:
                return json.dumps({"error": f"reminder {rid} not found or not yours"})
            if existing["fired"]:
                return json.dumps({"error": f"reminder {rid} already fired, cannot update"})

            # Build update query
            sets = []
            params = []
            idx = 1
            if new_fire_at_utc:
                sets.append(f"fire_at = ${idx}")
                params.append(new_fire_at_utc)
                idx += 1
            if new_content:
                sets.append(f"content = ${idx}")
                params.append(new_content)
                idx += 1
            params.extend([rid, uid])
            sql = f"UPDATE reminders SET {', '.join(sets)} WHERE id = ${idx} AND uid = ${idx+1}"
            await conn.execute(sql, *params)

            # Return the updated reminder for confirmation
            updated = await conn.fetchrow(
                "SELECT id, content, fire_at FROM reminders WHERE id=$1",
                rid
            )
            fire_la = updated["fire_at"].astimezone(LA_TZ) if updated["fire_at"] else None
            return json.dumps({
                "ok": True,
                "id": updated["id"],
                "content": updated["content"],
                "fire_at_la": fire_la.strftime("%Y-%m-%d %H:%M") if fire_la else None,
                "message": f"提醒已更新: {updated['content']} → {fire_la.strftime('%Y-%m-%d %H:%M') if fire_la else 'N/A'}",
            }, ensure_ascii=False)
        finally:
            await conn.close()

    if name == "create_event":
        uid = ctx.get("uid", 0)
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        title = (inp.get("title") or "").strip()
        if not title:
            return json.dumps({"error": "title is required"})
        start_str = inp.get("start_at")
        if not start_str:
            return json.dumps({"error": "start_at is required"})
        try:
            start_utc = _parse_iso_to_utc(start_str)
        except Exception as e:
            return json.dumps({"error": f"could not parse start_at: {e}"})
        end_utc = None
        if inp.get("end_at"):
            try:
                end_utc = _parse_iso_to_utc(inp["end_at"])
            except Exception:
                pass
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            row = await conn.fetchrow("""
                INSERT INTO events (uid, user_name, title, notes, location, start_at, end_at)
                VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id, start_at, end_at
            """, uid, ctx.get("username", ""), title, inp.get("notes", ""), inp.get("location", ""), start_utc, end_utc)
            return json.dumps({"ok": True, "id": row["id"], "title": title,
                "start_at_la": _fmt_la(row["start_at"]),
                "end_at_la": _fmt_la(row["end_at"]) if row["end_at"] else None,
                "message": f"Event: {title} @ {_fmt_la(row['start_at'])}"}, ensure_ascii=False)
        finally:
            await conn.close()

    if name == "list_events":
        uid = ctx.get("uid", 0)
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        try:
            start = _parse_iso_to_utc(inp["date_from"]) if inp.get("date_from") else datetime.datetime.now(UTC_TZ) - datetime.timedelta(hours=1)
            end = _parse_iso_to_utc(inp["date_to"]) if inp.get("date_to") else datetime.datetime.now(UTC_TZ) + datetime.timedelta(days=30)
        except Exception as e:
            return json.dumps({"error": f"date parse error: {e}"})
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            rows = await conn.fetch("SELECT id, title, notes, location, start_at, end_at FROM events WHERE uid=$1 AND start_at >= $2 AND start_at <= $3 ORDER BY start_at ASC", uid, start, end)
            out = [{"id": r["id"], "title": r["title"], "start_at_la": _fmt_la(r["start_at"]),
                    "end_at_la": _fmt_la(r["end_at"]) if r["end_at"] else None,
                    "notes": r["notes"], "location": r["location"]} for r in rows]
            return json.dumps({"count": len(out), "events": out}, ensure_ascii=False)
        finally:
            await conn.close()

    if name == "delete_event":
        uid = ctx.get("uid", 0)
        eid = inp.get("id")
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        if not eid:
            return json.dumps({"error": "id is required"})
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            result = await conn.execute("DELETE FROM events WHERE id=$1 AND uid=$2", eid, uid)
            if result.endswith(" 1"):
                return json.dumps({"ok": True, "deleted_id": eid})
            return json.dumps({"error": f"event {eid} not found"})
        finally:
            await conn.close()

    if name == "set_my_contact":
        uid = ctx.get("uid", 0)
        if not uid:
            return json.dumps({"error": "user not authenticated"})
        email = (inp.get("email") or "").strip() or None
        phone = (inp.get("phone") or "").strip() or None
        if not email and not phone:
            return json.dumps({"error": "provide at least one of: email, phone"})
        if phone and not phone.startswith("+"):
            return json.dumps({"error": "phone must be E.164 format starting with +, e.g. +13105551234"})
        conn = await get_db_conn()
        if not conn:
            return json.dumps({"error": "database unavailable"})
        try:
            await conn.execute("""
                INSERT INTO user_contacts (uid, email, phone, updated_at) VALUES ($1, $2, $3, NOW())
                ON CONFLICT (uid) DO UPDATE SET
                    email = COALESCE(EXCLUDED.email, user_contacts.email),
                    phone = COALESCE(EXCLUDED.phone, user_contacts.phone), updated_at = NOW()
            """, uid, email, phone)
            return json.dumps({"ok": True, "email": email, "phone": phone, "message": "Contact preferences saved."})
        finally:
            await conn.close()

    return "Unknown tool"

# ─────────────────────────────────────────────
# OpenAI chat helper
# ─────────────────────────────────────────────

def fix_schema_for_openai(schema: dict) -> dict:
    """Recursively fix schema to satisfy OpenAI requirements.
    - array types must have 'items'
    - remove unsupported keys like 'default' at top level properties
    """
    if not isinstance(schema, dict):
        return schema
    result = {}
    for k, v in schema.items():
        if isinstance(v, dict):
            v = fix_schema_for_openai(v)
        elif isinstance(v, list):
            v = [fix_schema_for_openai(i) if isinstance(i, dict) else i for i in v]
        result[k] = v
    # Ensure array type has items
    if result.get("type") == "array" and "items" not in result:
        result["items"] = {}
    return result

def convert_tools_to_openai(tools: list) -> list:
    """Convert Anthropic tool format to OpenAI function format."""
    oai_tools = []
    for t in tools:
        fixed_schema = fix_schema_for_openai(t["input_schema"])
        oai_tools.append({
            "type": "function",
            "function": {
                "name": t["name"],
                "description": t["description"],
                "parameters": fixed_schema
            }
        })
    return oai_tools

async def chat_openai(messages: list, system: str, model: str, tools: list, context: dict = None) -> str:
    """Call OpenAI Chat Completions API with full tool use support."""
    openai_key = os.getenv("OPENAI_API_KEY", "")
    if not openai_key:
        return "OpenAI API key not configured. Please add OPENAI_API_KEY to Railway environment variables."
    try:
        oai_tools = convert_tools_to_openai(tools)
        oai_messages = [{"role": "system", "content": system}] + messages

        async with httpx.AsyncClient(timeout=300) as c:
            current_messages = list(oai_messages)
            for _ in range(8):
                payload = {
                    "model": model,
                    "messages": current_messages,
                }
                # GPT-5.x uses max_completion_tokens; older models use max_tokens
                if model.startswith("gpt-5"):
                    payload["max_completion_tokens"] = 4096
                else:
                    payload["max_tokens"] = 4096
                if oai_tools:
                    payload["tools"] = oai_tools
                    payload["tool_choice"] = "auto"

                r = await c.post(
                    "https://api.openai.com/v1/chat/completions",
                    headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                    json=payload
                )
                data = r.json()
                if "error" in data:
                    return f"OpenAI error: {data['error'].get('message', str(data['error']))}"

                choice = data["choices"][0]
                msg = choice["message"]
                finish = choice.get("finish_reason")

                # If tool calls needed
                if finish == "tool_calls" and msg.get("tool_calls"):
                    current_messages.append(msg)
                    for tc in msg["tool_calls"]:
                        fn_name = tc["function"]["name"]
                        try:
                            fn_args = json.loads(tc["function"]["arguments"])
                        except Exception:
                            fn_args = {}
                        result = await run_tool(fn_name, fn_args, context=context)
                        current_messages.append({
                            "role": "tool",
                            "tool_call_id": tc["id"],
                            "content": result
                        })
                else:
                    return msg.get("content", "") or "No response."

        return "Sorry, max iterations reached."
    except Exception as e:
        return f"OpenAI request failed: {e}"

# ─────────────────────────────────────────────
# Admin endpoints
# ─────────────────────────────────────────────

class CrawlRequest(BaseModel):
    sites: list = []  # empty = crawl all TARGET_SITES
    admin_key: str = ""

@app.post("/admin/crawl")
async def admin_crawl(req: CrawlRequest, background_tasks: BackgroundTasks):
    admin_key = os.getenv("ADMIN_KEY", "chumart2024")
    if req.admin_key != admin_key:
        return {"error": "Invalid admin key"}

    sites_to_crawl = req.sites if req.sites else TARGET_SITES

    conn = await get_db_conn()
    log_ids = []
    if conn:
        for site in sites_to_crawl:
            site_url = site["url"] if isinstance(site, dict) else site
            row = await conn.fetchrow(
                "INSERT INTO crawl_log (site_url, status) VALUES ($1, 'running') RETURNING id",
                site_url
            )
            log_ids.append(row["id"])
        await conn.close()

    async def crawl_all():
        for i, site in enumerate(sites_to_crawl):
            if isinstance(site, str):
                site = {"url": site, "name": site, "category": "other"}
            log_id = log_ids[i] if i < len(log_ids) else 0
            await crawl_site(site, log_id)

    background_tasks.add_task(crawl_all)
    return {
        "status": "started",
        "sites": [s["url"] if isinstance(s, dict) else s for s in sites_to_crawl],
        "message": "Crawling started in background. Check /admin/kb-status for progress."
    }

@app.get("/admin/kb-status")
async def kb_status():
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        total = await conn.fetchval("SELECT COUNT(*) FROM knowledge_chunks")
        by_site = await conn.fetch("SELECT site_name, COUNT(*) as chunks FROM knowledge_chunks GROUP BY site_name ORDER BY chunks DESC")
        logs = await conn.fetch("SELECT site_url, status, pages, chunks, started_at, finished_at FROM crawl_log ORDER BY started_at DESC LIMIT 10")
        # Also show doc chunks breakdown
        doc_chunks = await conn.fetch("""
            SELECT site_name, category, COUNT(*) as chunks
            FROM knowledge_chunks WHERE site_url LIKE 'doc:%'
            GROUP BY site_name, category ORDER BY chunks DESC
        """)
        return {
            "total_chunks": total,
            "by_site": [dict(r) for r in by_site],
            "doc_chunks": [dict(r) for r in doc_chunks],
            "recent_crawls": [dict(r) for r in logs]
        }
    finally:
        await conn.close()

@app.get("/admin/audit")
async def audit_recent(limit: int = 50, model: str = "", who: str = "",
                        operation: str = "", since_hours: int = 0):
    """
    Query the Odoo write audit log.
    Shows exactly which records the AI wrote, when, by whom, old vs new values.

    Query params:
      limit         max rows (default 50, max 500)
      model         filter by Odoo model, e.g. 'product.supplierinfo'
      who           filter by who_name (ilike match)
      operation     'create' or 'update'
      since_hours   only show entries from the last N hours
    """
    limit = max(1, min(int(limit), 500))
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        clauses = []
        args = []
        if model:
            args.append(model)
            clauses.append(f"model = ${len(args)}")
        if operation:
            args.append(operation)
            clauses.append(f"operation = ${len(args)}")
        if who:
            args.append(f"%{who}%")
            clauses.append(f"who_name ILIKE ${len(args)}")
        if since_hours and since_hours > 0:
            args.append(since_hours)
            clauses.append(f"ts >= NOW() - (${len(args)}::int * INTERVAL '1 hour')")
        where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        args.append(limit)
        q = (f"SELECT id, ts, who_uid, who_name, tool_name, model, record_id, "
             f"operation, old_values, new_values, extra_info, status "
             f"FROM odoo_write_audit {where} "
             f"ORDER BY ts DESC LIMIT ${len(args)}")
        rows = await conn.fetch(q, *args)
        out = []
        for r in rows:
            d = dict(r)
            # Parse jsonb columns back into Python
            for k in ("old_values", "new_values", "extra_info"):
                if d.get(k) and isinstance(d[k], str):
                    try:
                        d[k] = json.loads(d[k])
                    except Exception:
                        pass
            d["ts"] = d["ts"].isoformat() if d.get("ts") else None
            out.append(d)
        return {"count": len(out), "entries": out}
    finally:
        await conn.close()

@app.get("/admin/reindex-docs")
async def reindex_docs(admin_key: str = "", background_tasks: BackgroundTasks = None):
    """Re-extract and re-index all uploaded documents from R2."""
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        rows = await conn.fetch("""
            SELECT id, filename, original_name, category, mime_type, r2_key
            FROM documents ORDER BY created_at DESC
        """)
    finally:
        await conn.close()

    if not rows:
        return {"status": "no documents found"}

    async def do_reindex():
        ok_count = 0
        fail_count = 0
        for row in rows:
            try:
                doc_id = row["id"]
                r2_key = row["r2_key"]
                category = row["category"]
                mime_type = row["mime_type"]
                original_name = row["original_name"]
                # Fetch file bytes from R2
                url = f"{R2_PUBLIC_URL}/{r2_key}"
                async with httpx.AsyncClient(timeout=60) as c:
                    r = await c.get(url)
                    if r.status_code != 200:
                        print(f"REINDEX FAIL fetch {original_name}: HTTP {r.status_code}")
                        fail_count += 1
                        continue
                    file_bytes = r.content
                text = await extract_text_from_file(file_bytes, original_name, mime_type)
                if text:
                    count = await process_document_to_kb(doc_id, original_name, text, category)
                    print(f"REINDEX OK: {original_name} → {count} chunks")
                    ok_count += 1
                else:
                    print(f"REINDEX SKIP {original_name}: no text extracted")
                    fail_count += 1
            except Exception as e:
                print(f"REINDEX ERROR {row.get('original_name')}: {e}")
                fail_count += 1
        print(f"REINDEX DONE: {ok_count} ok, {fail_count} failed")

    background_tasks.add_task(do_reindex)
    return {
        "status": "reindex started",
        "documents": len(rows),
        "message": f"Re-indexing {len(rows)} documents in background. Check Railway logs for progress."
    }

@app.delete("/admin/kb-clear")
async def kb_clear(site_url: str = "", admin_key: str = ""):
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        if site_url:
            deleted = await conn.execute("DELETE FROM knowledge_chunks WHERE site_url = $1", site_url)
        else:
            deleted = await conn.execute("DELETE FROM knowledge_chunks")
        return {"status": "cleared", "detail": deleted}
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# File extraction
# ─────────────────────────────────────────────

@app.post("/extract-file")
async def extract_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        filename = file.filename.lower()
        if filename.endswith(('.txt', '.md', '.csv')):
            return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}

        # Excel files — parse with openpyxl, return as tab-separated text
        if filename.endswith(('.xlsx', '.xls')):
            try:
                import io, openpyxl
                wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
                all_text = []
                for sheet_name in wb.sheetnames:
                    ws = wb[sheet_name]
                    rows = list(ws.iter_rows(values_only=True))
                    if not rows:
                        continue
                    all_text.append(f"=== Sheet: {sheet_name} ===")
                    for row in rows:
                        cells = [str(c) if c is not None else "" for c in row]
                        if any(c.strip() for c in cells):  # skip fully empty rows
                            all_text.append("\t".join(cells))
                wb.close()
                text = "\n".join(all_text)
                print(f"EXTRACT-FILE: {file.filename} (xlsx) → {len(text)} chars, {len(all_text)} rows")
                return {"text": text, "name": file.filename}
            except Exception as e:
                print(f"EXTRACT-FILE xlsx error: {e}")
                return {"text": f"[Excel parse error: {e}]", "name": file.filename}

        cleanup_caches()  # Clean expired entries on each upload
        now = datetime.datetime.now()
        if filename.endswith('.pdf'):
            b64 = base64.standard_b64encode(content).decode('utf-8')
            fid = str(uuid.uuid4())
            FILE_CACHE[fid] = {"b64": b64, "media_type": "application/pdf", "name": file.filename, "created_at": now}
            return {"text": "", "name": file.filename, "file_id": fid}
        if filename.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            ext = filename.split('.')[-1].replace('jpg', 'jpeg')
            media_type = f"image/{ext}"
            b64 = base64.standard_b64encode(content).decode('utf-8')
            fid = str(uuid.uuid4())
            FILE_CACHE[fid] = {"b64": b64, "media_type": media_type, "name": file.filename, "created_at": now}
            return {"text": "", "name": file.filename, "file_id": fid, "is_image": True, "preview_url": f"data:{media_type};base64,{b64}"}
        return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# Chat
# ─────────────────────────────────────────────

ALLOWED_MODELS = {
    # Anthropic Claude
    "claude-sonnet-4-5":          "Claude Sonnet 4.5",
    "claude-opus-4-5":            "Claude Opus 4.5",
    "claude-haiku-4-5-20251001":  "Claude Haiku 4.5",
    # OpenAI GPT-5 (latest)
    "gpt-5.4":                    "GPT-5.4 · Flagship",
    "gpt-5.4-mini":               "GPT-5.4 Mini · Fast",
    "gpt-5.4-nano":               "GPT-5.4 Nano · Fastest",
    # OpenAI GPT-4 (still available via API)
    "gpt-4o":                     "GPT-4o",
    "gpt-4o-mini":                "GPT-4o Mini",
}

# Models non-admin users can choose from
NON_ADMIN_MODELS = {"claude-sonnet-4-5", "claude-haiku-4-5-20251001"}

OPENAI_MODELS = {"gpt-5.4", "gpt-5.4-mini", "gpt-5.4-nano", "gpt-4o", "gpt-4o-mini"}


# ─────────────────────────────────────────────
# Chat session persistence
# ─────────────────────────────────────────────

async def db_save_session(session_id: str, uid: int, username: str, title: str, messages: list):
    conn = await get_db_conn()
    if not conn: return
    try:
        # PostgreSQL text columns cannot contain \u0000 — strip null bytes
        msg_json = json.dumps(messages, ensure_ascii=False).replace('\x00', '')
        await conn.execute("""
            INSERT INTO chat_sessions (id, uid, username, title, messages, updated_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT (id) DO UPDATE
            SET messages = $5::jsonb, title = $4, updated_at = NOW()
        """, session_id, uid, username, title, msg_json)
    except Exception as e:
        print(f"Session save error: {e}")
    finally:
        await conn.close()

async def db_get_sessions(uid: int) -> list:
    conn = await get_db_conn()
    if not conn: return []
    try:
        rows = await conn.fetch("""
            SELECT id, title, messages, updated_at
            FROM chat_sessions WHERE uid = $1
            ORDER BY updated_at DESC LIMIT 50
        """, uid)
        return [{"id": r["id"], "title": r["title"],
                 "history": json.loads(r["messages"]),
                 "updated_at": r["updated_at"].isoformat()} for r in rows]
    except Exception as e:
        print(f"Session fetch error: {e}")
        return []
    finally:
        await conn.close()

async def db_delete_session(session_id: str, uid: int):
    conn = await get_db_conn()
    if not conn: return
    try:
        await conn.execute("DELETE FROM chat_sessions WHERE id=$1 AND uid=$2", session_id, uid)
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# User memory
# ─────────────────────────────────────────────

async def db_get_memory(uid: int) -> list:
    conn = await get_db_conn()
    if not conn: return []
    try:
        row = await conn.fetchrow("SELECT memories FROM user_memory WHERE uid=$1", uid)
        return json.loads(row["memories"]) if row else []
    except Exception as e:
        print(f"Memory fetch error: {e}")
        return []
    finally:
        await conn.close()

async def db_save_memory(uid: int, username: str, memories: list):
    conn = await get_db_conn()
    if not conn: return
    try:
        await conn.execute("""
            INSERT INTO user_memory (uid, username, memories, updated_at)
            VALUES ($1, $2, $3::jsonb, NOW())
            ON CONFLICT (uid) DO UPDATE
            SET memories = $3::jsonb, updated_at = NOW()
        """, uid, username, json.dumps(memories, ensure_ascii=False))
    except Exception as e:
        print(f"Memory save error: {e}")
    finally:
        await conn.close()

async def extract_and_update_memory(uid: int, username: str, conversation: list):
    """After each conversation, ask Claude to extract memorable facts and update user memory."""
    if len(conversation) < 2:
        return
    try:
        existing = await db_get_memory(uid)
        existing_str = "\n".join(f"- {m}" for m in existing) if existing else "None yet."

        # Build a short summary of the conversation for memory extraction
        conv_summary = []
        for m in conversation[-10:]:  # last 10 messages
            role = m.get("role", "")
            content = m.get("content", "")
            if isinstance(content, str) and content:
                conv_summary.append(f"{role}: {content[:300]}")

        if not conv_summary:
            return

        prompt = f"""You are a memory extractor. Given a conversation, extract ONLY genuinely useful long-term facts about the user that would help personalize future conversations.

EXISTING MEMORIES:
{existing_str}

RECENT CONVERSATION:
{chr(10).join(conv_summary)}

Extract new memorable facts (preferences, habits, important context, recurring needs). 
Rules:
- Only extract facts that are truly useful for future conversations
- Do NOT extract temporary/one-time queries
- Do NOT duplicate existing memories
- Keep each memory concise (max 20 words)
- Return a JSON array of strings, or empty array [] if nothing new
- Max 5 new memories per conversation

Reply ONLY with a JSON array, nothing else. Example: ["Prefers reports in Chinese", "Usually queries March data"]"""

        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={"model": "claude-haiku-4-5-20251001", "max_tokens": 300,
                      "messages": [{"role": "user", "content": prompt}]})
            data = r.json()
            text = "".join(b.get("text","") for b in data.get("content",[]) if b.get("type")=="text")
            text = text.strip()
            if text.startswith("["):
                new_memories = json.loads(text)
                if new_memories:
                    # Merge with existing, keep max 30 total
                    all_memories = existing + new_memories
                    all_memories = all_memories[-30:]
                    await db_save_memory(uid, username, all_memories)
    except Exception as e:
        print(f"Memory extraction error: {e}")

def rebuild_history_with_files(history: list) -> list:
    """Rebuild history messages, re-attaching files from FILE_CACHE where file_id is present.
    This allows Claude to 'see' images/PDFs from earlier in the conversation."""
    rebuilt = []
    for msg in history:
        role = msg.get("role", "user")
        content = msg.get("content", "")
        fid = msg.get("file_id", "")
        fname = msg.get("file_name", "")
        fcontent = msg.get("file_content", "")

        if role == "user" and fid and fid in FILE_CACHE:
            cached = FILE_CACHE[fid]
            doc_type = "document" if cached["media_type"] == "application/pdf" else "image"
            rebuilt.append({
                "role": "user",
                "content": [
                    {"type": doc_type, "source": {"type": "base64", "media_type": cached["media_type"], "data": cached["b64"]}},
                    {"type": "text", "text": f"[Attached file: {cached['name']}]\n\n{content}"}
                ]
            })
        elif role == "user" and fcontent and fname:
            rebuilt.append({
                "role": "user",
                "content": f"=== ATTACHED FILE: {fname} ===\n{fcontent}\n=== END ===\n\n{content}"
            })
        else:
            rebuilt.append({"role": role, "content": content})
    return rebuilt

class ChatRequest(BaseModel):
    message: str
    history: list = []
    file_content: str = ""
    file_name: str = ""
    file_id: str = ""
    role: str = "guest"       # Fallback only — server verifies via session_token
    user_name: str = ""
    user_id: int = 0
    model: str = "claude-haiku-4-5-20251001"
    free_mode: bool = False
    session_id: str = ""
    session_title: str = ""
    session_token: str = ""   # Server-side session token for role verification

def resolve_session(req: ChatRequest) -> dict:
    """Resolve uid/role from server-side session token.
    Falls back to client-supplied values only if token is absent (backward compat).
    Returns dict with uid, role, user_name."""
    if req.session_token and req.session_token in SESSION_STORE:
        s = SESSION_STORE[req.session_token]
        # 用每个 session 自己的 TTL (web=12h, mobile=30天)
        ttl = s.get("ttl_hours", SESSION_TTL_HOURS)
        age = (datetime.datetime.now() - s["created_at"]).total_seconds()
        if age < ttl * 3600:
            return {"uid": s["uid"], "role": s["role"], "user_name": s["name"]}
        else:
            del SESSION_STORE[req.session_token]
    # No valid token in memory — treat as guest
    if req.session_token:
        print(f"SECURITY: invalid/expired session_token, treating as guest")
        return {"uid": 0, "role": "guest", "user_name": ""}
    # No token at all — legacy mode (backward compat during transition)
    return {"uid": req.user_id, "role": req.role, "user_name": req.user_name}


async def resolve_session_with_db(req: ChatRequest) -> dict:
    """Same as resolve_session but also checks DB (for mobile sessions that survived restart).
    If found in DB, restores to in-memory cache."""
    # 先查内存
    if req.session_token and req.session_token in SESSION_STORE:
        return resolve_session(req)

    # 内存没有 → 查 DB (mobile session 重启后会落在这里)
    if req.session_token:
        try:
            conn = await get_db_conn()
            if conn:
                try:
                    row = await conn.fetchrow("""
                        SELECT uid, username, name, role, client_type, created_at
                        FROM user_sessions
                        WHERE token = $1 AND expires_at > NOW()
                    """, req.session_token)
                    if row:
                        # 恢复到内存
                        ttl_hours = 24 * 30 if row["client_type"] == "mobile" else SESSION_TTL_HOURS
                        SESSION_STORE[req.session_token] = {
                            "uid": row["uid"],
                            "username": row["username"],
                            "name": row["name"],
                            "role": row["role"],
                            "created_at": row["created_at"].replace(tzinfo=None) if row["created_at"] else datetime.datetime.now(),
                            "ttl_hours": ttl_hours,
                        }
                        print(f"SESSION: restored from DB for uid={row['uid']} client={row['client_type']}")
                        return {"uid": row["uid"], "role": row["role"], "user_name": row["name"]}
                finally:
                    await conn.close()
        except Exception as e:
            print(f"resolve_session_with_db error: {e}")
        print(f"SECURITY: invalid/expired session_token, treating as guest")
        return {"uid": 0, "role": "guest", "user_name": ""}
    # No token at all — legacy mode
    return {"uid": req.user_id, "role": req.role, "user_name": req.user_name}

@app.post("/chat")
async def chat(req: ChatRequest, background_tasks: BackgroundTasks):
    sess = await resolve_session_with_db(req)
    verified_role = sess["role"]
    verified_uid = sess["uid"]
    verified_name = sess["user_name"]
    perms = ROLE_PERMISSIONS.get(verified_role, ROLE_PERMISSIONS["guest"])

    # Filter tools based on permissions
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax", "odoo_match_payment_to_customer"}
    write_tools = {"odoo_create_record", "odoo_add_order_line", "odoo_confirm_order", "odoo_update_record", "odoo_update_vendor_price", "odoo_create_invoice_from_so", "odoo_register_payment", "odoo_export_invoice_pdf", "release_so"}
    # Tools that expose vendor names, prices, PO costs — only admin / finance / purchase should see these
    cost_tools = {"odoo_find_recent_purchases_by_skus", "odoo_get_product_vendors", "odoo_create_bulk_po", "get_po_with_so_links", "odoo_restock_analysis"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms.get("can_see_finance"):
            continue
        if tname in write_tools and not perms.get("can_write_odoo"):
            continue
        if tname in cost_tools and not perms.get("can_see_cost"):
            continue
        allowed_tools.append(tool)

    has_file = False
    cached_file = None
    if req.file_id and req.file_id in FILE_CACHE:
        cached_file = FILE_CACHE[req.file_id]
        has_file = True

    if has_file and cached_file:
        doc_type = "document" if cached_file["media_type"] == "application/pdf" else "image"
        # Anthropic format (default)
        user_message_content = [
            {"type": doc_type, "source": {"type": "base64", "media_type": cached_file["media_type"], "data": cached_file["b64"]}},
            {"type": "text", "text": f"[Attached file: {cached_file['name']}]\n\nUser question: {req.message}"}
        ]
        # Prepare OpenAI format separately
        openai_image_content = [
            {"type": "image_url", "image_url": {"url": f"data:{cached_file['media_type']};base64,{cached_file['b64']}"}},
            {"type": "text", "text": f"[Attached file: {cached_file['name']}]\n\nUser question: {req.message}"}
        ]
    elif req.file_content and req.file_name:
        user_message_content = (
            f"=== ATTACHED FILE: {req.file_name} ===\n{req.file_content}\n=== END OF FILE ===\n\nUser question: {req.message}"
        )
        openai_image_content = None
    else:
        user_message_content = req.message
        openai_image_content = None

    messages = rebuild_history_with_files(req.history) + [{"role": "user", "content": user_message_content}]
    headers = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    memories = []
    if verified_uid:
        memories = await db_get_memory(verified_uid)

    # Determine model — admin/finance default to Sonnet, others to Haiku
    default_model = "claude-sonnet-4-5" if verified_role in ("admin", "finance") else "claude-haiku-4-5-20251001"
    if verified_role == "admin" and req.model in ALLOWED_MODELS:
        selected_model = req.model
    elif verified_role != "admin" and req.model in NON_ADMIN_MODELS:
        selected_model = req.model
    else:
        selected_model = default_model

    system_prompt = get_system_prompt(verified_role, verified_name, verified_uid, req.free_mode, memories)
    tool_context = {"uid": verified_uid, "username": verified_name, "role": verified_role}

    # Route to OpenAI if selected
    if selected_model in OPENAI_MODELS:
        # For OpenAI, swap image format if file attached
        if has_file and openai_image_content:
            oai_messages = req.history + [{"role": "user", "content": openai_image_content}]
        else:
            oai_messages = messages
        reply = await chat_openai(oai_messages, system_prompt, selected_model, allowed_tools, context=tool_context)
    else:
        # Anthropic path
        async with httpx.AsyncClient(timeout=300) as c:
            current_messages = list(messages)
            for _ in range(8):
                r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                    "model": selected_model,
                    "max_tokens": 4096,
                    "system": system_prompt,
                    "tools": allowed_tools,
                    "messages": current_messages
                })
                d = r.json()
                if "error" in d:
                    return {"reply": f"API error: {d['error'].get('message', str(d['error']))}"}
                if d.get("stop_reason") == "tool_use":
                    tool_results = []
                    for block in d.get("content", []):
                        if block.get("type") == "tool_use":
                            result = await run_tool(block["name"], block.get("input", {}), context=tool_context)
                            tool_results.append({"type":"tool_result","tool_use_id":block["id"],"content":result})
                    current_messages.append({"role": "assistant", "content": d["content"]})
                    current_messages.append({"role": "user", "content": tool_results})
                else:
                    break
            reply = "".join(b.get("text","") for b in d.get("content",[]) if b.get("type")=="text")

    reply = reply or "Sorry, no response generated."

    # Persist session to DB in background
    if req.session_id and req.user_id:
        full_history = req.history + [
            {"role": "user", "content": req.message},
            {"role": "assistant", "content": reply}
        ]
        background_tasks.add_task(
            db_save_session,
            req.session_id, req.user_id, req.user_name,
            req.session_title or req.message[:20],
            full_history
        )
        # Extract memory every 4 turns
        if len(full_history) % 8 == 0:
            background_tasks.add_task(
                extract_and_update_memory,
                req.user_id, req.user_name, full_history
            )

    return {"reply": reply}

# ─────────────────────────────────────────────
# Streaming chat (SSE) — Claude models only. OpenAI falls back to /chat.
# ─────────────────────────────────────────────

@app.post("/chat/stream")
async def chat_stream(req: ChatRequest, background_tasks: BackgroundTasks):
    """Server-Sent Events streaming endpoint.
    Event types sent to client:
      - {type: "text", delta: "..."}        incremental text
      - {type: "tool_use", name: "..."}     tool being invoked
      - {type: "tool_result", name: "..."}  tool finished
      - {type: "ping"}                      heartbeat (keep connection alive)
      - {type: "done"}                      stream complete
      - {type: "error", message: "..."}     error occurred
    """
    from fastapi.responses import StreamingResponse
    import asyncio as _asyncio

    # Heartbeat wrapper: interleaves {"type":"ping"} into an SSE generator every
    # `interval` seconds of silence, so CDNs/proxies/browsers don't kill an idle
    # connection during long tool calls. The frontend ignores ping events
    # but uses them to know the server is still alive.
    async def with_heartbeat(inner_gen, interval: float = 10.0):
        ping_payload = f"data: {json.dumps({'type': 'ping'})}\n\n"
        it = inner_gen.__aiter__()
        next_task = None
        try:
            while True:
                if next_task is None:
                    next_task = _asyncio.ensure_future(it.__anext__())
                try:
                    chunk = await _asyncio.wait_for(_asyncio.shield(next_task), timeout=interval)
                    next_task = None
                    yield chunk
                except _asyncio.TimeoutError:
                    # No event from inner generator in `interval` seconds → send ping.
                    # Keep next_task alive to consume its eventual value on the next loop.
                    yield ping_payload
                except StopAsyncIteration:
                    next_task = None
                    break
        finally:
            if next_task is not None and not next_task.done():
                next_task.cancel()
                try:
                    await next_task
                except BaseException:
                    pass

    sess = await resolve_session_with_db(req)
    verified_role = sess["role"]
    verified_uid = sess["uid"]
    verified_name = sess["user_name"]
    perms = ROLE_PERMISSIONS.get(verified_role, ROLE_PERMISSIONS["guest"])

    # Filter tools based on permissions
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax", "odoo_match_payment_to_customer"}
    write_tools = {"odoo_create_record", "odoo_add_order_line", "odoo_confirm_order", "odoo_update_record", "odoo_update_vendor_price", "odoo_create_invoice_from_so", "odoo_register_payment", "odoo_export_invoice_pdf", "release_so"}
    # Tools that expose vendor names, prices, PO costs — only admin / finance / purchase should see these
    cost_tools = {"odoo_find_recent_purchases_by_skus", "odoo_get_product_vendors", "odoo_create_bulk_po", "get_po_with_so_links", "odoo_restock_analysis"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms.get("can_see_finance"):
            continue
        if tname in write_tools and not perms.get("can_write_odoo"):
            continue
        if tname in cost_tools and not perms.get("can_see_cost"):
            continue
        allowed_tools.append(tool)

    has_file = False
    cached_file = None
    if req.file_id and req.file_id in FILE_CACHE:
        cached_file = FILE_CACHE[req.file_id]
        has_file = True

    if has_file and cached_file:
        doc_type = "document" if cached_file["media_type"] == "application/pdf" else "image"
        user_message_content = [
            {"type": doc_type, "source": {"type": "base64", "media_type": cached_file["media_type"], "data": cached_file["b64"]}},
            {"type": "text", "text": f"[Attached file: {cached_file['name']}]\n\nUser question: {req.message}"}
        ]
    elif req.file_content and req.file_name:
        user_message_content = (
            f"=== ATTACHED FILE: {req.file_name} ===\n{req.file_content}\n=== END OF FILE ===\n\nUser question: {req.message}"
        )
    else:
        user_message_content = req.message

    messages = rebuild_history_with_files(req.history) + [{"role": "user", "content": user_message_content}]

    # Load memory & build system prompt
    memories = []
    if verified_uid:
        memories = await db_get_memory(verified_uid)

    # Determine model — admin/finance default to Sonnet, others to Haiku
    default_model = "claude-sonnet-4-5" if verified_role in ("admin", "finance") else "claude-haiku-4-5-20251001"
    if verified_role == "admin" and req.model in ALLOWED_MODELS:
        selected_model = req.model
    elif verified_role != "admin" and req.model in NON_ADMIN_MODELS:
        selected_model = req.model
    else:
        selected_model = default_model

    # Context passed to tools (for buyer attribution on PO creation, etc.)
    tool_context = {"uid": verified_uid, "username": verified_name, "role": verified_role}

    if selected_model in OPENAI_MODELS:
        # OpenAI streaming path with tool-call support
        openai_key = os.getenv("OPENAI_API_KEY", "")
        system_prompt = get_system_prompt(verified_role, verified_name, verified_uid, req.free_mode, memories)

        # Build OpenAI-format messages
        if has_file and cached_file:
            oai_image_content = [
                {"type": "image_url", "image_url": {"url": f"data:{cached_file['media_type']};base64,{cached_file['b64']}"}},
                {"type": "text", "text": f"[Attached file: {cached_file['name']}]\n\nUser question: {req.message}"}
            ]
            oai_messages_input = req.history + [{"role": "user", "content": oai_image_content}]
        else:
            oai_messages_input = messages

        async def openai_stream():
            if not openai_key:
                yield f"data: {json.dumps({'type': 'error', 'message': 'OPENAI_API_KEY not configured'})}\n\n"
                return

            oai_tools = convert_tools_to_openai(allowed_tools)
            current_messages = [{"role": "system", "content": system_prompt}] + oai_messages_input
            full_reply_text = ""

            try:
                for iteration in range(8):
                    payload = {
                        "model": selected_model,
                        "messages": current_messages,
                        "stream": True,
                    }
                    # GPT-5.x uses max_completion_tokens
                    if selected_model.startswith("gpt-5"):
                        payload["max_completion_tokens"] = 4096
                    else:
                        payload["max_tokens"] = 4096
                    if oai_tools:
                        payload["tools"] = oai_tools
                        payload["tool_choice"] = "auto"

                    # Accumulate the assistant message as we stream
                    assistant_content = ""
                    # tool_calls[index] -> {"id": str, "name": str, "arguments": str}
                    tool_calls_acc = {}
                    finish_reason = None

                    async with httpx.AsyncClient(timeout=300) as c:
                        async with c.stream(
                            "POST",
                            "https://api.openai.com/v1/chat/completions",
                            headers={"Authorization": f"Bearer {openai_key}", "Content-Type": "application/json"},
                            json=payload
                        ) as r:
                            if r.status_code != 200:
                                body = await r.aread()
                                err_txt = body.decode("utf-8", errors="ignore")[:500]
                                yield f"data: {json.dumps({'type': 'error', 'message': f'OpenAI {r.status_code}: {err_txt}'})}\n\n"
                                return

                            async for line in r.aiter_lines():
                                if not line or not line.startswith("data: "):
                                    continue
                                data_str = line[6:]
                                if data_str.strip() == "[DONE]":
                                    break
                                try:
                                    event = json.loads(data_str)
                                except Exception:
                                    continue

                                choices = event.get("choices", [])
                                if not choices:
                                    continue
                                choice = choices[0]
                                delta = choice.get("delta", {}) or {}
                                fr = choice.get("finish_reason")
                                if fr:
                                    finish_reason = fr

                                # Text content delta
                                text_chunk = delta.get("content")
                                if text_chunk:
                                    assistant_content += text_chunk
                                    full_reply_text += text_chunk
                                    yield f"data: {json.dumps({'type': 'text', 'delta': text_chunk})}\n\n"

                                # Tool call deltas
                                tc_deltas = delta.get("tool_calls") or []
                                for tcd in tc_deltas:
                                    idx = tcd.get("index", 0)
                                    if idx not in tool_calls_acc:
                                        tool_calls_acc[idx] = {"id": "", "name": "", "arguments": ""}
                                    entry = tool_calls_acc[idx]
                                    if tcd.get("id"):
                                        entry["id"] = tcd["id"]
                                    fn = tcd.get("function") or {}
                                    if fn.get("name"):
                                        # Announce first time we see a name
                                        if not entry["name"]:
                                            yield f"data: {json.dumps({'type': 'tool_use', 'name': fn['name']})}\n\n"
                                        entry["name"] = fn["name"]
                                    if fn.get("arguments"):
                                        entry["arguments"] += fn["arguments"]

                    # Decide next action
                    if finish_reason == "tool_calls" and tool_calls_acc:
                        # Build the assistant message with tool_calls for the next round
                        tc_list = []
                        for idx in sorted(tool_calls_acc.keys()):
                            e = tool_calls_acc[idx]
                            tc_list.append({
                                "id": e["id"],
                                "type": "function",
                                "function": {"name": e["name"], "arguments": e["arguments"] or "{}"}
                            })
                        current_messages.append({
                            "role": "assistant",
                            "content": assistant_content or None,
                            "tool_calls": tc_list
                        })
                        # Execute tools
                        for tc in tc_list:
                            fn_name = tc["function"]["name"]
                            try:
                                fn_args = json.loads(tc["function"]["arguments"])
                            except Exception:
                                fn_args = {}
                            result = await run_tool(fn_name, fn_args, context=tool_context)
                            yield f"data: {json.dumps({'type': 'tool_result', 'name': fn_name})}\n\n"
                            current_messages.append({
                                "role": "tool",
                                "tool_call_id": tc["id"],
                                "content": result
                            })
                        # Loop for another streaming round
                        continue
                    else:
                        # Natural stop
                        break

                # Persist session
                if req.session_id and req.user_id:
                    full_history = req.history + [
                        {"role": "user", "content": req.message},
                        {"role": "assistant", "content": full_reply_text}
                    ]
                    asyncio.create_task(db_save_session(
                        req.session_id, req.user_id, req.user_name,
                        req.session_title or req.message[:20], full_history
                    ))
                    if len(full_history) % 8 == 0:
                        asyncio.create_task(extract_and_update_memory(
                            req.user_id, req.user_name, full_history
                        ))

                yield f"data: {json.dumps({'type': 'done'})}\n\n"

            except asyncio.CancelledError:
                print(f"OpenAI stream cancelled. Reply so far: {len(full_reply_text)} chars")
                if req.session_id and req.user_id and full_reply_text:
                    full_history = req.history + [
                        {"role": "user", "content": req.message},
                        {"role": "assistant", "content": full_reply_text + "\n\n[stopped by user]"}
                    ]
                    asyncio.create_task(db_save_session(
                        req.session_id, req.user_id, req.user_name,
                        req.session_title or req.message[:20], full_history
                    ))
                raise
            except Exception as e:
                print(f"OpenAI stream error: {e}")
                yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

        return StreamingResponse(with_heartbeat(openai_stream()), media_type="text/event-stream", headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
        })

    # Anthropic streaming path
    system_prompt = get_system_prompt(verified_role, verified_name, verified_uid, req.free_mode, memories)
    headers = {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    async def claude_stream():
        current_messages = list(messages)
        full_reply_text = ""
        try:
            for iteration in range(8):
                payload = {
                    "model": selected_model,
                    "max_tokens": 4096,
                    "system": system_prompt,
                    "tools": allowed_tools,
                    "messages": current_messages,
                    "stream": True,
                }
                async with httpx.AsyncClient(timeout=300) as c:
                    async with c.stream("POST", "https://api.anthropic.com/v1/messages",
                                        headers=headers, json=payload) as r:
                        if r.status_code != 200:
                            body = await r.aread()
                            err_txt = body.decode("utf-8", errors="ignore")[:500]
                            yield f"data: {json.dumps({'type': 'error', 'message': f'API {r.status_code}: {err_txt}'})}\n\n"
                            return

                        # Accumulate the assistant's content blocks so we can replay on tool_use loop
                        content_blocks = []  # list of {type, ...}
                        current_block = None
                        current_tool_input_buf = ""
                        stop_reason = None

                        async for line in r.aiter_lines():
                            if not line or not line.startswith("data: "):
                                continue
                            try:
                                event = json.loads(line[6:])
                            except Exception:
                                continue

                            et = event.get("type", "")

                            if et == "content_block_start":
                                block = event.get("content_block", {})
                                idx = event.get("index", 0)
                                if block.get("type") == "text":
                                    current_block = {"type": "text", "text": "", "_idx": idx}
                                elif block.get("type") == "tool_use":
                                    current_block = {
                                        "type": "tool_use",
                                        "id": block.get("id"),
                                        "name": block.get("name"),
                                        "input": {},
                                        "_idx": idx
                                    }
                                    current_tool_input_buf = ""
                                    # Tell frontend a tool is being invoked
                                    yield f"data: {json.dumps({'type': 'tool_use', 'name': block.get('name')})}\n\n"

                            elif et == "content_block_delta":
                                delta = event.get("delta", {})
                                dt = delta.get("type", "")
                                if dt == "text_delta" and current_block and current_block.get("type") == "text":
                                    text_chunk = delta.get("text", "")
                                    current_block["text"] += text_chunk
                                    full_reply_text += text_chunk
                                    yield f"data: {json.dumps({'type': 'text', 'delta': text_chunk})}\n\n"
                                elif dt == "input_json_delta" and current_block and current_block.get("type") == "tool_use":
                                    current_tool_input_buf += delta.get("partial_json", "")

                            elif et == "content_block_stop":
                                if current_block:
                                    if current_block.get("type") == "tool_use":
                                        # Finalize tool input
                                        try:
                                            current_block["input"] = json.loads(current_tool_input_buf) if current_tool_input_buf else {}
                                        except Exception as e:
                                            print(f"Tool input JSON parse error: {e}, buf={current_tool_input_buf[:200]}")
                                            current_block["input"] = {}
                                    # Strip internal index field
                                    cb = {k: v for k, v in current_block.items() if not k.startswith("_")}
                                    content_blocks.append(cb)
                                    current_block = None
                                    current_tool_input_buf = ""

                            elif et == "message_delta":
                                delta = event.get("delta", {})
                                if "stop_reason" in delta:
                                    stop_reason = delta["stop_reason"]

                            elif et == "message_stop":
                                pass

                            elif et == "error":
                                err = event.get("error", {})
                                msg = err.get("message", str(err))
                                yield f"data: {json.dumps({'type': 'error', 'message': msg})}\n\n"
                                return

                # After stream for this iteration ends, decide next step
                if stop_reason == "tool_use":
                    # Run all tool_use blocks and loop
                    tool_results = []
                    for block in content_blocks:
                        if block.get("type") == "tool_use":
                            result = await run_tool(block["name"], block.get("input", {}), context=tool_context)
                            yield f"data: {json.dumps({'type': 'tool_result', 'name': block['name']})}\n\n"
                            tool_results.append({
                                "type": "tool_result",
                                "tool_use_id": block["id"],
                                "content": result
                            })
                    current_messages.append({"role": "assistant", "content": content_blocks})
                    current_messages.append({"role": "user", "content": tool_results})
                    # Continue outer for loop for another streaming iteration
                    continue
                else:
                    # Natural stop — we're done
                    break

            # Persist session
            if req.session_id and req.user_id:
                full_history = req.history + [
                    {"role": "user", "content": req.message},
                    {"role": "assistant", "content": full_reply_text}
                ]
                # Use create_task since BackgroundTasks doesn't run inside StreamingResponse reliably
                asyncio.create_task(db_save_session(
                    req.session_id, req.user_id, req.user_name,
                    req.session_title or req.message[:20], full_history
                ))
                if len(full_history) % 8 == 0:
                    asyncio.create_task(extract_and_update_memory(
                        req.user_id, req.user_name, full_history
                    ))

            yield f"data: {json.dumps({'type': 'done'})}\n\n"

        except asyncio.CancelledError:
            # Client disconnected (Stop button pressed)
            print(f"Client disconnected mid-stream. Reply so far: {len(full_reply_text)} chars")
            # Still save partial reply if we got something useful
            if req.session_id and req.user_id and full_reply_text:
                full_history = req.history + [
                    {"role": "user", "content": req.message},
                    {"role": "assistant", "content": full_reply_text + "\n\n[stopped by user]"}
                ]
                asyncio.create_task(db_save_session(
                    req.session_id, req.user_id, req.user_name,
                    req.session_title or req.message[:20], full_history
                ))
            raise
        except Exception as e:
            print(f"Stream error: {e}")
            yield f"data: {json.dumps({'type': 'error', 'message': str(e)})}\n\n"

    return StreamingResponse(with_heartbeat(claude_stream()), media_type="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",  # Disable nginx buffering
    })

# ─────────────────────────────────────────────
# Session & Memory API
# ─────────────────────────────────────────────

@app.get("/sessions/{uid}")
async def get_sessions(uid: int):
    sessions = await db_get_sessions(uid)
    return {"sessions": sessions}

@app.delete("/sessions/{session_id}")
async def delete_session(session_id: str, uid: int):
    await db_delete_session(session_id, uid)
    return {"status": "deleted"}

@app.get("/memory/{uid}")
async def get_memory(uid: int):
    memories = await db_get_memory(uid)
    return {"memories": memories}

@app.delete("/memory/{uid}")
async def clear_memory(uid: int):
    conn = await get_db_conn()
    if conn:
        await conn.execute("DELETE FROM user_memory WHERE uid=$1", uid)
        await conn.close()
    return {"status": "cleared"}

# ─────────────────────────────────────────────
# Document Management (Admin)
# ─────────────────────────────────────────────

ALLOWED_CATEGORIES = ["service_manual", "product_manual", "spec_sheet", "employee_handbook", "after_sales", "warranty", "general"]

@app.post("/admin/upload-doc")
async def upload_doc(
    background_tasks: BackgroundTasks,
    request: Request,
    file: UploadFile = File(...),
    category: str = "general",
    description: str = "",
    admin_key: str = "",
    key: str = ""
):
    # Also read from query params (more reliable with multipart)
    qp = request.query_params
    effective_key = qp.get("admin_key", "") or admin_key or key
    cat_from_qp = qp.get("category", "")
    desc_from_qp = qp.get("description", "")
    if cat_from_qp:
        category = cat_from_qp
    if desc_from_qp:
        description = desc_from_qp

    if effective_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    if not R2_ACCOUNT_ID or not R2_ACCESS_KEY:
        return {"error": "R2 not configured."}
    if category not in ALLOWED_CATEGORIES:
        category = "general"

    try:
        file_bytes = await file.read()
        file_size = len(file_bytes)
        doc_id = str(uuid.uuid4())
        ext = file.filename.rsplit('.', 1)[-1].lower() if '.' in file.filename else 'bin'
        r2_key = f"{category}/{doc_id}.{ext}"
        public_url = f"{R2_PUBLIC_URL}/{r2_key}"

        # Determine mime type
        mime_map = {
            'pdf': 'application/pdf',
            'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            'doc': 'application/msword',
            'txt': 'text/plain',
            'md': 'text/markdown',
            'png': 'image/png',
            'jpg': 'image/jpeg',
            'jpeg': 'image/jpeg',
            'webp': 'image/webp',
        }
        mime_type = mime_map.get(ext, 'application/octet-stream')

        # Upload to R2
        ok = await r2_upload(file_bytes, r2_key, mime_type)
        if not ok:
            return {"error": "Failed to upload to R2. Check R2 credentials."}

        # Save metadata to DB
        conn = await get_db_conn()
        if conn:
            await conn.execute("""
                INSERT INTO documents (id, filename, original_name, category, description, file_size, mime_type, r2_key, public_url)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, doc_id, r2_key, file.filename, category, description, file_size, mime_type, r2_key, public_url)
            await conn.close()

        # Process text extraction and knowledge base indexing in background
        background_tasks.add_task(
            _process_doc_background,
            doc_id, file.filename, file_bytes, mime_type, category
        )

        return {
            "status": "uploaded",
            "doc_id": doc_id,
            "filename": file.filename,
            "category": category,
            "public_url": public_url,
            "message": "File uploaded. Knowledge base indexing started in background (may take 1-2 min)."
        }

    except Exception as e:
        return {"error": str(e)}

async def _process_doc_background(doc_id: str, filename: str, file_bytes: bytes, mime_type: str, category: str):
    """Background task: extract text, auto-generate description with model numbers, and index."""
    print(f"DOC INDEX START: {filename} ({len(file_bytes)//1024}KB) category={category}")
    text = await extract_text_from_file(file_bytes, filename, mime_type)
    if not text:
        print(f"DOC INDEX FAIL: {filename} — no text extracted")
        return

    # Auto-extract model numbers and keywords from text to enrich description
    try:
        async with httpx.AsyncClient(timeout=30) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 200,
                    "messages": [{"role": "user", "content": f"Extract all product model numbers, SKUs, and brand names from this document. Return ONLY a comma-separated list of identifiers, nothing else. Max 20 items.\n\nDocument name: {filename}\n\nContent (first 2000 chars):\n{text[:2000]}"}]
                }
            )
            keywords = r.json().get("content", [{}])[0].get("text", "").strip()
            if keywords:
                conn = await get_db_conn()
                if conn:
                    try:
                        # Only update description if it's empty or generic
                        row = await conn.fetchrow("SELECT description FROM documents WHERE id=$1", doc_id)
                        existing_desc = (row["description"] or "").strip() if row else ""
                        if not existing_desc or existing_desc == filename.rsplit(".", 1)[0]:
                            new_desc = keywords[:500]
                            await conn.execute("UPDATE documents SET description=$1 WHERE id=$2", new_desc, doc_id)
                            print(f"DOC AUTO-DESC: {filename} → {new_desc[:100]}")
                    finally:
                        await conn.close()
    except Exception as e:
        print(f"DOC AUTO-DESC ERROR: {e}")

    count = await process_document_to_kb(doc_id, filename, text, category)
    print(f"DOC INDEX OK: {filename} → {count} chunks")

@app.get("/admin/documents")
async def list_documents(admin_key: str = ""):
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        rows = await conn.fetch("""
            SELECT id, original_name, category, description, file_size, chunk_count, public_url, created_at
            FROM documents ORDER BY created_at DESC
        """)
        return {"documents": [dict(r) for r in rows]}
    finally:
        await conn.close()

@app.get("/admin/documents/{doc_id}/chunks")
async def get_document_chunks(doc_id: str, admin_key: str = ""):
    """Return all knowledge chunks for a specific document, for preview/debugging."""
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        rows = await conn.fetch("""
            SELECT id, chunk_text, category, page_title
            FROM knowledge_chunks
            WHERE site_url = $1
            ORDER BY id ASC
        """, f"doc:{doc_id}")
        chunks = []
        for i, r in enumerate(rows):
            chunks.append({
                "index": i + 1,
                "id": r["id"],
                "text": r["chunk_text"],
                "category": r["category"],
                "page_title": r["page_title"],
                "char_count": len(r["chunk_text"] or ""),
            })
        return {"doc_id": doc_id, "total": len(chunks), "chunks": chunks}
    finally:
        await conn.close()

@app.get("/admin/kb-search")
async def kb_search_test(q: str = "", admin_key: str = "", limit: int = 5):
    """Test knowledge base search — returns top matching chunks for a query."""
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    if not q.strip():
        return {"error": "Query q is required"}
    limit = max(1, min(int(limit), 20))

    embedding = await get_embedding(q.strip())
    if not embedding:
        return {"error": "Failed to generate embedding — check OPENAI_API_KEY"}

    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        rows = await conn.fetch("""
            SELECT kc.id, kc.chunk_text, kc.site_name, kc.page_title, kc.category,
                   kc.site_url,
                   1 - (kc.embedding <=> $1::vector) AS score
            FROM knowledge_chunks kc
            WHERE kc.embedding IS NOT NULL
            ORDER BY kc.embedding <=> $1::vector
            LIMIT $2
        """, json.dumps(embedding), limit)
        results = []
        for r in rows:
            doc_id = r["site_url"].replace("doc:", "") if r["site_url"] and r["site_url"].startswith("doc:") else None
            results.append({
                "score": round(float(r["score"]), 4),
                "chunk_id": r["id"],
                "doc_id": doc_id,
                "site_name": r["site_name"],
                "page_title": r["page_title"],
                "category": r["category"],
                "text": r["chunk_text"],
                "char_count": len(r["chunk_text"] or ""),
            })
        return {"query": q, "limit": limit, "results": results}
    finally:
        await conn.close()

@app.delete("/admin/documents/{doc_id}")
async def delete_document(doc_id: str, admin_key: str = ""):
    if admin_key != os.getenv("ADMIN_KEY", "chumart2024"):
        return {"error": "Invalid admin key"}
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        row = await conn.fetchrow("SELECT r2_key FROM documents WHERE id=$1", doc_id)
        if not row:
            return {"error": "Document not found"}
        # Delete from R2
        await r2_delete(row["r2_key"])
        # Delete chunks from knowledge base
        await conn.execute("DELETE FROM knowledge_chunks WHERE site_url=$1", f"doc:{doc_id}")
        # Delete metadata
        await conn.execute("DELETE FROM documents WHERE id=$1", doc_id)
        return {"status": "deleted"}
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# Signed URL for secure document downloads
# ─────────────────────────────────────────────

@app.get("/docs/signed-url/{doc_id}")
async def get_signed_url(doc_id: str, download: bool = True):
    """Generate a time-limited signed URL and redirect directly to the file for download."""
    from fastapi.responses import RedirectResponse
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        row = await conn.fetchrow("SELECT r2_key, original_name FROM documents WHERE id=$1", doc_id)
        if not row:
            return {"error": "Document not found"}

        loop = asyncio.get_event_loop()
        client = get_r2_client()
        if not client:
            return {"error": "Storage not configured"}

        params = {'Bucket': R2_BUCKET, 'Key': row['r2_key']}
        if download:
            # Force browser to download with original filename
            params['ResponseContentDisposition'] = f"attachment; filename=\"{row['original_name']}\""

        signed_url = await loop.run_in_executor(None, lambda: client.generate_presigned_url(
            'get_object', Params=params, ExpiresIn=3600
        ))
        # 302 redirect → browser downloads the file directly
        return RedirectResponse(url=signed_url, status_code=302)
    except Exception as e:
        return {"error": str(e)}
    finally:
        await conn.close()

# ─────────────────────────────────────────────
# Health
# ─────────────────────────────────────────────

@app.get("/invoice-stats")
async def invoice_stats(year: int, month: int):
    return await monthly_tax(year, month)


@app.get("/admin/pending-payments")
async def admin_pending_payments():
    """View all pending partial payments awaiting combo completion."""
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB unavailable"}
    try:
        rows = await conn.fetch("""
            SELECT so_name, so_amount,
                   array_agg(channel) as channels,
                   array_agg(amount) as amounts,
                   SUM(amount) as accumulated,
                   so_amount - SUM(amount) as remaining,
                   MIN(created_at) as first_payment,
                   MAX(created_at) as last_payment
            FROM pending_payments
            WHERE status = 'pending'
            GROUP BY so_name, so_amount, so_id
            ORDER BY MAX(created_at) DESC
        """)
        result = []
        for r in rows:
            result.append({
                "so_name": r["so_name"],
                "so_amount": float(r["so_amount"]),
                "channels": list(r["channels"]),
                "amounts": [float(a) for a in r["amounts"]],
                "accumulated": float(r["accumulated"]),
                "remaining": float(r["remaining"]),
                "first_payment": r["first_payment"].isoformat() if r["first_payment"] else None,
                "last_payment": r["last_payment"].isoformat() if r["last_payment"] else None,
            })
        return {"pending": result, "count": len(result)}
    finally:
        await conn.close()


# ─────────────────────────────────────────────
# Odoo Discuss Bot endpoint
# ─────────────────────────────────────────────

class OdooBotRequest(BaseModel):
    uid: int                    # Odoo user ID who sent the message
    message: str                # The message text
    channel_id: int = 0         # Discuss channel ID (for reply routing)
    author_name: str = ""       # Display name of the sender
    bot_secret: str = ""        # Shared secret to authenticate Odoo → Railway

# In-memory conversation history per Odoo user (last 10 turns)
ODOO_BOT_HISTORY: dict = {}  # uid -> list of {role, content}


# Map tool name → friendly progress label (zh + en) shown in Discuss as "正在..."/"Working on..."
TOOL_PROGRESS_LABELS_ZH = {
    "list_reminders":              "🔍 正在查询提醒列表...",
    "create_reminder":             "⏰ 正在创建提醒...",
    "cancel_reminder":             "🗑 正在取消提醒...",
    "update_reminder":             "✏️ 正在更新提醒...",
    "list_events":                 "📅 正在查询日程...",
    "create_event":                "📅 正在创建日程...",
    "delete_event":                "🗑 正在删除日程...",
    "set_my_contact":              "📇 正在更新联系方式...",
    "odoo_query":                  "🔍 正在查询 Odoo 数据...",
    "odoo_query_count":            "🔢 正在统计...",
    "odoo_search_product":         "📦 正在搜索产品...",
    "odoo_search_partner":         "👤 正在搜索客户...",
    "odoo_check_stock":            "📦 正在查库存...",
    "odoo_recent_sales":           "💰 正在查最近销售...",
    "odoo_restock_analysis":       "📦 正在分析补货...",
    "odoo_create_record":          "✏️ 正在创建记录...",
    "odoo_update_record":          "✏️ 正在更新记录...",
    "odoo_add_order_line":         "➕ 正在添加订单行...",
    "odoo_confirm_order":          "✅ 正在确认订单...",
    "odoo_update_vendor_price":    "💲 正在更新供应商价格...",
    "odoo_create_invoice_from_so": "📄 正在创建发票...",
    "odoo_register_payment":       "💰 正在登记付款...",
    "odoo_export_invoice_pdf":     "📥 正在导出 PDF...",
    "release_so":                  "🚀 正在 release 订单...",
    "check_so_payment_status":     "🔎 正在查 SO 付款状态...",
    "search_knowledge_base":       "📚 正在搜索知识库...",
    "get_monthly_tax":             "📊 正在生成月度税报表...",
    "get_quarterly_tax":           "📊 正在生成季度税报表...",
    "get_monthly_sales":           "📊 正在生成销售提成报表...",
    "find_missing_tax":            "🔍 正在检查缺税订单...",
    "save_user_memory":            "💾 正在保存记忆...",
    "search_user_memories":        "🧠 正在查询历史记忆...",
}

TOOL_PROGRESS_LABELS_EN = {
    "list_reminders":              "🔍 Fetching reminders...",
    "create_reminder":             "⏰ Creating reminder...",
    "cancel_reminder":             "🗑 Cancelling reminder...",
    "update_reminder":             "✏️ Updating reminder...",
    "list_events":                 "📅 Fetching events...",
    "create_event":                "📅 Creating event...",
    "delete_event":                "🗑 Deleting event...",
    "set_my_contact":              "📇 Updating contact...",
    "odoo_query":                  "🔍 Querying Odoo...",
    "odoo_query_count":            "🔢 Counting records...",
    "odoo_search_product":         "📦 Searching products...",
    "odoo_search_partner":         "👤 Searching customers...",
    "odoo_check_stock":            "📦 Checking stock...",
    "odoo_recent_sales":           "💰 Fetching recent sales...",
    "odoo_restock_analysis":       "📦 Analyzing restock...",
    "odoo_create_record":          "✏️ Creating record...",
    "odoo_update_record":          "✏️ Updating record...",
    "odoo_add_order_line":         "➕ Adding order line...",
    "odoo_confirm_order":          "✅ Confirming order...",
    "odoo_update_vendor_price":    "💲 Updating vendor price...",
    "odoo_create_invoice_from_so": "📄 Creating invoice...",
    "odoo_register_payment":       "💰 Registering payment...",
    "odoo_export_invoice_pdf":     "📥 Exporting PDF...",
    "release_so":                  "🚀 Releasing order...",
    "check_so_payment_status":     "🔎 Checking payment status...",
    "search_knowledge_base":       "📚 Searching knowledge base...",
    "get_monthly_tax":             "📊 Generating monthly tax report...",
    "get_quarterly_tax":           "📊 Generating quarterly tax report...",
    "get_monthly_sales":           "📊 Generating sales commission report...",
    "find_missing_tax":            "🔍 Checking missing tax...",
    "save_user_memory":            "💾 Saving memory...",
    "search_user_memories":        "🧠 Searching past memories...",
}


def _detect_user_language(text: str) -> str:
    """Detect if user message is Chinese or English. Returns 'zh' or 'en'.
    Threshold: if >= 20% of characters are CJK, treat as Chinese."""
    if not text:
        return "en"
    cjk_count = sum(1 for ch in text if '\u4e00' <= ch <= '\u9fff')
    if cjk_count == 0:
        return "en"
    if cjk_count / max(len(text), 1) >= 0.2:
        return "zh"
    return "en"


# Specific labels for common Odoo models when tool is odoo_query / odoo_query_count
ODOO_MODEL_LABELS = {
    "sale.order":          ("🛒 正在查销售订单 (SO)...",  "🛒 Querying Sales Orders..."),
    "purchase.order":      ("📋 正在查采购单 (PO)...",    "📋 Querying Purchase Orders..."),
    "stock.picking":       ("🚚 正在查送货单...",         "🚚 Querying delivery orders..."),
    "stock.move":          ("📦 正在查库存移动...",       "📦 Querying stock moves..."),
    "stock.move.line":     ("📦 正在查库存明细...",       "📦 Querying stock move lines..."),
    "stock.quant":         ("📦 正在查库存余额...",       "📦 Querying stock quantities..."),
    "product.template":    ("🏷 正在查产品...",           "🏷 Querying products..."),
    "product.product":     ("🏷 正在查产品明细...",       "🏷 Querying product variants..."),
    "res.partner":         ("👤 正在查客户/供应商...",    "👤 Querying contacts..."),
    "account.move":        ("📄 正在查发票...",           "📄 Querying invoices..."),
    "account.payment":     ("💰 正在查付款记录...",       "💰 Querying payments..."),
    "account.move.line":   ("📊 正在查账目...",           "📊 Querying journal items..."),
    "repair.order":        ("🔧 正在查维修单...",         "🔧 Querying repair orders..."),
    "mrp.production":      ("⚙️ 正在查生产单...",         "⚙️ Querying manufacturing orders..."),
    "hr.employee":         ("👷 正在查员工...",           "👷 Querying employees..."),
    "crm.lead":            ("🎯 正在查商机...",           "🎯 Querying leads..."),
}


def _get_tool_progress_label(tool_name: str, lang: str, tool_input: dict = None) -> str:
    """Get progress label in user's language. For odoo_query/odoo_query_count,
    use a model-specific label if available."""
    # 对 odoo_query / odoo_query_count 做特殊处理: 根据 model 给具体提示
    if tool_name in ("odoo_query", "odoo_query_count") and tool_input:
        model = tool_input.get("model", "")
        if model in ODOO_MODEL_LABELS:
            zh_label, en_label = ODOO_MODEL_LABELS[model]
            return zh_label if lang == "zh" else en_label

    table = TOOL_PROGRESS_LABELS_ZH if lang == "zh" else TOOL_PROGRESS_LABELS_EN
    if lang == "zh":
        return table.get(tool_name, f"⚙️ 正在执行 {tool_name}...")
    return table.get(tool_name, f"⚙️ Running {tool_name}...")


async def _odoo_bot_post_progress(channel_id: int, text: str):
    """Post a progress update message to Odoo Discuss channel as the ChumartAI bot.
    Called between tool iterations so user sees what the bot is doing in real time."""
    if not channel_id:
        return
    try:
        cookies = await odoo_get_session()
        # Find the bot partner
        bot_partner_r = await _odoo_call("res.partner", "search_read",
            [[["name", "=ilike", "ChumartAI"]]],
            {"fields": ["id"], "limit": 1},
            cookies=cookies)
        if not bot_partner_r:
            return
        bot_partner_id = bot_partner_r[0]["id"]

        # Post the progress message as the bot
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {
                    "model": "discuss.channel",
                    "method": "message_post",
                    "args": [[channel_id]],
                    "kwargs": {
                        "body": f"<i>{text}</i>",
                        "message_type": "comment",
                        "subtype_xmlid": "mail.mt_comment",
                        "author_id": bot_partner_id,
                    }
                }
            }, cookies=cookies)
    except Exception as e:
        print(f"[ODOO-BOT] post_progress error: {e}")


async def _odoo_call(model: str, method: str, args: list, kwargs: dict, cookies=None):
    """Generic Odoo RPC call helper."""
    if not cookies:
        cookies = await odoo_get_session()
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
        r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {"model": model, "method": method, "args": args, "kwargs": kwargs}
        }, cookies=cookies)
        d = r.json()
        return d.get("result")
ODOO_BOT_MAX_HISTORY = 20    # messages (10 turns)

@app.post("/odoo-bot/chat")
async def odoo_bot_chat(req: OdooBotRequest):
    """Endpoint for Odoo Discuss Bot module.
    Odoo custom module POSTs here when a user messages the bot.
    Returns the AI reply as JSON {reply: "..."}.
    """
    # Authenticate: shared secret between Odoo module and Railway
    expected_secret = os.getenv("ODOO_BOT_SECRET", "")
    if expected_secret and req.bot_secret != expected_secret:
        return {"error": "Invalid bot_secret"}

    if not req.message.strip():
        return {"reply": "请输入您的问题。"}

    uid = req.uid
    author = req.author_name or f"User#{uid}"
    print(f"[ODOO-BOT] uid={uid} name={author} msg={req.message[:100]}")

    # Get user role from Odoo groups
    role = await get_user_role(uid)
    perms = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["guest"])
    print(f"[ODOO-BOT] uid={uid} role={role}")

    # Filter tools by permission (same logic as /chat)
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax", "odoo_match_payment_to_customer"}
    write_tools = {"odoo_create_record", "odoo_add_order_line", "odoo_confirm_order", "odoo_update_record", "odoo_update_vendor_price", "odoo_create_invoice_from_so", "odoo_register_payment", "odoo_export_invoice_pdf", "release_so"}
    cost_tools = {"odoo_find_recent_purchases_by_skus", "odoo_get_product_vendors", "odoo_create_bulk_po", "get_po_with_so_links", "odoo_restock_analysis"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms.get("can_see_finance"):
            continue
        if tname in write_tools and not perms.get("can_write_odoo"):
            continue
        if tname in cost_tools and not perms.get("can_see_cost"):
            continue
        allowed_tools.append(tool)

    # Build conversation history
    if uid not in ODOO_BOT_HISTORY:
        ODOO_BOT_HISTORY[uid] = []
    conv = ODOO_BOT_HISTORY[uid]
    conv.append({"role": "user", "content": req.message})
    # Trim to last N messages
    if len(conv) > ODOO_BOT_MAX_HISTORY:
        conv = conv[-ODOO_BOT_MAX_HISTORY:]
        ODOO_BOT_HISTORY[uid] = conv

    # Load user memory
    memories = await db_get_memory(uid)

    # Build system prompt — add Odoo Bot-specific rules
    system_prompt = get_system_prompt(role, author, uid, False, memories)
    system_prompt += """

ODOO DISCUSS BOT RULES (you are responding inside Odoo Discuss chat, NOT the web frontend):
- Keep replies SHORT and concise — Discuss chat window is narrow, long replies are hard to read
- Do NOT mention "前端会自动生成下载按钮" — there is no frontend here, only Discuss chat
- For Excel/PDF downloads, give the FULL URL directly: https://chumart-ai.up.railway.app/export/commission?year=YYYY&month=MM
- For document downloads, give the FULL URL: https://chumart-ai.up.railway.app/docs/signed-url/{doc_id}
- Do NOT use Markdown tables (they render poorly in Discuss) — use compact card-style or bullet lists instead
- Use simple formatting: **bold** for emphasis, bullet points (•) for lists
- Maximum reply length: ~500 words. If data is larger, give a summary and offer the download link.
"""

    # Call Claude — Sonnet for admin/finance (better reasoning for reminders/invoicing),
    # Haiku for others (faster, cheaper).
    tool_context = {"uid": uid, "username": author, "role": role}
    headers = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}
    bot_model = "claude-sonnet-4-5" if role in ("admin", "finance") else "claude-haiku-4-5-20251001"
    print(f"[ODOO-BOT] uid={uid} role={role} model={bot_model}")

    try:
        async with httpx.AsyncClient(timeout=300) as c:
            current_messages = list(conv)
            for _ in range(8):  # max tool iterations
                r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                    "model": bot_model,
                    "max_tokens": 2048,
                    "system": system_prompt,
                    "tools": allowed_tools,
                    "messages": current_messages
                })
                d = r.json()
                if "error" in d:
                    print(f"[ODOO-BOT] API error: {d['error']}")
                    return {"reply": f"AI 错误: {d['error'].get('message', str(d['error']))}"}

                if d.get("stop_reason") == "tool_use":
                    tool_results = []
                    for block in d.get("content", []):
                        if block.get("type") == "tool_use":
                            tool_name = block["name"]
                            tool_input = block.get("input", {})
                            print(f"[ODOO-BOT] tool: {tool_name}")
                            # 发个进度消息让用户知道 bot 在做什么 (语言根据用户输入自动判断)
                            user_lang = _detect_user_language(req.message)
                            progress_label = _get_tool_progress_label(tool_name, user_lang, tool_input)
                            await _odoo_bot_post_progress(req.channel_id, progress_label)
                            result = await run_tool(tool_name, tool_input, context=tool_context)
                            tool_results.append({"type": "tool_result", "tool_use_id": block["id"], "content": result})
                    current_messages.append({"role": "assistant", "content": d["content"]})
                    current_messages.append({"role": "user", "content": tool_results})
                else:
                    break

            reply = "".join(b.get("text", "") for b in d.get("content", []) if b.get("type") == "text")
    except Exception as e:
        print(f"[ODOO-BOT] exception: {e}")
        reply = f"AI 请求失败: {str(e)}"

    if not reply:
        reply = "抱歉，没有生成回复。"

    # Save to history
    conv.append({"role": "assistant", "content": reply})
    if len(conv) > ODOO_BOT_MAX_HISTORY:
        ODOO_BOT_HISTORY[uid] = conv[-ODOO_BOT_MAX_HISTORY:]

    print(f"[ODOO-BOT] reply to uid={uid}: {reply[:100]}...")
    return {"reply": reply}

@app.post("/odoo-bot/reset")
async def odoo_bot_reset(uid: int = 0, bot_secret: str = ""):
    """Reset conversation history for an Odoo bot user."""
    expected_secret = os.getenv("ODOO_BOT_SECRET", "")
    if expected_secret and bot_secret != expected_secret:
        return {"error": "Invalid bot_secret"}
    if uid in ODOO_BOT_HISTORY:
        del ODOO_BOT_HISTORY[uid]
    return {"status": "reset", "uid": uid}



# ─────────────────────────────────────────────
# Payment Channels — Stripe / Square / Zelle
# ─────────────────────────────────────────────

async def _match_so_by_amount_and_customer(
    amount: float,
    customer_hint: str = "",
    reference: str = "",
    cookies=None,
    allow_partial: bool = False,
) -> dict | None:
    """
    根据金额 + 客户名/reference 从 Odoo 找到匹配的 SO。
    allow_partial=True 时，也匹配 SO 金额大于付款金额的（部分付款场景）。
    同时检查 pending_payments 表，看是否有已记录的部分付款。
    """
    if not cookies:
        cookies = await odoo_get_session()

    # 1) 按 SO name 直接查
    if reference:
        so_r = json.loads(await odoo_query("sale.order",
            [["name", "=", reference], ["company_id", "=", 1],
             ["state", "in", ["sale", "done"]]],
            ["id", "name", "partner_id", "amount_total", "invoice_status", "invoice_ids"],
            limit=1, cookies=cookies))
        if isinstance(so_r, list) and so_r:
            return so_r[0]

    # 2) 精确金额匹配（±0.01 容差），未完全开票
    domain = [
        ["company_id", "=", 1],
        ["state", "in", ["sale", "done"]],
        ["invoice_status", "!=", "invoiced"],
        ["amount_total", ">=", amount - 0.01],
        ["amount_total", "<=", amount + 0.01],
    ]
    so_list = json.loads(await odoo_query("sale.order", domain,
        ["id", "name", "partner_id", "amount_total", "invoice_status", "invoice_ids"],
        limit=20, order="id desc", cookies=cookies))

    if isinstance(so_list, list) and so_list:
        if len(so_list) == 1:
            return so_list[0]
        # 多个结果 → 用 customer_hint 模糊匹配
        if customer_hint:
            hint_lower = customer_hint.lower()
            for so in so_list:
                partner_name = (so.get("partner_id") or [0, ""])[1].lower()
                if hint_lower in partner_name or partner_name in hint_lower:
                    return so
        return so_list[0]

    # 3) 如果精确匹配没结果 + 允许部分付款 → 查金额更大的 SO
    if allow_partial and amount > 0:
        # 先看 pending_payments 里有没有已记录的部分付款（同一个 SO 在等后续款项）
        conn = await get_db_conn()
        if conn:
            try:
                pending_rows = await conn.fetch("""
                    SELECT so_name, so_id, so_amount, MAX(created_at) AS last_created
                    FROM pending_payments
                    WHERE status = 'pending'
                    GROUP BY so_name, so_id, so_amount
                    ORDER BY last_created DESC LIMIT 20
                """)
                for row in pending_rows:
                    # 检查 pending 里的 SO 是否还有剩余
                    pending_total = await conn.fetchval("""
                        SELECT COALESCE(SUM(amount), 0) FROM pending_payments
                        WHERE so_name = $1 AND status = 'pending'
                    """, row["so_name"])
                    remaining = float(row["so_amount"]) - float(pending_total)
                    # 如果这笔付款接近剩余金额，很可能就是给这个 SO 的
                    if abs(remaining - amount) < 0.01 or (remaining > 0 and amount <= remaining + 0.01):
                        so_r = json.loads(await odoo_query("sale.order",
                            [["name", "=", row["so_name"]], ["company_id", "=", 1]],
                            ["id", "name", "partner_id", "amount_total", "invoice_status", "invoice_ids"],
                            limit=1, cookies=cookies))
                        if isinstance(so_r, list) and so_r:
                            print(f"[COMBO] Matched partial payment ${amount} to pending SO {row['so_name']} (remaining=${remaining:.2f})")
                            return so_r[0]
            finally:
                await conn.close()

        # 搜索金额更大的 SO（可能是第一笔部分付款）
        domain_partial = [
            ["company_id", "=", 1],
            ["state", "in", ["sale", "done"]],
            ["invoice_status", "!=", "invoiced"],
            ["amount_total", ">", amount + 0.01],
        ]
        so_partial = json.loads(await odoo_query("sale.order", domain_partial,
            ["id", "name", "partner_id", "amount_total", "invoice_status", "invoice_ids"],
            limit=10, order="id desc", cookies=cookies))

        if isinstance(so_partial, list) and so_partial and customer_hint:
            hint_lower = customer_hint.lower()
            for so in so_partial:
                partner_name = (so.get("partner_id") or [0, ""])[1].lower()
                if hint_lower in partner_name or partner_name in hint_lower:
                    print(f"[COMBO] Matched partial payment ${amount} to SO {so['name']} (total=${so['amount_total']})")
                    return so

    return None


async def _record_partial_payment(
    so_name: str,
    so_id: int,
    so_amount: float,
    channel: str,
    amount: float,
    reference: str = "",
) -> dict:
    """
    记录一笔部分付款到 pending_payments 表。
    检查累计金额：如果 >= SO 总额，返回 {"ready": True, payments: [...]}
    否则返回 {"ready": False, "accumulated": X, "remaining": Y}
    """
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not available"}
    try:
        # 先检查是否有重复（同 channel + 相近金额 + 5分钟内）
        dup = await conn.fetchrow("""
            SELECT id FROM pending_payments
            WHERE so_name = $1 AND channel = $2
              AND amount = $3 AND status = 'pending'
              AND created_at > NOW() - INTERVAL '5 minutes'
        """, so_name, channel, amount)
        if dup:
            print(f"[COMBO] Duplicate payment detected: {so_name} {channel} ${amount}")
            # 即使重复，也检查累计
        else:
            await conn.execute("""
                INSERT INTO pending_payments (so_name, so_id, so_amount, channel, amount, reference)
                VALUES ($1, $2, $3, $4, $5, $6)
            """, so_name, so_id, so_amount, channel, amount, reference)

        # 查累计
        rows = await conn.fetch("""
            SELECT id, channel, amount, reference FROM pending_payments
            WHERE so_name = $1 AND status = 'pending'
            ORDER BY created_at
        """, so_name)

        accumulated = sum(float(r["amount"]) for r in rows)
        remaining = so_amount - accumulated

        print(f"[COMBO] SO {so_name}: accumulated=${accumulated:.2f} / total=${so_amount:.2f}, remaining=${remaining:.2f}")

        if remaining <= 0.01:  # 容差 1 分钱
            payments = [{"channel": r["channel"], "amount": float(r["amount"]), "reference": r["reference"]} for r in rows]
            return {"ready": True, "accumulated": accumulated, "payments": payments}
        else:
            return {"ready": False, "accumulated": accumulated, "remaining": remaining, "so_amount": so_amount}
    finally:
        await conn.close()


async def _mark_payments_done(so_name: str):
    """把 SO 的所有 pending payments 标记为 done"""
    conn = await get_db_conn()
    if not conn:
        return
    try:
        await conn.execute("""
            UPDATE pending_payments SET status = 'done'
            WHERE so_name = $1 AND status = 'pending'
        """, so_name)
    finally:
        await conn.close()


def _build_combo_payment_method(payments: list) -> str:
    """
    根据多笔部分付款构建 payment_method 字符串。
    单笔: "Stripe"
    多笔: "Combo(Stripe+Zelle)"
    """
    channels = list(dict.fromkeys(p["channel"] for p in payments))  # 保持顺序去重
    if len(channels) == 1:
        return channels[0]
    return f"Combo({'+'.join(channels)})"


async def _record_received_payment(
    so_name: str,
    channel: str,
    amount: float,
    external_ref: str = "",
    customer_name: str = "",
) -> dict:
    """
    把一笔已 capture / 已收到的付款写入 received_payments 队列。
    不开票！等用户 release。
    external_ref 用于去重 (Stripe pi_id, Square payment_id, Gmail msg_id)。
    """
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not available"}
    try:
        # 用 external_ref 去重
        if external_ref:
            existing = await conn.fetchrow("""
                SELECT id, status FROM received_payments
                WHERE channel = $1 AND external_ref = $2
            """, channel, external_ref)
            if existing:
                return {
                    "duplicate": True,
                    "existing_id": existing["id"],
                    "status": existing["status"],
                    "message": f"Payment {external_ref} already recorded (status={existing['status']})",
                }

        row = await conn.fetchrow("""
            INSERT INTO received_payments (so_name, channel, amount, external_ref, customer_name)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        """, so_name, channel, amount, external_ref, customer_name)
        return {
            "success": True,
            "id": row["id"],
            "so_name": so_name,
            "channel": channel,
            "amount": amount,
        }
    finally:
        await conn.close()


async def _get_received_payments_for_so(so_name: str, status: str = "received") -> list:
    """查某个 SO 的已收款记录"""
    conn = await get_db_conn()
    if not conn:
        return []
    try:
        rows = await conn.fetch("""
            SELECT id, channel, amount, external_ref, customer_name, status, created_at
            FROM received_payments
            WHERE so_name = $1 AND status = $2
            ORDER BY created_at
        """, so_name, status)
        return [{
            "id": r["id"],
            "channel": r["channel"],
            "amount": float(r["amount"]),
            "external_ref": r["external_ref"],
            "customer_name": r["customer_name"],
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        } for r in rows]
    finally:
        await conn.close()


async def _mark_received_payments_released(so_name: str, invoice_name: str):
    """把 SO 的所有 received payments 标记为 released"""
    conn = await get_db_conn()
    if not conn:
        return
    try:
        await conn.execute("""
            UPDATE received_payments
            SET status = 'released', released_at = NOW(), invoice_name = $2
            WHERE so_name = $1 AND status = 'received'
        """, so_name, invoice_name)
    finally:
        await conn.close()


async def _reconcile_existing_payments(invoice_id: int, so_amount: float, partner_name: str = "") -> dict:
    """
    把 invoice 与已存在的 unreconciled account.payment 关联起来。
    
    场景: Stripe/Square/Zelle 付款时 Odoo 已经自动创建了 account.payment 记录 (state=posted, 
    is_reconciled=False)。我们创建 invoice 后需要手动 reconcile 这些 payment。
    
    步骤:
    1. 找 invoice 对应的 partner
    2. 找该 partner 名下所有 state=posted, is_reconciled=False 的 account.payment
    3. 按金额匹配，调用 invoice 的 js_assign_outstanding_line 把 payment 关联进来
    """
    try:
        cookies = await odoo_get_session()

        # 1) 获取 invoice 信息
        inv_r = json.loads(await odoo_query("account.move",
            [["id", "=", invoice_id]],
            ["id", "name", "partner_id", "amount_residual", "state", "line_ids"],
            limit=1, cookies=cookies))
        if not isinstance(inv_r, list) or not inv_r:
            return {"error": f"Invoice {invoice_id} not found"}
        inv = inv_r[0]
        partner_id = inv.get("partner_id", [0, ""])[0] if inv.get("partner_id") else 0
        if not partner_id:
            return {"error": "Invoice has no partner"}

        residual = float(inv.get("amount_residual") or 0)
        if residual <= 0.01:
            return {"already_reconciled": True, "message": "Invoice has no outstanding amount"}

        # 2) 找该 partner 名下未对账的 payment
        # 注意: account.payment 中的 reconciled_invoices_count 字段或 is_matched 可用
        payments_r = json.loads(await odoo_query("account.payment",
            [
                ["partner_id", "=", partner_id],
                ["state", "in", ["posted", "in_process"]],
                ["payment_type", "=", "inbound"],
                ["is_reconciled", "=", False],
            ],
            ["id", "name", "amount", "date", "journal_id", "is_reconciled", "move_id"],
            limit=20, order="date desc", cookies=cookies))

        if not isinstance(payments_r, list) or not payments_r:
            return {
                "no_unreconciled_payments": True,
                "message": f"No unreconciled payments found for partner. User may need to manually reconcile in Odoo.",
            }

        # 3) 按金额匹配（先精确，后部分）
        # 收集需要 reconcile 的 payment IDs
        target_amount = residual
        payments_to_reconcile = []
        accumulated = 0.0

        # 先尝试找一笔精确匹配的
        exact = [p for p in payments_r if abs(float(p["amount"]) - target_amount) < 0.01]
        if exact:
            payments_to_reconcile = [exact[0]]
            accumulated = float(exact[0]["amount"])
        else:
            # 凑金额（可能是 combo 付款）
            for p in payments_r:
                if accumulated >= target_amount - 0.01:
                    break
                payments_to_reconcile.append(p)
                accumulated += float(p["amount"])

        if not payments_to_reconcile or accumulated < target_amount - 0.01:
            return {
                "insufficient_payments": True,
                "found_payments": payments_r,
                "message": f"Found ${accumulated:.2f} unreconciled but invoice needs ${target_amount:.2f}",
            }

        # 4) 调用 Odoo 的 reconciliation API
        # 方法: 找到 payment 对应的 move_id 中的 receivable line，然后调用 invoice 的 js_assign_outstanding_line
        reconciled_ids = []
        for p in payments_to_reconcile:
            move_id = p.get("move_id", [0])[0] if p.get("move_id") else 0
            if not move_id:
                continue

            # 找 move 中的 receivable line (account_type = 'asset_receivable')
            move_lines_r = json.loads(await odoo_query("account.move.line",
                [
                    ["move_id", "=", move_id],
                    ["account_type", "=", "asset_receivable"],
                    ["reconciled", "=", False],
                ],
                ["id", "balance"],
                limit=5, cookies=cookies))

            if not isinstance(move_lines_r, list) or not move_lines_r:
                continue

            # 调用 invoice.js_assign_outstanding_line 来 reconcile
            for ml in move_lines_r:
                async with httpx.AsyncClient(timeout=30, follow_redirects=True) as c:
                    r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                        "jsonrpc": "2.0", "method": "call", "id": 100,
                        "params": {
                            "model": "account.move",
                            "method": "js_assign_outstanding_line",
                            "args": [[invoice_id], ml["id"]],
                            "kwargs": {}
                        }
                    }, cookies=cookies)
                    rdata = r.json()
                    if rdata.get("error"):
                        print(f"[RECONCILE] Failed to assign payment line {ml['id']}: {rdata['error']}")
                        continue
                    reconciled_ids.append(p["id"])
                    print(f"[RECONCILE] Linked payment {p.get('name')} (${p['amount']}) to invoice {inv.get('name')}")
                    break

        # 5) 验证 invoice 现在的 payment_state
        inv_after = json.loads(await odoo_query("account.move",
            [["id", "=", invoice_id]],
            ["id", "name", "payment_state", "amount_residual"],
            limit=1, cookies=cookies))
        inv_final = inv_after[0] if isinstance(inv_after, list) and inv_after else {}

        return {
            "success": True,
            "invoice_id": invoice_id,
            "reconciled_payment_ids": reconciled_ids,
            "reconciled_count": len(reconciled_ids),
            "payment_state": inv_final.get("payment_state"),
            "amount_residual": inv_final.get("amount_residual", 0),
            "message": f"Reconciled {len(reconciled_ids)} payment(s) with invoice {inv.get('name')}, payment_state now: {inv_final.get('payment_state')}",
        }

    except Exception as e:
        print(f"[RECONCILE] error: {e}")
        import traceback; traceback.print_exc()
        return {"error": f"Reconciliation failed: {str(e)}"}


async def _auto_invoice_pipeline(
    so_name: str,
    payment_method: str,
    journal_name: str,
    skip_register: bool = False,
    source: str = "webhook",
    amount: float = None,
    so_id: int = None,
    so_amount: float = None,
    is_combo_ready: bool = False,
    combo_payments: list = None,
) -> dict:
    """
    统一的自动开票流水线。
    
    两种模式:
    1. 单笔全额付款 → 直接开票 (is_combo_ready=False, amount >= so_amount)
    2. 部分付款 → 先记录到 pending_payments，凑齐后才开票
    
    当 is_combo_ready=True 时，说明已经凑齐，combo_payments 包含所有部分付款明细。
    """
    print(f"[{source.upper()}] Auto-invoice pipeline: SO={so_name}, method={payment_method}, "
          f"amount=${amount}, so_amount=${so_amount}, combo_ready={is_combo_ready}")

    # --- 如果没有明确说已凑齐，检查是否需要走 combo 逻辑 ---
    if not is_combo_ready and so_amount and amount:
        # 判断：这笔付款是否覆盖全部金额？（容差 1 分钱）
        if amount < so_amount - 0.01:
            # 部分付款 → 记录并检查累计
            record_result = await _record_partial_payment(
                so_name=so_name,
                so_id=so_id or 0,
                so_amount=so_amount,
                channel=payment_method,
                amount=amount,
                reference=f"{source}:{datetime.datetime.now(datetime.timezone.utc).isoformat()}",
            )
            if record_result.get("error"):
                return record_result

            if not record_result.get("ready"):
                # 还没凑齐，不开票
                return {
                    "status": "partial_recorded",
                    "so_name": so_name,
                    "channel": payment_method,
                    "this_payment": amount,
                    "accumulated": record_result["accumulated"],
                    "remaining": record_result["remaining"],
                    "so_amount": record_result["so_amount"],
                    "message": f"Partial payment ${amount:,.2f} recorded for {so_name}. "
                               f"Accumulated: ${record_result['accumulated']:,.2f} / ${record_result['so_amount']:,.2f}. "
                               f"Remaining: ${record_result['remaining']:,.2f}",
                }
            else:
                # 凑齐了！切换到 combo 模式
                is_combo_ready = True
                combo_payments = record_result["payments"]
                payment_method = _build_combo_payment_method(combo_payments)
                print(f"[{source.upper()}] Combo payment complete! method={payment_method}")

    ctx = {"uid": 0, "username": f"auto-{source}", "role": "admin"}

    # Step 1: 创建 Invoice
    result1 = await run_tool("odoo_create_invoice_from_so", {
        "so_name": so_name,
        "payment_method": payment_method,
    }, context=ctx)
    r1 = json.loads(result1) if isinstance(result1, str) else result1

    if r1.get("already_invoiced"):
        invoice_id = r1["invoice_id"]
        print(f"[{source.upper()}] SO {so_name} already invoiced: {r1.get('invoice_name')}")
    elif r1.get("success"):
        invoice_id = r1["invoice_id"]
    else:
        return {"error": f"Create invoice failed: {r1.get('error', 'unknown')}"}

    # Step 2: Register Payment
    # Stripe 单独付全款时跳过（Odoo 自动处理）
    # Combo 模式: 统一走 Revenue and COGS
    if is_combo_ready:
        # Combo: 用总金额 register 一次（Odoo 端用 Revenue and COGS）
        skip_register = False
        journal_name = "Revenue and COGS"

    if not skip_register:
        result2 = await run_tool("odoo_register_payment", {
            "invoice_id": invoice_id,
            "journal_name": journal_name,
            "amount": amount if not is_combo_ready else None,  # combo 模式用发票全额
        }, context=ctx)
        r2 = json.loads(result2) if isinstance(result2, str) else result2
        if not r2.get("success") and not r2.get("already_paid"):
            return {"error": f"Register payment failed: {r2.get('error', 'unknown')}"}

    # Step 3: Export PDF
    result3 = await run_tool("odoo_export_invoice_pdf", {
        "invoice_id": invoice_id,
    }, context=ctx)
    r3 = json.loads(result3) if isinstance(result3, str) else result3

    # 标记 pending payments 为 done
    if is_combo_ready:
        await _mark_payments_done(so_name)

    return {
        "success": True,
        "so_name": so_name,
        "invoice_id": invoice_id,
        "invoice_name": r1.get("invoice_name", ""),
        "payment_method": payment_method,
        "pdf_url": r3.get("download_url", ""),
        "source": source,
        "combo": is_combo_ready,
        "combo_payments": combo_payments if is_combo_ready else None,
    }


# ---- Stripe Webhook ----

# Notification recipients for payment alerts
PAYMENT_ALERT_EMAILS = ["di@chumartusa.com", "ashley@chumartusa.com"]

async def _notify_stripe_duplicate(so_name: str, amount: float, keep_pi_id: str, duplicate_pi_ids: list):
    """通知管理员 Stripe 检测到同一 SO 的重复付款"""
    dup_lines = "\n".join(
        f"  - {pid}  →  https://dashboard.stripe.com/payments/{pid}"
        for pid in duplicate_pi_ids
    )
    subject = f"⚠️ Stripe 重复付款 — {so_name} (${amount:,.2f})"
    body = (
        f"SO {so_name} 在 Stripe 存在多笔相同金额的待 capture 付款。\n"
        f"AI 已自动 capture 第一笔，其余已拦截。\n\n"
        f"✅ 已 capture:\n"
        f"  - {keep_pi_id}  →  https://dashboard.stripe.com/payments/{keep_pi_id}\n\n"
        f"❌ 已拦截（需要人工 cancel）:\n{dup_lines}\n\n"
        f"请登录 Stripe Dashboard 取消多余的付款。\n"
        f"7 天内未 capture 的付款会自动释放回客户。"
    )
    for email in PAYMENT_ALERT_EMAILS:
        ok, err = await _send_email(email, subject, body)
        if ok:
            print(f"[STRIPE] Duplicate alert sent to {email}")
        else:
            print(f"[STRIPE] Failed to send alert to {email}: {err}")


@app.post("/stripe/webhook")
async def stripe_webhook(request: Request):
    """
    Stripe webhook — Odoo 发付款链接 → 客户付款 → requires_capture。
    
    核心逻辑（capture 前检测重复）:
    1. 从 description 读 SO 编号
    2. 调 Stripe API 搜索同一 SO 的所有 requires_capture 的 PaymentIntent
    3. 如果 > 1 个 → 只 capture 第一个，其余拦截 + 通知管理员
    4. capture 成功 → 自动开票
    """
    import stripe
    stripe.api_key = STRIPE_SECRET_KEY

    payload = await request.body()
    sig_header = request.headers.get("stripe-signature", "")

    # 验证签名（但不用返回的 StripeObject，直接 parse raw payload 为 dict）
    try:
        stripe.Webhook.construct_event(payload, sig_header, STRIPE_WEBHOOK_SECRET)
    except ValueError:
        print("[STRIPE] Invalid payload")
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Invalid payload"}, status_code=400)
    except stripe.error.SignatureVerificationError:
        print("[STRIPE] Invalid signature")
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Invalid signature"}, status_code=400)

    # 直接 parse raw JSON → 普通 dict，避免 StripeObject 折腾
    try:
        event = json.loads(payload)
    except Exception as e:
        print(f"[STRIPE] JSON parse failed: {e}")
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "JSON parse failed"}, status_code=400)

    event_type = event.get("type", "")
    print(f"[STRIPE] Event: {event_type}")

    if event_type in ("payment_intent.amount_capturable_updated", "payment_intent.requires_capture"):
        pi = event.get("data", {}).get("object", {})  # 已经是普通 dict
        pi_id = pi.get("id", "")
        amount_capturable = pi.get("amount_capturable", 0) or 0
        amount = (pi.get("amount", 0) or 0) / 100.0

        # 只在有钱可 capture 时处理（amount_capturable > 0）
        if amount_capturable == 0:
            print(f"[STRIPE] amount_capturable=0 for {pi_id}, skipping (already captured or cancelled)")
            return {"status": "skipped_no_capturable_amount"}

        # --- 从 Odoo 生成的 PaymentIntent 提取信息 ---
        so_ref = (pi.get("description") or "").strip()
        charges_data = pi.get("charges") or {}
        charges_list = charges_data.get("data", []) if isinstance(charges_data, dict) else []
        first_charge = charges_list[0] if charges_list else {}
        billing = first_charge.get("billing_details", {}) if isinstance(first_charge, dict) else {}
        customer_name = (billing.get("name") or "").strip() if isinstance(billing, dict) else ""
        if not customer_name:
            shipping = pi.get("shipping") or {}
            customer_name = (shipping.get("name") or "").strip() if isinstance(shipping, dict) else ""
        customer_email = pi.get("receipt_email", "") or ""

        print(f"[STRIPE] requires_capture: pi={pi_id}, ${amount}, SO={so_ref}, customer={customer_name}")

        try:
            # ============================================
            # Webhook 只做一件事: 记录 PI 到队列。
            # 不 capture, 不开票, 不检查重复 — 全部在 release_so 时做。
            # ============================================
            recorded = await _record_received_payment(
                so_name=so_ref,
                channel="Stripe",
                amount=amount,
                external_ref=pi_id,
                customer_name=customer_name or "",
            )
            print(f"[STRIPE] Recorded to queue: {recorded}")
            return {
                "status": "recorded",
                "so_name": so_ref,
                "pi_id": pi_id,
                "amount": amount,
                "message": f"PaymentIntent {pi_id} recorded for {so_ref or '<no SO>'}, awaiting user release.",
            }

        except Exception as e:
            print(f"[STRIPE] Error: {e}")
            import traceback; traceback.print_exc()
            from fastapi.responses import JSONResponse
            return JSONResponse({"error": str(e)}, status_code=500)

    return {"status": "ok"}


# ---- Square Webhook ----

def _verify_square_signature(body: bytes, signature: str, signature_key: str, notification_url: str) -> bool:
    combined = notification_url.encode("utf-8") + body
    expected = hmac.new(
        signature_key.encode("utf-8"),
        combined,
        hashlib.sha256
    ).digest()
    expected_b64 = base64.b64encode(expected).decode("utf-8")
    return hmac.compare_digest(expected_b64, signature)


@app.post("/square/webhook")
async def square_webhook(request: Request):
    """Square webhook: payment.completed → match SO → invoice → register payment → PDF"""
    body = await request.body()
    signature = request.headers.get("x-square-hmacsha256-signature", "")

    notification_url = str(request.url)
    if SQUARE_WEBHOOK_SIGNATURE_KEY and not _verify_square_signature(
        body, signature, SQUARE_WEBHOOK_SIGNATURE_KEY, notification_url
    ):
        print("[SQUARE] Invalid signature")
        from fastapi.responses import JSONResponse
        return JSONResponse({"error": "Invalid signature"}, status_code=400)

    event = json.loads(body)
    event_type = event.get("type", "")
    print(f"[SQUARE] Event: {event_type}")

    if event_type == "payment.completed":
        payment = event.get("data", {}).get("object", {}).get("payment", {})
        amount = payment.get("total_money", {}).get("amount", 0) / 100.0
        square_payment_id = payment.get("id", "")
        note = payment.get("note", "")
        customer_id = payment.get("customer_id", "")

        print(f"[SQUARE] Payment: id={square_payment_id}, ${amount}, note={note}")

        so_ref = ""
        if note:
            m = re.search(r'S\d{5,}', note, re.IGNORECASE)
            if m:
                so_ref = m.group(0)

        try:
            customer_name = ""
            if customer_id and SQUARE_ACCESS_TOKEN:
                try:
                    from square.client import Client as SquareClient
                    sq_client = SquareClient(access_token=SQUARE_ACCESS_TOKEN, environment=SQUARE_ENVIRONMENT)
                    cust_r = sq_client.customers.retrieve_customer(customer_id=customer_id)
                    if cust_r.is_success():
                        cust = cust_r.body.get("customer", {})
                        customer_name = f"{cust.get('given_name', '')} {cust.get('family_name', '')}".strip()
                except Exception as e:
                    print(f"[SQUARE] Customer lookup failed: {e}")

            cookies = await odoo_get_session()
            so_name_for_record = so_ref or ""
            if not so_name_for_record:
                so = await _match_so_by_amount_and_customer(
                    amount=amount,
                    customer_hint=customer_name,
                    reference="",
                    cookies=cookies,
                    allow_partial=False,
                )
                if so:
                    so_name_for_record = so["name"]

            recorded = await _record_received_payment(
                so_name=so_name_for_record,
                channel="POS Machine",
                amount=amount,
                external_ref=square_payment_id,
                customer_name=customer_name or "",
            )
            print(f"[SQUARE] Recorded payment: {recorded}")
            return {
                "status": "captured_and_recorded",
                "so_name": so_name_for_record,
                "amount": amount,
                "channel": "POS Machine",
                "message": f"Payment ${amount:,.2f} recorded for {so_name_for_record or '<unmatched>'}. "
                           f"Waiting for user to release via AI.",
            }

        except Exception as e:
            print(f"[SQUARE] Error: {e}")
            import traceback; traceback.print_exc()
            from fastapi.responses import JSONResponse
            return JSONResponse({"error": str(e)}, status_code=500)

    return {"status": "ok"}


# ---- Zelle Gmail Monitor ----

async def _check_zelle_emails():
    """每 90 秒检查 Gmail 的 Zelle 收款通知 → 匹配 SO → 自动开票"""
    if not GMAIL_CREDENTIALS_JSON or not GMAIL_USER_EMAIL:
        return

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
        import base64 as b64

        creds_dict = json.loads(GMAIL_CREDENTIALS_JSON)

        if creds_dict.get("type") == "service_account":
            creds = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=["https://www.googleapis.com/auth/gmail.readonly",
                        "https://www.googleapis.com/auth/gmail.modify"],
            )
            creds = creds.with_subject(GMAIL_USER_EMAIL)
        else:
            from google.oauth2.credentials import Credentials
            creds = Credentials.from_authorized_user_info(creds_dict)

        service = build("gmail", "v1", credentials=creds)

        query_parts = ["is:unread"]
        if ZELLE_BANK_SENDER:
            query_parts.append(f"from:{ZELLE_BANK_SENDER}")
        query_parts.append("(subject:Zelle OR subject:received)")
        query = " ".join(query_parts)

        results = service.users().messages().list(userId="me", q=query, maxResults=10).execute()
        messages = results.get("messages", [])
        if not messages:
            return

        print(f"[ZELLE] Found {len(messages)} unread Zelle notification(s)")

        for msg_meta in messages:
            msg_id = msg_meta["id"]
            msg = service.users().messages().get(userId="me", id=msg_id, format="full").execute()

            headers = {h["name"]: h["value"] for h in msg.get("payload", {}).get("headers", [])}
            subject = headers.get("Subject", "")

            body_text = ""
            payload = msg.get("payload", {})
            if payload.get("body", {}).get("data"):
                body_text = b64.urlsafe_b64decode(payload["body"]["data"]).decode("utf-8", errors="replace")
            elif payload.get("parts"):
                for part in payload["parts"]:
                    if part.get("mimeType") == "text/plain" and part.get("body", {}).get("data"):
                        body_text = b64.urlsafe_b64decode(part["body"]["data"]).decode("utf-8", errors="replace")
                        break

            combined = f"{subject} {body_text}"
            print(f"[ZELLE] Processing: {subject[:80]}")

            amount_match = re.search(r'\$?([\d,]+\.\d{2})', combined)
            if not amount_match:
                print(f"[ZELLE] Cannot parse amount from: {subject}")
                service.users().messages().modify(
                    userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
                ).execute()
                continue

            amount = float(amount_match.group(1).replace(",", ""))

            sender_match = re.search(r'from\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)', combined)
            sender_name = sender_match.group(1) if sender_match else ""

            print(f"[ZELLE] Parsed: ${amount} from '{sender_name}'")

            cookies = await odoo_get_session()
            so = await _match_so_by_amount_and_customer(
                amount=amount, customer_hint=sender_name, cookies=cookies,
                allow_partial=False,
            )

            so_name_for_record = so["name"] if so else ""
            recorded = await _record_received_payment(
                so_name=so_name_for_record,
                channel="Zelle",
                amount=amount,
                external_ref=msg_id,
                customer_name=sender_name,
            )
            print(f"[ZELLE] Recorded payment: {recorded}")

            service.users().messages().modify(
                userId="me", id=msg_id, body={"removeLabelIds": ["UNREAD"]}
            ).execute()

    except Exception as e:
        print(f"[ZELLE] Check error: {e}")
        import traceback; traceback.print_exc()


@app.get("/health")
async def health():
    conn = await get_db_conn()
    db_ok = conn is not None
    if conn: await conn.close()
    return {"status": "ok", "db": "connected" if db_ok else "disconnected"}

# ─────────────────────────────────────────────
# Auth — login with Odoo credentials + RBAC
# ─────────────────────────────────────────────

# Role permission matrix
ROLE_PERMISSIONS = {
    "admin": {
        "label": "Administrator",
        "can_see_finance":    True,
        "can_see_all_sales":  True,
        "can_see_cost":       True,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         True,
        "can_write_odoo":     True,
    },
    "finance": {
        "label": "Finance",
        "can_see_finance":    True,
        "can_see_all_sales":  True,
        "can_see_cost":       True,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         True,
        "can_write_odoo":     True,
    },
    "purchase": {
        "label": "Purchase",
        "can_see_finance":    False,
        "can_see_all_sales":  True,
        "can_see_cost":       True,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         True,
        "can_write_odoo":     True,
    },
    "sales": {
        "label": "Sales",
        "can_see_finance":    False,
        "can_see_all_sales":  False,   # only own orders
        "can_see_cost":       False,
        "can_see_inventory":  True,
        "can_see_products":   True,    # price yes, cost no
        "can_export":         False,
    },
    "warehouse": {
        "label": "Warehouse",
        "can_see_finance":    False,
        "can_see_all_sales":  False,
        "can_see_cost":       False,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         False,
    },
    "guest": {
        "label": "Guest",
        "can_see_finance":    False,
        "can_see_all_sales":  False,
        "can_see_cost":       False,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         False,
    },
}

# Odoo group XML IDs → role mapping (checked in priority order)
ODOO_GROUP_ROLE_MAP = [
    # Admin / Settings
    ("base.group_system",                  "admin"),
    ("base.group_erp_manager",             "admin"),
    # Finance / Accounting — only real accountants, NOT invoice users
    ("account.group_account_manager",      "finance"),
    ("account.group_account_user",         "finance"),
    # Sales — checked BEFORE account_invoice so salespeople aren't misclassified
    ("sales_team.group_sale_manager",      "sales"),
    ("sales_team.group_sale_salesman",     "sales"),
    ("base.group_sale_salesman",           "sales"),
    # Warehouse / Inventory
    ("stock.group_stock_manager",          "warehouse"),
    ("stock.group_stock_user",             "warehouse"),
    ("purchase.group_purchase_manager",    "warehouse"),
    # Invoice users — salespeople who can create invoices, treated as sales not finance
    ("account.group_account_invoice",      "sales"),
]

async def get_user_role(uid: int, cookies=None) -> str:
    """Query Odoo groups for a logged-in user and return their highest role.
    Uses the admin service account to ensure sufficient permissions to read group data."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            # Use admin session — user's own session may lack permission to read ir.model.data
            login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            admin_cookies = dict(login_r.cookies)

            # Get user's group IDs
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {
                    "model": "res.users", "method": "read",
                    "args": [[uid]],
                    "kwargs": {"fields": ["groups_id"]}
                }
            }, cookies=admin_cookies)
            data = r.json()
            result = data.get("result", [])
            if not result:
                print(f"ROLE DETECT uid={uid}: no user record found")
                return "guest"
            group_ids = result[0].get("groups_id", [])
            print(f"ROLE DETECT uid={uid}: {len(group_ids)} groups")

            if not group_ids:
                return "guest"

            # Get group XML IDs (raise limit — users can have 200+ groups)
            r2 = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 3,
                "params": {
                    "model": "ir.model.data", "method": "search_read",
                    "args": [[["model", "=", "res.groups"], ["res_id", "in", group_ids]]],
                    "kwargs": {"fields": ["module", "name", "res_id"], "limit": 500}
                }
            }, cookies=admin_cookies)
            data2 = r2.json()
            xml_ids = set()
            for rec in data2.get("result", []):
                xml_ids.add(f"{rec['module']}.{rec['name']}")

            print(f"ROLE DETECT uid={uid}: {len(xml_ids)} XML IDs resolved")

            # Match to role in priority order
            for xml_id, role in ODOO_GROUP_ROLE_MAP:
                if xml_id in xml_ids:
                    print(f"ROLE DETECT uid={uid}: matched '{xml_id}' → role={role}")
                    return role

            # Log what we found for debugging
            account_groups = [x for x in xml_ids if 'account' in x or 'sale' in x or 'stock' in x or 'purchase' in x]
            print(f"ROLE DETECT uid={uid}: no match found. Relevant groups: {account_groups}")
            return "guest"
    except Exception as e:
        print(f"Role detection error for uid={uid}: {e}")
        return "guest"


# In-memory Odoo session cache per user: uid -> {"cookies": dict, "time": datetime}
USER_ODOO_SESSIONS: dict = {}

class LoginRequest(BaseModel):
    username: str
    password: str
    client_type: str = "web"   # "web" (12h, in-memory) or "mobile" (30 days, DB-persisted)

@app.post("/auth/login")
async def login(req: LoginRequest):
    """Verify user against Odoo, detect role from groups, return permissions."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": req.username, "password": req.password}
            })
            data = r.json()
            result = data.get("result", {})

            if not result or not result.get("uid"):
                return {"success": False, "error": "Invalid username or password"}

            uid = result.get("uid")
            name = result.get("name", req.username)

            # Cache user's Odoo session for write operations
            USER_ODOO_SESSIONS[uid] = {
                "cookies": dict(r.cookies),
                "time": datetime.datetime.now()
            }

            # Detect role server-side (uses admin session internally)
            role = await get_user_role(uid)
            permissions = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["guest"])

            # Generate server-side session token
            session_token = str(uuid.uuid4())
            cleanup_caches()

            # 决定 TTL: mobile 30 天 (DB 持久化), web 12 小时 (内存)
            is_mobile = (req.client_type or "").lower() == "mobile"
            ttl_hours = 24 * 30 if is_mobile else SESSION_TTL_HOURS

            # 内存缓存所有 session (web 直接用,mobile 也用内存做加速)
            SESSION_STORE[session_token] = {
                "uid": uid,
                "username": req.username,
                "name": name,
                "role": role,
                "created_at": datetime.datetime.now(),
                "ttl_hours": ttl_hours,
            }

            # mobile 额外存到 DB (服务器重启后仍能找回)
            if is_mobile:
                try:
                    conn = await get_db_conn()
                    if conn:
                        try:
                            expires_at = datetime.datetime.now(UTC_TZ) + datetime.timedelta(hours=ttl_hours)
                            await conn.execute("""
                                CREATE TABLE IF NOT EXISTS user_sessions (
                                    token TEXT PRIMARY KEY,
                                    uid INTEGER NOT NULL,
                                    username TEXT,
                                    name TEXT,
                                    role TEXT,
                                    client_type TEXT,
                                    created_at TIMESTAMPTZ DEFAULT NOW(),
                                    expires_at TIMESTAMPTZ NOT NULL
                                )
                            """)
                            await conn.execute("""
                                INSERT INTO user_sessions (token, uid, username, name, role, client_type, expires_at)
                                VALUES ($1, $2, $3, $4, $5, $6, $7)
                                ON CONFLICT (token) DO UPDATE SET expires_at = EXCLUDED.expires_at
                            """, session_token, uid, req.username, name, role, "mobile", expires_at)
                        finally:
                            await conn.close()
                except Exception as e:
                    print(f"WARN: failed to persist mobile session to DB: {e}")

            print(f"LOGIN: uid={uid} name={name} role={role} client={req.client_type} ttl={ttl_hours}h token={session_token[:8]}...")

            return {
                "success":       True,
                "uid":           uid,
                "name":          name,
                "username":      req.username,
                "role":          role,
                "role_label":    permissions["label"],
                "permissions":   permissions,
                "session_token": session_token,
                "expires_in_hours": ttl_hours,
            }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ─────────────────────────────────────────────
# Excel Export
# ─────────────────────────────────────────────

@app.get("/export/commission")
async def export_commission(year: int, month: int, salesperson: str = ""):
    """Export commission data as Excel. Optional salesperson filter."""
    from fastapi.responses import StreamingResponse
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from io import BytesIO

    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to   = f"{year}-{month:02d}-{last_day}"

    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits,  err2 = await fetch_credits(date_from, date_to)
    if err1 or err2:
        return {"error": err1 or err2}

    all_records = invoices + credits

    def get_salesperson(r):
        user = r.get("invoice_user_id")
        if user and isinstance(user, (list, tuple)) and len(user) > 1:
            return user[1]
        return "Unassigned"

    # COLUMNS: exactly matching Odoo template (9 columns)
    headers = [
        "Invoice Partner Display Name",
        "Invoice/Bill Date",
        "Number",
        "Origin",
        "Untaxed Amount Signed",
        "Reference",
        "Source",
        "Payment Method",
        "Tags",
    ]

    # Group all records by salesperson
    by_person = {}
    for r in all_records:
        sp = get_salesperson(r)
        if sp not in by_person:
            by_person[sp] = []
        source = r.get("source_id")
        tags = r.get("tag_ids", [])
        is_credit = r.get("move_type") == "out_refund"
        sign = -1 if is_credit else 1
        untaxed = r.get("amount_untaxed_signed")
        if untaxed is None:
            untaxed = r.get("amount_untaxed", 0) * sign

        by_person[sp].append({
            "Invoice Partner Display Name": r.get("invoice_partner_display_name") or (r["partner_id"][1] if r.get("partner_id") else ""),
            "Invoice/Bill Date": r.get("invoice_date", ""),
            "Number": r.get("name", ""),
            "Origin": r.get("invoice_origin", "") or "",
            "Untaxed Amount Signed": round(untaxed, 2),
            "Reference": r.get("ref", "") or "",
            "Source": (source[1] if source and isinstance(source, (list, tuple)) and len(source) > 1 else "") if source else "",
            "Payment Method": r.get("x_payment_method", "") or "",
            "Tags": ", ".join(str(t) for t in tags) if tags else "",
        })

    # Sort each group by date descending
    for sp in by_person:
        by_person[sp].sort(key=lambda x: x["Invoice/Bill Date"] or "", reverse=True)

    # Filter by salesperson if specified
    if salesperson:
        filtered = {}
        for sp, rows in by_person.items():
            if salesperson.lower() in sp.lower():
                filtered[sp] = rows
        if filtered:
            by_person = filtered
        # If no match, keep all (fallback)

    # Sort salespersons by total amount descending
    sp_totals = {sp: sum(r["Untaxed Amount Signed"] for r in rows) for sp, rows in by_person.items()}
    sorted_persons = sorted(by_person.keys(), key=lambda sp: sp_totals[sp], reverse=True)

    # Build Excel
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Sheet1"

    # Row 1: Headers
    for ci, h in enumerate(headers, 1):
        ws.cell(row=1, column=ci, value=h)

    row_num = 2
    group_font = Font(bold=True)

    for sp in sorted_persons:
        rows = by_person[sp]
        count = len(rows)
        total = round(sp_totals[sp], 2)

        # Group header row: "Name (count)" in col A, total in col E (Untaxed Amount Signed)
        ws.cell(row=row_num, column=1, value=f"{sp} ({count})")
        ws.cell(row=row_num, column=1).font = group_font
        ws.cell(row=row_num, column=5, value=total)
        ws.cell(row=row_num, column=5).font = group_font
        ws.cell(row=row_num, column=5).number_format = '#,##0.00'
        row_num += 1

        # Detail rows
        for r in rows:
            for ci, h in enumerate(headers, 1):
                val = r[h]
                cell = ws.cell(row=row_num, column=ci, value=val)
                if h == "Untaxed Amount Signed":
                    cell.number_format = '#,##0.00'
            row_num += 1

    # Column widths
    col_widths = [35, 16, 18, 25, 20, 20, 20, 18, 12]
    for ci, w in enumerate(col_widths, 1):
        ws.column_dimensions[ws.cell(row=1, column=ci).column_letter].width = w

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"Commission_{year}_{month:02d}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
