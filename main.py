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
from urllib.parse import urljoin, urlparse
from typing import Optional

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

VALID_STATES = ["paid", "in_payment", "reversed"]
CA_STATE_ID  = 13

# In-memory file cache with timestamps for TTL cleanup
FILE_CACHE: dict = {}  # file_id -> {b64, media_type, name, created_at}

# Server-side session store: token -> {uid, role, username, created_at}
# This prevents clients from forging their own role
SESSION_STORE: dict = {}
SESSION_TTL_HOURS = 12  # Sessions expire after 12 hours
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
        print("DB initialized OK")
    except Exception as e:
        print(f"DB init error: {e}")
    finally:
        await conn.close()

@app.on_event("startup")
async def startup():
    print("=" * 60)
    print("CHUMART AI BACKEND — BUILD: security-v13 (2026-04-22)")
    print("=" * 60)
    await init_db()

# ─────────────────────────────────────────────
# Embedding
# ─────────────────────────────────────────────

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
    """Extract text from PDF, Word, image, or plain text files."""
    fname = filename.lower()

    # Plain text
    if fname.endswith(('.txt', '.md', '.csv')):
        return file_bytes.decode('utf-8', errors='ignore')

    # PDF or image — use Claude vision
    if fname.endswith('.pdf'):
        doc_type = "document"
        media_type = "application/pdf"
    elif fname.endswith(('.png', '.jpg', '.jpeg', '.webp')):
        ext = fname.split('.')[-1].replace('jpg', 'jpeg')
        doc_type = "image"
        media_type = f"image/{ext}"
    elif fname.endswith(('.docx', '.doc')):
        # Try python-docx first
        try:
            import io
            from docx import Document as DocxDocument
            doc = DocxDocument(io.BytesIO(file_bytes))
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            return text
        except Exception:
            # Fallback to Claude
            doc_type = "document"
            media_type = "application/vnd.openxmlformats-officedocument.wordprocessingml.document"
    else:
        return file_bytes.decode('utf-8', errors='ignore')

    # Use Claude to extract text from PDF/image/docx
    b64 = base64.standard_b64encode(file_bytes).decode('utf-8')
    print(f"TEXT EXTRACT: {filename} ({len(file_bytes)//1024}KB) via Claude")

    # For large PDFs, extract in two passes to avoid token limit truncation
    # Pass 1: full document extraction
    # Pass 2: if result seems truncated, extract the second half separately
    async def extract_pass(prompt_suffix=""):
        try:
            async with httpx.AsyncClient(timeout=300) as c:
                r = await c.post(
                    "https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                    json={
                        "model": "claude-sonnet-4-5",  # Use Sonnet for better long-doc extraction
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
            # Merge: first pass + second pass (deduplicated roughly)
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

async def odoo_query(model, domain, fields, limit=2000, order="id desc", cookies=None):
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
                "params": {
                    "model": model, "method": "search_read",
                    "args": [domain],
                    "kwargs": {"fields": fields, "limit": limit, "order": order}
                }
            }, cookies=cookies)
            data = r.json()
            if data.get("error"):
                return json.dumps({"error": data["error"].get("message", str(data["error"]))})
            return json.dumps(data.get("result", []), default=str, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

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
         "amount_tax", "amount_total", "payment_state",
         "ref", "source_id", "x_payment_method", "tag_ids"],
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
         "amount_tax", "amount_total", "payment_state",
         "ref", "source_id", "x_payment_method", "tag_ids"],
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
        untaxed_signed = r.get("amount_untaxed_signed")
        if untaxed_signed is not None:
            total_untaxed += untaxed_signed
        else:
            total_untaxed += r.get("amount_untaxed", 0) * sign
        total_tax    += r.get("amount_tax", 0) * sign
        total_amount += r.get("amount_total", 0) * sign
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
            untaxed_signed = r.get("amount_untaxed_signed")
            by_person[name]["amount_untaxed"] += untaxed_signed if untaxed_signed is not None else r.get("amount_untaxed", 0) * sign
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
        "description": "Add a product line to an existing purchase.order or sale.order. Call after odoo_create_record to add products. Requires order_id, product_id, quantity, and price_unit.",
        "input_schema": {
            "type": "object",
            "properties": {
                "order_type": {"type": "string", "description": "purchase or sale"},
                "order_id": {"type": "integer", "description": "The ID of the order"},
                "product_id": {"type": "integer", "description": "Product ID from Odoo"},
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
        "description": "Get all vendors for a list of products from product.supplierinfo. Returns each product with ALL its vendors (name, price, min_qty). If a product has multiple vendors, AI must ask user to choose. Use before creating any purchase order.",
        "input_schema": {
            "type": "object",
            "properties": {
                "product_ids": {"type": "array", "items": {"type": "integer"}, "description": "List of product IDs"}
            },
            "required": ["product_ids"]
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
    }
]

def get_system_prompt(role: str = "guest", user_name: str = "", user_id: int = 0, free_mode: bool = False, memories: list = []):
    today = datetime.date.today().strftime("%Y年%m月%d日")
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
2. Call get_monthly_sales with the correct year and month
3. Present results in TWO parts:

PART A — Summary table (by salesperson):
| 销售员 | 发票数 | 退款数 | 发票金额 | 退款金额 | 净销售额(税前) |
Use the by_salesperson data from the tool result. Show ALL salespeople. Add a total row at the bottom.
Use $ with commas for all amounts. Copy numbers directly from tool response — never recalculate.

PART B — Commission base totals:
Show commission_base values: net_sales_excl_tax, net_tax, net_sales_incl_tax, invoice_count, credit_note_count.

PART C — Excel export button:
Always end with this exact markdown link for Excel download:
[📥 Export Excel](BACKEND_URL/export/commission?year=YYYY&month=MM)
Replace BACKEND_URL with: {os.getenv('RAILWAY_PUBLIC_DOMAIN', 'https://chumart-ai.up.railway.app')}
Replace YYYY and MM with the actual year and month numbers.

Example: user says "3月commission" in 2026 → call get_monthly_sales(year=2026, month=3) → show table → show [📥 Export Excel](https://chumart-ai.up.railway.app/export/commission?year=2026&month=3)

The Excel export follows the SALE COMMISSION NEW template with fields:
Invoice Partner Display Name, Invoice/Bill Date, Number, Origin, Untaxed Amount Signed, Reference, Source, Payment Method, Tags, Salesperson, Payment Status"""
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
- Cannot view commission reports for others"""

    cost_rules = ""
    if not perms["can_see_cost"]:
        cost_rules = """
COST/MARGIN RULES (NO ACCESS):
- NEVER show standard_price, cost, or margin fields
- If asked about cost or margin, say this information is restricted
- Only show sales price (list_price), not cost price (standard_price)"""
    else:
        cost_rules = "COST RULES: Can view all cost and margin data."

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
        return f"""今天是{today}。你是 Chumart AI，Chumart 管理员的专属私人助手。
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

    return f"""今天是{today}。You are Chumart Assistant, an enterprise AI assistant.
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

When showing financial data: use $ with commas, be precise.
When helping sales: be specific, cite model numbers, give concrete talking points.
Only use markdown tables when data is genuinely tabular (multi-row comparisons, reports, lists with multiple columns). Do NOT use tables for single items, simple answers, or narrative responses.
CALCULATION RULES: When summing financial data from tool results, always use the exact numbers returned by the tool. Never recalculate totals yourself — use the pre-calculated values from the data (commission_base.net_sales_excl_tax etc). If showing a summary, copy the numbers directly from the tool response.

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

STEP 1 — EXACT SEARCH
Search for the exact term: search_knowledge(query="[model/topic as given]")
→ If results contain useful content: answer directly using the chunk text. DONE.
→ If empty or irrelevant: go to Step 2.

STEP 2 — DOCUMENT SCAN
Search just the model/product name to find which documents exist:
search_knowledge(query="[model number]") and search_knowledge(query="[product type]")
→ If you find a matching document: search WITHIN it using doc_name filter:
   search_knowledge(query="[the topic/symptom]", doc_name="[document name]")
→ If results still insufficient: go to Step 3.

STEP 3 — INFER AND BROADEN
Infer the product category from the model number or name:
- FLM- prefix → Flamaster brand (gas fryers, griddles, ranges)
- PLM- prefix → Polarman brand (refrigerators, freezers)
- CMPC/SLBM etc → Chumart accessories
- "fryer" / "freezer" / "refrigerator" → search by equipment type
Then search: search_knowledge(query="[inferred brand] [equipment type] [topic]")
→ If found: answer and note "I found this in our [document name], which covers similar models"
→ If still not found: go to Step 4.

STEP 4 — ASK THE USER
Tell the user specifically what you searched and what's missing. Ask a focused question:
- "I searched for [X] but didn't find a specific manual for [model]. Is this a [product type]? Do you have a service manual I can add to the knowledge base?"
- Never just say "not found" — always explain what you tried and what would help.

CRITICAL RULES FOR DOCUMENT CONTENT:
- The chunk_text in search results IS the actual text from the document — read it and use it directly
- NEVER say "I cannot open/read the file" or "download to check" — the text is already in the results
- If a chunk mentions a troubleshooting table, error code, or procedure — quote it directly in your answer
- Always cite the source document name when using document content
- If the user asks about content that IS in the results but you're unsure — quote the relevant section verbatim

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
        vals = {
            order_field: inp["order_id"],
            "product_id": inp["product_id"],
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
                ["product_id", "product_tmpl_id", "partner_id", "price", "min_qty", "currency_id", "company_id"],
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
                ["product_id", "product_tmpl_id", "partner_id", "price", "min_qty", "currency_id", "company_id"],
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
                ["product_id", "product_tmpl_id", "partner_id", "price"],
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

    if name == "odoo_create_bulk_po":
        orders = inp.get("purchase_orders", [])
        created = []
        errors = []
        import datetime
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
        await conn.execute("""
            INSERT INTO chat_sessions (id, uid, username, title, messages, updated_at)
            VALUES ($1, $2, $3, $4, $5::jsonb, NOW())
            ON CONFLICT (id) DO UPDATE
            SET messages = $5::jsonb, title = $4, updated_at = NOW()
        """, session_id, uid, username, title, json.dumps(messages, ensure_ascii=False))
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
        # Check expiry
        age = (datetime.datetime.now() - s["created_at"]).total_seconds()
        if age < SESSION_TTL_HOURS * 3600:
            return {"uid": s["uid"], "role": s["role"], "user_name": s["name"]}
        else:
            del SESSION_STORE[req.session_token]
    # No valid token — treat as guest (prevents role spoofing)
    if req.session_token:
        print(f"SECURITY: invalid/expired session_token, treating as guest")
        return {"uid": 0, "role": "guest", "user_name": ""}
    # No token at all — legacy mode (backward compat during transition)
    return {"uid": req.user_id, "role": req.role, "user_name": req.user_name}

@app.post("/chat")
async def chat(req: ChatRequest, background_tasks: BackgroundTasks):
    sess = resolve_session(req)
    verified_role = sess["role"]
    verified_uid = sess["uid"]
    verified_name = sess["user_name"]
    perms = ROLE_PERMISSIONS.get(verified_role, ROLE_PERMISSIONS["guest"])

    # Filter tools based on permissions
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax"}
    write_tools = {"odoo_create_record", "odoo_add_order_line", "odoo_confirm_order", "odoo_update_record"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms.get("can_see_finance"):
            continue
        if tname in write_tools and not perms.get("can_write_odoo"):
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

    # Determine model
    if verified_role == "admin" and req.model in ALLOWED_MODELS:
        selected_model = req.model
    elif verified_role != "admin" and req.model in NON_ADMIN_MODELS:
        selected_model = req.model
    else:
        selected_model = "claude-haiku-4-5-20251001"

    system_prompt = get_system_prompt(verified_role, verified_name, verified_uid, req.free_mode, memories)

    # Context passed to tools (for buyer attribution, etc.)
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
      - {type: "done"}                      stream complete
      - {type: "error", message: "..."}     error occurred
    """
    from fastapi.responses import StreamingResponse

    sess = resolve_session(req)
    verified_role = sess["role"]
    verified_uid = sess["uid"]
    verified_name = sess["user_name"]
    perms = ROLE_PERMISSIONS.get(verified_role, ROLE_PERMISSIONS["guest"])

    # Filter tools based on permissions
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax"}
    write_tools = {"odoo_create_record", "odoo_add_order_line", "odoo_confirm_order", "odoo_update_record"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms.get("can_see_finance"):
            continue
        if tname in write_tools and not perms.get("can_write_odoo"):
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

    # Determine model
    if verified_role == "admin" and req.model in ALLOWED_MODELS:
        selected_model = req.model
    elif verified_role != "admin" and req.model in NON_ADMIN_MODELS:
        selected_model = req.model
    else:
        selected_model = "claude-haiku-4-5-20251001"

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

        return StreamingResponse(openai_stream(), media_type="text/event-stream", headers={
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

    return StreamingResponse(claude_stream(), media_type="text/event-stream", headers={
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
    # Finance / Accounting
    ("account.group_account_manager",      "finance"),
    ("account.group_account_user",         "finance"),
    ("account.group_account_invoice",      "finance"),
    # Sales
    ("sales_team.group_sale_manager",      "sales"),
    ("sales_team.group_sale_salesman",     "sales"),
    ("base.group_sale_salesman",           "sales"),
    # Warehouse / Inventory
    ("stock.group_stock_manager",          "warehouse"),
    ("stock.group_stock_user",             "warehouse"),
    ("purchase.group_purchase_manager",    "warehouse"),
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
            # Client will send this in X-Session-Token header; backend resolves uid/role from it
            session_token = str(uuid.uuid4())
            cleanup_caches()
            SESSION_STORE[session_token] = {
                "uid": uid,
                "username": req.username,
                "name": name,
                "role": role,
                "created_at": datetime.datetime.now()
            }
            print(f"LOGIN: uid={uid} name={name} role={role} token={session_token[:8]}...")

            return {
                "success":       True,
                "uid":           uid,
                "name":          name,
                "username":      req.username,
                "role":          role,
                "role_label":    permissions["label"],
                "permissions":   permissions,
                "session_token": session_token,  # Frontend stores and sends this
            }
    except Exception as e:
        return {"success": False, "error": str(e)}

# ─────────────────────────────────────────────
# Excel Export
# ─────────────────────────────────────────────

@app.get("/export/commission")
async def export_commission(year: int, month: int):
    """Export full commission data as Excel matching SALE COMMISSION NEW template."""
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

    def get_salesperson(r):
        user = r.get("invoice_user_id")
        if user and isinstance(user, (list, tuple)) and len(user) > 1:
            return user[1]
        return "Unassigned"

    def make_row(r, is_credit=False):
        source = r.get("source_id")
        sign = -1 if is_credit else 1
        return {
            "Invoice Partner Display Name": r.get("invoice_partner_display_name") or (r["partner_id"][1] if r.get("partner_id") else ""),
            "Invoice/Bill Date":            r.get("invoice_date", ""),
            "Number":                       r.get("name", ""),
            "Origin":                       r.get("invoice_origin", "") or "",
            "Untaxed Amount Signed":        round((r.get("amount_untaxed_signed") or r.get("amount_untaxed", 0) * sign), 2),
            "Reference":                    r.get("ref", "") or "",
            "Source":                       (source[1] if source and isinstance(source,(list,tuple)) and len(source)>1 else ""),
            "Payment Method":               r.get("x_payment_method", "") or "",
            "Tags":                         "",
            "Salesperson":                  get_salesperson(r),
            "Payment Status":               r.get("payment_state", ""),
        }

    all_rows = (
        [make_row(r, False) for r in invoices] +
        [make_row(r, True)  for r in credits]
    )
    all_rows.sort(key=lambda x: (x["Salesperson"], x["Invoice/Bill Date"]))

    # Build Excel
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"{year}-{month:02d} Commission"

    headers = ["Invoice Partner Display Name","Invoice/Bill Date","Number","Origin",
               "Untaxed Amount Signed","Reference","Source","Payment Method","Tags",
               "Salesperson","Payment Status"]

    # Header style
    header_fill = PatternFill("solid", fgColor="1F4E79")
    header_font = Font(bold=True, color="FFFFFF", size=11)
    thin = Side(style="thin", color="CCCCCC")
    border = Border(left=thin, right=thin, top=thin, bottom=thin)

    for ci, h in enumerate(headers, 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", vertical="center")
        cell.border = border

    ws.row_dimensions[1].height = 20

    # Data rows
    for ri, row in enumerate(all_rows, 2):
        for ci, h in enumerate(headers, 1):
            val = row.get(h, "")
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.border = border
            cell.alignment = Alignment(vertical="center")
            # Format amount column
            if h == "Untaxed Amount Signed":
                cell.number_format = '#,##0.00'
                cell.alignment = Alignment(horizontal="right", vertical="center")
            # Alternate row color
            if ri % 2 == 0:
                cell.fill = PatternFill("solid", fgColor="F2F7FF")

    # Auto column widths
    col_widths = [30, 14, 18, 20, 18, 15, 20, 16, 12, 16, 14]
    for ci, w in enumerate(col_widths, 1):
        ws.column_dimensions[ws.cell(row=1, column=ci).column_letter].width = w

    ws.freeze_panes = "A2"

    # Summary sheet
    ws2 = wb.create_sheet("Summary")
    ws2.append(["Salesperson", "Invoice Count", "Credit Count", "Net Sales (Excl Tax)"])
    by_person = {}
    for r in all_rows:
        sp = r["Salesperson"]
        if sp not in by_person:
            by_person[sp] = {"inv": 0, "crd": 0, "net": 0}
        if r["Payment Status"] in VALID_STATES:
            amt = r["Untaxed Amount Signed"]
            if amt < 0:
                by_person[sp]["crd"] += 1
            else:
                by_person[sp]["inv"] += 1
            by_person[sp]["net"] += amt
    for sp, v in sorted(by_person.items(), key=lambda x: -x[1]["net"]):
        ws2.append([sp, v["inv"], v["crd"], round(v["net"], 2)])

    # Style summary header
    for cell in ws2[1]:
        cell.font = Font(bold=True, color="FFFFFF")
        cell.fill = PatternFill("solid", fgColor="1F4E79")

    output = BytesIO()
    wb.save(output)
    output.seek(0)

    filename = f"Commission_{year}_{month:02d}.xlsx"
    return StreamingResponse(
        output,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"}
    )
