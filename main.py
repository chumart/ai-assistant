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
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

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

# In-memory file cache
FILE_CACHE: dict = {}

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
        await conn.execute("""
            CREATE INDEX IF NOT EXISTS knowledge_embedding_idx
            ON knowledge_chunks
            USING ivfflat (embedding vector_cosine_ops)
            WITH (lists = 100)
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
    print("CHUMART AI BACKEND — BUILD: vendor-fix-v4 (2026-04-21)")
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
    try:
        async with httpx.AsyncClient(timeout=300) as c:
            r = await c.post(
                "https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={
                    "model": "claude-haiku-4-5-20251001",
                    "max_tokens": 8000,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": doc_type, "source": {"type": "base64", "media_type": media_type, "data": b64}},
                            {"type": "text", "text": "Extract ALL text content from this document completely. Include every section, table, specification, error code, procedure, and detail. Return raw text only, no commentary."}
                        ]
                    }]
                }
            )
            data = r.json()
            return "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
    except Exception as e:
        print(f"Text extraction error: {e}")
        return ""

async def process_document_to_kb(doc_id: str, doc_name: str, text: str, category: str):
    """Chunk document text and store in knowledge base."""
    if not text.strip():
        return 0

    conn = await get_db_conn()
    if not conn:
        return 0

    try:
        # Delete old chunks for this doc
        await conn.execute("DELETE FROM knowledge_chunks WHERE site_url = $1", f"doc:{doc_id}")

        chunks = chunk_text(text, chunk_size=600, overlap=100)
        count = 0
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
                    json.dumps(embedding), f"doc_{category}")
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

async def search_knowledge(query: str, top_k: int = 5, category: str = None) -> list:
    """Vector similarity search in knowledge base."""
    conn = await get_db_conn()
    if not conn:
        return []
    try:
        embedding = await get_embedding(query)
        if not embedding:
            return []

        if category:
            rows = await conn.fetch("""
                SELECT site_name, page_url, page_title, chunk_text,
                       1 - (embedding <=> $1::vector) AS similarity
                FROM knowledge_chunks
                WHERE category = $2
                ORDER BY embedding <=> $1::vector
                LIMIT $3
            """, json.dumps(embedding), category, top_k)
        else:
            rows = await conn.fetch("""
                SELECT site_name, page_url, page_title, chunk_text,
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
        "description": "Search the internal knowledge base — includes websites (chumartusa.com, polarmanusa.com, flamasterusa.com, chefasstusa.com) AND uploaded internal documents (employee handbook, service manuals, after-sales procedures, warranty docs). ALWAYS use this first for ANY product question, maintenance, repair, troubleshooting, error codes, company policy, or procedures.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Search query"},"top_k":{"type":"integer","default":6,"description":"Number of results"}},"required":["query"]}
    },
    {
        "name": "search_documents",
        "description": "Search for specific internal documents by name or category. Use when user asks to find or download a specific file like a service manual, employee handbook, or procedure document. Returns document name, category, and download link.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Document name or keywords"},"category":{"type":"string","description":"Optional: service_manual, employee_handbook, after_sales, warranty, general"}},"required":["query"]}
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
        finance_rules = """
FINANCIAL REPORT RULES (you have access):
- Monthly tax -> get_monthly_tax
- Quarterly tax -> get_quarterly_tax
- Monthly sales / commission base -> get_monthly_sales
- CA invoices missing tax -> get_missing_tax
- Can query account.move, account.payment with full access"""
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

SCOPE OF KNOWLEDGE (answer freely):
- Our own products: Chumart, Polarman, Flamaster, ChefAsst — specs, pricing, installation, maintenance
- Competitor/industry products: True, Turbo Air, Beverage-Air, Hoshizaki, Manitowoc, Continental, Victory, Traulsen, Arctic Air, and any other commercial refrigeration or foodservice equipment brands — answer product questions, maintenance, repair, troubleshooting, comparisons
- General commercial kitchen equipment: installation guides, cleaning procedures, error codes, preventive maintenance, repair tips
- Food service industry knowledge: NSF standards, health codes, energy efficiency, refrigerant types (R290, R404A, R134a etc.)
- General business questions related to the industry
- For questions completely outside work context (personal topics, entertainment etc): politely redirect to work topics"""


async def run_tool(name, inp):
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
    if name == "search_knowledge":
        results = await search_knowledge(inp["query"], inp.get("top_k", 6))
        if not results:
            return "No relevant knowledge found in knowledge base or documents."
        parts = []
        for r in results:
            if r.get("similarity", 0) > 0.25:
                source = r['site_name']
                # Add download link for document sources
                if r.get('site_url', '').startswith('doc:'):
                    doc_id = r['site_url'].replace('doc:', '')
                    parts.append(f"[📄 {source}]\n{r['chunk_text']}\n[Doc ID: {doc_id}]")
                else:
                    parts.append(f"[{source} | {r['page_title']}]\n{r['chunk_text']}")
        return "\n\n---\n\n".join(parts) if parts else "No sufficiently relevant results found."
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
        # If nothing found by template, try by product_id directly
        if not sup_rows and product_ids:
            r2 = await odoo_query(
                "product.supplierinfo",
                [["product_id", "in", product_ids]],
                ["product_id", "product_tmpl_id", "partner_id", "price", "min_qty", "currency_id", "company_id"],
                limit=1000, order="sequence asc"
            )
            sup_rows = json.loads(r2)
            if isinstance(sup_rows, dict) and "error" in sup_rows:
                sup_rows = []
        # If still nothing, try without any product filter (get ALL supplierinfo and match manually)
        if not sup_rows and tmpl_ids:
            # Broader search: get all supplierinfo records
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

        # Get one shared session for all Odoo calls
        cookies = await odoo_get_session()

        # ─────────────────────────────────────────────────────────────
        # Phase 0: ID rescue — rescue AI-hallucinated product_id / partner_id
        # by falling back to SKU / product_name / partner_name lookup.
        # ─────────────────────────────────────────────────────────────

        # (A) Batch-validate all product_ids the AI passed
        all_pids = list({l.get("product_id") for po in orders
                         for l in po.get("lines", []) if l.get("product_id")})
        valid_pids = set()
        if all_pids:
            r = json.loads(await odoo_query("product.product",
                [["id","in",all_pids],["active","=",True]],
                ["id"], limit=1000, cookies=cookies))
            if isinstance(r, list):
                valid_pids = {p["id"] for p in r}
        print(f"ID RESCUE: {len(valid_pids)}/{len(all_pids)} product_ids valid as-is")

        # (B) For invalid product_ids, try SKU → exact name → prefix name
        for po in orders:
            for line in po.get("lines", []):
                pid = line.get("product_id")
                if pid and pid in valid_pids:
                    continue
                sku = (line.get("sku") or "").strip()
                pname = (line.get("product_name") or "").strip()
                resolved = None
                via = None

                if sku:
                    r = json.loads(await odoo_query("product.product",
                        [["default_code","=",sku],["active","=",True]],
                        ["id"], limit=1, cookies=cookies))
                    if isinstance(r, list) and r:
                        resolved, via = r[0]["id"], f"SKU={sku}"

                if not resolved and pname:
                    r = json.loads(await odoo_query("product.product",
                        [["name","=",pname],["active","=",True]],
                        ["id"], limit=1, cookies=cookies))
                    if isinstance(r, list) and r:
                        resolved, via = r[0]["id"], "exact name"

                if not resolved and pname:
                    # Use the first distinctive chunk of name (before first comma)
                    prefix = pname.split(",")[0].strip()[:60]
                    if len(prefix) >= 10:
                        r = json.loads(await odoo_query("product.product",
                            [["name","ilike",prefix],["active","=",True]],
                            ["id","name"], limit=3, cookies=cookies))
                        if isinstance(r, list) and len(r) == 1:
                            resolved, via = r[0]["id"], f"prefix match '{prefix[:30]}'"

                if resolved:
                    print(f"ID RESCUE: product_id {pid} → {resolved} (via {via})")
                    line["product_id"] = resolved
                    valid_pids.add(resolved)
                else:
                    print(f"ID RESCUE FAIL: pid={pid} sku='{sku}' name='{pname[:60]}'")

        # (C) Validate partner_ids; rescue via partner_name
        all_partner_ids = list({po.get("partner_id") for po in orders if po.get("partner_id")})
        valid_suppliers = set()
        if all_partner_ids:
            r = json.loads(await odoo_query("res.partner",
                [["id","in",all_partner_ids]],
                ["id","supplier_rank"], limit=100, cookies=cookies))
            if isinstance(r, list):
                valid_suppliers = {p["id"] for p in r
                                   if (p.get("supplier_rank", 0) or 0) > 0}
        print(f"ID RESCUE: {len(valid_suppliers)}/{len(all_partner_ids)} partner_ids are valid suppliers")

        for po in orders:
            pid = po.get("partner_id")
            if pid in valid_suppliers:
                continue
            pname = (po.get("partner_name") or "").strip()
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
            po_result = await odoo_create("purchase.order", {
                "partner_id": partner_id,
                "company_id": 1,
                "date_order": now_str,
            }, cookies=cookies)
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

async def chat_openai(messages: list, system: str, model: str, tools: list) -> str:
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
                    "max_tokens": 4096,
                }
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
                        result = await run_tool(fn_name, fn_args)
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
        return {
            "total_chunks": total,
            "by_site": [dict(r) for r in by_site],
            "recent_crawls": [dict(r) for r in logs]
        }
    finally:
        await conn.close()

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
        if filename.endswith('.pdf'):
            b64 = base64.standard_b64encode(content).decode('utf-8')
            fid = str(uuid.uuid4())
            FILE_CACHE[fid] = {"b64": b64, "media_type": "application/pdf", "name": file.filename}
            return {"text": "", "name": file.filename, "file_id": fid}
        if filename.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            ext = filename.split('.')[-1].replace('jpg', 'jpeg')
            media_type = f"image/{ext}"
            b64 = base64.standard_b64encode(content).decode('utf-8')
            fid = str(uuid.uuid4())
            FILE_CACHE[fid] = {"b64": b64, "media_type": media_type, "name": file.filename}
            preview = f"data:{media_type};base64,{b64[:200]}..."  # truncated for transport
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

class ChatRequest(BaseModel):
    message: str
    history: list = []
    file_content: str = ""
    file_name: str = ""
    file_id: str = ""
    role: str = "guest"
    user_name: str = ""
    user_id: int = 0
    model: str = "claude-sonnet-4-5"
    free_mode: bool = False
    session_id: str = ""
    session_title: str = ""

@app.post("/chat")
async def chat(req: ChatRequest, background_tasks: BackgroundTasks):
    perms = ROLE_PERMISSIONS.get(req.role, ROLE_PERMISSIONS["guest"])

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

    messages = req.history + [{"role": "user", "content": user_message_content}]
    headers = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}

    # Load user memory
    memories = []
    if req.user_id:
        memories = await db_get_memory(req.user_id)

    # Determine model
    if req.role == "admin" and req.model in ALLOWED_MODELS:
        selected_model = req.model
    elif req.role != "admin" and req.model in NON_ADMIN_MODELS:
        selected_model = req.model
    else:
        selected_model = "claude-sonnet-4-5"

    system_prompt = get_system_prompt(req.role, req.user_name, req.user_id, req.free_mode, memories)

    # Route to OpenAI if selected
    if selected_model in OPENAI_MODELS:
        # For OpenAI, swap image format if file attached
        if has_file and openai_image_content:
            oai_messages = req.history + [{"role": "user", "content": openai_image_content}]
        else:
            oai_messages = messages
        reply = await chat_openai(oai_messages, system_prompt, selected_model, allowed_tools)
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
                            result = await run_tool(block["name"], block.get("input", {}))
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

ALLOWED_CATEGORIES = ["service_manual", "employee_handbook", "after_sales", "warranty", "general"]

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
    """Background task: extract text and index into knowledge base."""
    print(f"Processing document: {filename}")
    text = await extract_text_from_file(file_bytes, filename, mime_type)
    if text:
        count = await process_document_to_kb(doc_id, filename, text, category)
        print(f"Indexed {count} chunks for {filename}")
    else:
        print(f"No text extracted from {filename}")

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
async def get_signed_url(doc_id: str):
    """Generate a time-limited signed URL for secure document download (1 hour expiry)."""
    conn = await get_db_conn()
    if not conn:
        return {"error": "DB not connected"}
    try:
        row = await conn.fetchrow("SELECT r2_key, original_name FROM documents WHERE id=$1", doc_id)
        if not row:
            return {"error": "Document not found"}

        import asyncio
        loop = asyncio.get_event_loop()
        client = get_r2_client()
        if not client:
            return {"error": "Storage not configured"}

        # Generate presigned URL valid for 1 hour
        signed_url = await loop.run_in_executor(None, lambda: client.generate_presigned_url(
            'get_object',
            Params={'Bucket': R2_BUCKET, 'Key': row['r2_key']},
            ExpiresIn=3600  # 1 hour
        ))
        return {"url": signed_url, "filename": row["original_name"], "expires_in": 3600}
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

@app.get("/test-odoo")
async def test_odoo():
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc":"2.0","method":"call","id":1,
                "params":{"db":ODOO_DB,"login":ODOO_USERNAME,"password":ODOO_PASSWORD}
            })
            return {"status": r.status_code, "body": r.text[:300]}
    except Exception as e:
        return {"error": str(e)}

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
        "can_write_odoo":     False,
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

async def get_user_role(uid: int, cookies) -> str:
    """Query Odoo groups for a logged-in user and return their highest role."""
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            # Get user's group IDs
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 2,
                "params": {
                    "model": "res.users", "method": "read",
                    "args": [[uid]],
                    "kwargs": {"fields": ["groups_id"]}
                }
            }, cookies=cookies)
            data = r.json()
            result = data.get("result", [])
            if not result:
                return "guest"
            group_ids = result[0].get("groups_id", [])

            # Get group XML IDs
            r2 = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
                "jsonrpc": "2.0", "method": "call", "id": 3,
                "params": {
                    "model": "ir.model.data", "method": "search_read",
                    "args": [[["model", "=", "res.groups"], ["res_id", "in", group_ids]]],
                    "kwargs": {"fields": ["module", "name", "res_id"], "limit": 100}
                }
            }, cookies=cookies)
            data2 = r2.json()
            xml_ids = set()
            for rec in data2.get("result", []):
                xml_ids.add(f"{rec['module']}.{rec['name']}")

            # Match to role in priority order
            for xml_id, role in ODOO_GROUP_ROLE_MAP:
                if xml_id in xml_ids:
                    return role

            return "guest"
    except Exception as e:
        print(f"Role detection error: {e}")
        return "guest"


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
            cookies = r.cookies

            # Detect role
            role = await get_user_role(uid, cookies)
            permissions = ROLE_PERMISSIONS.get(role, ROLE_PERMISSIONS["guest"])

            return {
                "success":     True,
                "uid":         uid,
                "name":        result.get("name", req.username),
                "username":    req.username,
                "role":        role,
                "role_label":  permissions["label"],
                "permissions": permissions,
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
