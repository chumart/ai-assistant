from fastapi import FastAPI, UploadFile, File, BackgroundTasks
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
        print("DB initialized OK")
    except Exception as e:
        print(f"DB init error: {e}")
    finally:
        await conn.close()

@app.on_event("startup")
async def startup():
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
# Odoo helpers (unchanged)
# ─────────────────────────────────────────────

async def odoo_query(model, domain, fields, limit=2000, order="id desc"):
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as c:
            login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            cookies = login_r.cookies
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
        "description": "Search the internal knowledge base built from our websites (chumartusa.com, polarmanusa.com, flamasterusa.com, chefasstusa.com). Use this for product questions, specs, pricing, installation, sales training, competitor comparison. Always use this before answering product-related questions.",
        "input_schema": {"type":"object","properties":{"query":{"type":"string","description":"Search query"},"top_k":{"type":"integer","default":5,"description":"Number of results"}},"required":["query"]}
    }
]

def get_system_prompt(role: str = "guest", user_name: str = "", user_id: int = 0):
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

    return f"""今天是{today}。You are Chumart Assistant, an enterprise AI assistant.
You support both English and Chinese - reply in the same language the user uses.

CURRENT USER: {user_name} | ROLE: {perms['label']} | UID: {user_id}

KNOWLEDGE BASE RULES (MOST IMPORTANT):
- For ANY product question, spec, price, installation, or sales question: ALWAYS call search_knowledge first
- Never answer product questions from memory — always search first

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
When showing tabular data: ALWAYS format as markdown tables using | col | col | syntax."""


async def run_tool(name, inp):
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
        results = await search_knowledge(inp["query"], inp.get("top_k", 5))
        if not results:
            return "No relevant knowledge found. The knowledge base may not be crawled yet. Ask admin to run /admin/crawl."
        parts = []
        for r in results:
            if r.get("similarity", 0) > 0.25:
                parts.append(f"[{r['site_name']} | {r['page_title']}]\n{r['chunk_text']}")
        return "\n\n---\n\n".join(parts) if parts else "No sufficiently relevant results found."
    return "Unknown tool"

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
            return {"text": "", "name": file.filename, "file_id": fid}
        return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}
    except Exception as e:
        return {"error": str(e)}

# ─────────────────────────────────────────────
# Chat
# ─────────────────────────────────────────────

class ChatRequest(BaseModel):
    message: str
    history: list = []
    file_content: str = ""
    file_name: str = ""
    file_id: str = ""
    role: str = "guest"
    user_name: str = ""
    user_id: int = 0

@app.post("/chat")
async def chat(req: ChatRequest):
    perms = ROLE_PERMISSIONS.get(req.role, ROLE_PERMISSIONS["guest"])

    # Filter tools based on permissions
    allowed_tools = []
    finance_tools = {"get_monthly_tax", "get_quarterly_tax", "get_monthly_sales", "get_missing_tax"}
    for tool in TOOLS:
        tname = tool["name"]
        if tname in finance_tools and not perms["can_see_finance"]:
            continue  # hide finance tools from non-finance roles
        allowed_tools.append(tool)

    if req.file_id and req.file_id in FILE_CACHE:
        cached = FILE_CACHE[req.file_id]
        doc_type = "document" if cached["media_type"] == "application/pdf" else "image"
        user_message_content = [
            {"type": doc_type, "source": {"type": "base64", "media_type": cached["media_type"], "data": cached["b64"]}},
            {"type": "text", "text": f"[Attached file: {cached['name']}]\n\nUser question: {req.message}"}
        ]
    elif req.file_content and req.file_name:
        user_message_content = (
            f"=== ATTACHED FILE: {req.file_name} ===\n{req.file_content}\n=== END OF FILE ===\n\nUser question: {req.message}"
        )
    else:
        user_message_content = req.message

    messages = req.history + [{"role": "user", "content": user_message_content}]
    headers = {"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"}

    async with httpx.AsyncClient(timeout=120) as c:
        current_messages = list(messages)
        for _ in range(8):
            r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                "model": "claude-sonnet-4-5",
                "max_tokens": 4096,
                "system": get_system_prompt(req.role, req.user_name, req.user_id),
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
        return {"reply": reply or "Sorry, no response generated."}

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
    },
    "finance": {
        "label": "Finance",
        "can_see_finance":    True,
        "can_see_all_sales":  True,
        "can_see_cost":       True,
        "can_see_inventory":  True,
        "can_see_products":   True,
        "can_export":         True,
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
