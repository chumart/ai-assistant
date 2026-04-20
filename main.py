from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import json
import base64

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ODOO_URL      = os.getenv("ODOO_URL", "")
ODOO_DB       = os.getenv("ODOO_DB", "")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

class ChatRequest(BaseModel):
    message: str
    history: list = []
    file_content: str = ""
    file_name: str = ""

# ---------- Odoo ----------

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
            fields = {k: {"label": v.get("string",""), "type": v.get("type","")}
                      for k, v in data.get("result", {}).items()
                      if v.get("type") in ["char","integer","float","monetary","date","datetime","boolean","many2one","selection"]}
            return json.dumps(fields, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ---------- Tools ----------

TOOLS = [
    {
        "name": "odoo_search",
        "description": """Search Odoo data. Common models:
- sale.order: name, partner_id, state(draft/sent/sale/done/cancel), amount_total, date_order
- product.product: name, default_code, list_price, qty_available, virtual_available, barcode
- res.partner: name, phone, email, city, customer_rank
- account.move: name, partner_id, move_type, state, invoice_date, amount_untaxed, amount_tax, amount_total, payment_state
  move_type: out_invoice=customer invoice, out_refund=credit note, in_invoice=vendor bill
  state: draft, posted, cancel
  payment_state: not_paid, in_payment, paid, reversed, partial
- repair.order: name, partner_id, product_id, state
- stock.quant: product_id, location_id, quantity, reserved_quantity
- purchase.order: name, partner_id, state, amount_total, date_order
- account.payment: name, partner_id, amount, date, state, payment_type""",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string"},
                "domain": {"type": "array"},
                "fields": {"type": "array", "items": {"type": "string"}},
                "limit": {"type": "integer", "default": 2000},
                "order": {"type": "string", "default": "id desc"}
            },
            "required": ["model", "domain", "fields"]
        }
    },
    {
        "name": "odoo_fields",
        "description": "List available fields for any Odoo model.",
        "input_schema": {
            "type": "object",
            "properties": {"model": {"type": "string"}},
            "required": ["model"]
        }
    },
    {
        "name": "get_invoice_stats",
        "description": "Get accurate invoice statistics for a specific month. Returns exact count, tax total, amount total, and payment state breakdown. Always use this tool when user asks about invoice counts, tax totals, or payment status for a specific month or period. This is more accurate than odoo_search for invoice statistics.",
        "input_schema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer", "description": "Year, e.g. 2026"},
                "month": {"type": "integer", "description": "Month number 1-12, e.g. 2 for February"}
            },
            "required": ["year", "month"]
        }
    }
]

SYSTEM = """You are Chumart Assistant, an enterprise AI assistant connected to Odoo 17 ERP system.
You can query ANY data in Odoo using the odoo_search tool.
You support both English and Chinese — reply in the same language the user uses.

CRITICAL RULES for querying:
- ABSOLUTE RULE: When user mentions ANY time period (month/quarter/year/date), ALWAYS include date filters in the query. Never omit date filters when a time period is specified.
- For date ranges: use >= for start date and <= for end date. Example for February 2026: [["invoice_date",">=","2026-02-01"],["invoice_date","<=","2026-02-28"]]
- For monthly queries, always use the 1st as start and last day of month as end
- For aggregation queries (totals, sums), ALWAYS set limit to 2000 to get ALL records
- Invoice types: out_invoice = customer invoice, out_refund = credit note, in_invoice = vendor bill, in_refund = vendor credit note
- Always filter invoices by state=posted unless asked otherwise
- For account.move, sale.order, purchase.order, account.payment, res.partner, crm.lead, repair.order, stock.picking: automatically filtered by company_id=1
- For product.product, stock.quant and other inventory models: do NOT add company_id filter
- When calculating tax totals, query BOTH out_invoice AND out_refund separately or together, and clearly show the breakdown
- When showing invoice summaries, also break down by payment_state. Values: in_payment (In Payment), paid (Paid), reversed (Reversed). Always include ALL payment states in the count.

*** INVENTORY & SEARCH RULES ***
- FUZZY SEARCH FIRST: When a user searches for a product, customer, or document (e.g., "54rs", "PLM"), ALWAYS use the ilike operator instead of =. NEVER demand an exact match.
- PRODUCT SEARCH LOGIC: If looking for a product, ALWAYS use OR logic to search BOTH the internal reference and the name. Example: ["|", ["default_code", "ilike", "user_keyword"], ["name", "ilike", "user_keyword"]]
- EXCLUDE VIRTUAL LOCATIONS: When querying stock levels (stock.quant), you MUST ONLY return real, physical warehouse stock. ALWAYS append ["location_id.usage", "=", "internal"] to your domain to filter out all Virtual Locations, Inventory adjustments, Scrap, or Partner Locations.

When showing financial data, format numbers with $ and commas.
Be precise with numbers. Double check your math."""

async def run_tool(name, inp):
    if name == "odoo_search":
        model = inp["model"]
        domain = inp.get("domain", [])
        # Only add company_id filter for models that support it
        models_with_company = [
            "account.move", "sale.order", "purchase.order", "account.payment",
            "res.partner", "crm.lead", "repair.order", "stock.picking"
        ]
        if model in models_with_company:
            domain = domain + [["company_id", "=", 1]]
        return await odoo_query(
            model, domain, inp["fields"],
            inp.get("limit", 2000), inp.get("order", "id desc"))
    if name == "odoo_fields":
        return await odoo_list_fields(inp["model"])
    if name == "get_invoice_stats":
        year = inp.get("year", 2026)
        month = inp.get("month", 1)
        result = await invoice_stats(year, month)
        return json.dumps(result, ensure_ascii=False)
    return "Unknown tool"


# ---------- File extraction ----------

@app.post("/extract-file")
async def extract_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        filename = file.filename.lower()

        if filename.endswith(('.txt', '.md', '.csv')):
            return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}

        if filename.endswith('.pdf'):
            media_type = "application/pdf"
            doc_type = "document"
        elif filename.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            ext = filename.split('.')[-1].replace('jpg', 'jpeg')
            media_type = f"image/{ext}"
            doc_type = "image"
        else:
            return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}

        b64 = base64.standard_b64encode(content).decode('utf-8')
        async with httpx.AsyncClient(timeout=60) as c:
            r = await c.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key": ANTHROPIC_KEY, "anthropic-version": "2023-06-01", "content-type": "application/json"},
                json={
                    "model": "claude-sonnet-4-5",
                    "max_tokens": 2000,
                    "messages": [{
                        "role": "user",
                        "content": [
                            {"type": doc_type, "source": {"type": "base64", "media_type": media_type, "data": b64}},
                            {"type": "text", "text": "Extract and return ALL text content from this file. Return the raw text only, no commentary."}
                        ]
                    }]
                })
            data = r.json()
            text = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
            return {"text": text, "name": file.filename}
    except Exception as e:
        return {"error": str(e)}

# ---------- Chat ----------

@app.post("/chat")
async def chat(req: ChatRequest):
    user_content = req.message
    if req.file_content and req.file_name:
        user_content = f"[Attached file: {req.file_name}]\n{req.file_content}\n\nUser question: {req.message}"

    messages = req.history + [{"role": "user", "content": user_content}]
    headers = {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    async with httpx.AsyncClient(timeout=90) as c:
        current_messages = list(messages)
        for _ in range(5):
            r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                "model": "claude-sonnet-4-5",
                "max_tokens": 2048,
                "system": SYSTEM,
                "tools": TOOLS,
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
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block["id"],
                            "content": result
                        })
                current_messages.append({"role": "assistant", "content": d["content"]})
                current_messages.append({"role": "user", "content": tool_results})
            else:
                break

        reply = "".join(b.get("text", "") for b in d.get("content", []) if b.get("type") == "text")
        return {"reply": reply or "Sorry, no response generated."}
@app.get("/invoice-stats")
async def invoice_stats(year: int, month: int):
    """Fixed invoice query - always accurate"""
    import calendar
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"
    
    result = await odoo_query(
        "account.move",
        [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", date_from],
            ["invoice_date", "<=", date_to]
        ],
        ["name", "amount_tax", "amount_total", "amount_untaxed", "payment_state"],
        limit=2000
    )
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records:
        return records
    
    by_state = {}
    total_tax = 0
    total_amount = 0
    for r in records:
        state = r.get("payment_state", "unknown")
        by_state[state] = by_state.get(state, 0) + 1
        total_tax += r.get("amount_tax", 0)
        total_amount += r.get("amount_total", 0)
    
    return {
        "period": f"{year}-{month:02d}",
        "total_invoices": len(records),
        "by_payment_state": by_state,
        "total_tax": round(total_tax, 2),
        "total_amount": round(total_amount, 2)
    }

# ---------- Health ----------

@app.get("/test-odoo")
async def test_odoo():
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            return {"status": r.status_code, "body": r.text[:300]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok"}
