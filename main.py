from fastapi import FastAPI, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import json
import base64
import calendar

app = FastAPI()
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

ODOO_URL      = os.getenv("ODOO_URL", "")
ODOO_DB       = os.getenv("ODOO_DB", "")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

VALID_STATES  = ["paid", "in_payment", "reversed"]
CA_STATE_ID   = 13

class ChatRequest(BaseModel):
    message: str
    history: list = []
    file_content: str = ""
    file_name: str = ""
    file_b64: str = ""        # raw base64 for PDF/image direct passing
    file_media_type: str = "" # e.g. "application/pdf" or "image/jpeg"

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
            fields = {k: {"label": v.get("string", ""), "type": v.get("type", "")}
                      for k, v in data.get("result", {}).items()
                      if v.get("type") in ["char", "integer", "float", "monetary", "date", "datetime", "boolean", "many2one", "selection"]}
            return json.dumps(fields, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ---------- Core helpers ----------

async def fetch_moves(move_type, date_from, date_to):
    result = await odoo_query(
        "account.move",
        [
            ["move_type", "=", move_type],
            ["state", "=", "posted"],
            ["invoice_date", ">=", date_from],
            ["invoice_date", "<=", date_to],
            ["company_id", "=", 1],
            ["payment_state", "in", VALID_STATES]
        ],
        ["name", "partner_id", "invoice_date", "amount_untaxed", "amount_tax", "amount_total", "payment_state"],
        limit=2000
    )
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records:
        return [], records["error"]
    return records, None

def summarize_moves(records):
    by_state = {"paid": 0, "in_payment": 0, "reversed": 0}
    total_untaxed = 0
    total_tax = 0
    total_amount = 0
    for r in records:
        state = r.get("payment_state", "")
        if state in by_state:
            by_state[state] += 1
        total_untaxed += r.get("amount_untaxed", 0)
        total_tax += r.get("amount_tax", 0)
        total_amount += r.get("amount_total", 0)
    return {
        "count": len(records),
        "by_payment_state": by_state,
        "total_untaxed": round(total_untaxed, 2),
        "total_tax": round(total_tax, 2),
        "total_amount": round(total_amount, 2)
    }

# ---------- Reports ----------

@app.get("/report/monthly-tax")
async def monthly_tax(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"

    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits, err2 = await fetch_moves("out_refund", date_from, date_to)
    if err1 or err2:
        return {"error": err1 or err2}

    inv = summarize_moves(invoices)
    crd = summarize_moves(credits)

    return {
        "period": f"{year}-{month:02d}",
        "report_type": "Monthly Tax Report",
        "invoices": inv,
        "credit_notes": crd,
        "net": {
            "count": inv["count"] - crd["count"],
            "total_untaxed": round(inv["total_untaxed"] - crd["total_untaxed"], 2),
            "total_tax": round(inv["total_tax"] - crd["total_tax"], 2),
            "total_amount": round(inv["total_amount"] - crd["total_amount"], 2)
        }
    }

@app.get("/report/quarterly-tax")
async def quarterly_tax(year: int, quarter: int):
    if quarter not in [1, 2, 3, 4]:
        return {"error": "Quarter must be 1-4"}

    start_month = (quarter - 1) * 3 + 1
    end_month = start_month + 2
    last_day = calendar.monthrange(year, end_month)[1]
    date_from = f"{year}-{start_month:02d}-01"
    date_to = f"{year}-{end_month:02d}-{last_day}"

    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits, err2 = await fetch_moves("out_refund", date_from, date_to)
    if err1 or err2:
        return {"error": err1 or err2}

    inv = summarize_moves(invoices)
    crd = summarize_moves(credits)

    monthly = []
    for m in range(start_month, end_month + 1):
        ld = calendar.monthrange(year, m)[1]
        inv_m, _ = await fetch_moves("out_invoice", f"{year}-{m:02d}-01", f"{year}-{m:02d}-{ld}")
        crd_m, _ = await fetch_moves("out_refund", f"{year}-{m:02d}-01", f"{year}-{m:02d}-{ld}")
        inv_s = summarize_moves(inv_m)
        crd_s = summarize_moves(crd_m)
        monthly.append({
            "month": f"{year}-{m:02d}",
            "invoice_tax": inv_s["total_tax"],
            "credit_note_tax": crd_s["total_tax"],
            "net_tax": round(inv_s["total_tax"] - crd_s["total_tax"], 2),
            "invoice_count": inv_s["count"],
            "credit_note_count": crd_s["count"]
        })

    return {
        "period": f"Q{quarter} {year}",
        "report_type": "Quarterly Tax Report",
        "date_range": f"{date_from} to {date_to}",
        "invoices": inv,
        "credit_notes": crd,
        "net": {
            "total_untaxed": round(inv["total_untaxed"] - crd["total_untaxed"], 2),
            "total_tax": round(inv["total_tax"] - crd["total_tax"], 2),
            "total_amount": round(inv["total_amount"] - crd["total_amount"], 2)
        },
        "monthly_breakdown": monthly
    }

@app.get("/report/monthly-sales")
async def monthly_sales(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"

    invoices, err1 = await fetch_moves("out_invoice", date_from, date_to)
    credits, err2 = await fetch_moves("out_refund", date_from, date_to)
    if err1 or err2:
        return {"error": err1 or err2}

    inv = summarize_moves(invoices)
    crd = summarize_moves(credits)

    return {
        "period": f"{year}-{month:02d}",
        "report_type": "Monthly Sales Report (Commission Base)",
        "note": "Includes paid, in_payment, reversed only",
        "invoices": {
            **inv,
            "detail": [
                {
                    "name": r["name"],
                    "customer": r["partner_id"][1] if r.get("partner_id") else "N/A",
                    "date": r.get("invoice_date", ""),
                    "amount_untaxed": r.get("amount_untaxed", 0),
                    "amount_tax": r.get("amount_tax", 0),
                    "amount_total": r.get("amount_total", 0),
                    "payment_state": r.get("payment_state", "")
                }
                for r in sorted(invoices, key=lambda x: x.get("name", ""))
            ]
        },
        "credit_notes": {
            **crd,
            "detail": [
                {
                    "name": r["name"],
                    "customer": r["partner_id"][1] if r.get("partner_id") else "N/A",
                    "date": r.get("invoice_date", ""),
                    "amount_untaxed": r.get("amount_untaxed", 0),
                    "amount_tax": r.get("amount_tax", 0),
                    "amount_total": r.get("amount_total", 0),
                    "payment_state": r.get("payment_state", "")
                }
                for r in sorted(credits, key=lambda x: x.get("name", ""))
            ]
        },
        "commission_base": {
            "net_sales_excl_tax": round(inv["total_untaxed"] - crd["total_untaxed"], 2),
            "net_sales_incl_tax": round(inv["total_amount"] - crd["total_amount"], 2),
            "net_tax": round(inv["total_tax"] - crd["total_tax"], 2),
            "invoice_count": inv["count"],
            "credit_note_count": crd["count"]
        }
    }

@app.get("/report/missing-tax")
async def missing_tax(year: int, month: int):
    last_day = calendar.monthrange(year, month)[1]
    date_from = f"{year}-{month:02d}-01"
    date_to = f"{year}-{month:02d}-{last_day}"

    result = await odoo_query(
        "account.move",
        [
            ["move_type", "=", "out_invoice"],
            ["state", "=", "posted"],
            ["invoice_date", ">=", date_from],
            ["invoice_date", "<=", date_to],
            ["company_id", "=", 1],
            ["amount_tax", "=", 0],
            ["partner_shipping_id.state_id", "in", [CA_STATE_ID]]
        ],
        ["name", "partner_id", "partner_shipping_id", "invoice_date",
         "amount_untaxed", "amount_tax", "amount_total", "payment_state"],
        limit=2000
    )
    records = json.loads(result)
    if isinstance(records, dict) and "error" in records:
        return records

    return {
        "period": f"{year}-{month:02d}",
        "report_type": "Missing Tax Detection - CA Invoices",
        "total_found": len(records),
        "note": "These CA invoices have $0 tax - please review",
        "invoices": [
            {
                "name": r["name"],
                "customer": r["partner_id"][1] if r.get("partner_id") else "N/A",
                "date": r.get("invoice_date", ""),
                "amount": r.get("amount_untaxed", 0),
                "tax": r.get("amount_tax", 0),
                "total": r.get("amount_total", 0),
                "payment_state": r.get("payment_state", "")
            }
            for r in records
        ]
    }

# ---------- Tools ----------

TOOLS = [
    {
        "name": "odoo_search",
        "description": "Search Odoo data. Common models: sale.order, product.product, res.partner, account.move, repair.order, stock.quant, purchase.order, account.payment. For stock always add [\"location_id.usage\",\"=\",\"internal\"]. For products use ilike on both name and default_code with OR logic.",
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
        "name": "get_monthly_tax",
        "description": "Get accurate monthly tax report. Returns invoice tax, credit note tax, and net tax. Always use for monthly tax queries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer"},
                "month": {"type": "integer"}
            },
            "required": ["year", "month"]
        }
    },
    {
        "name": "get_quarterly_tax",
        "description": "Get accurate quarterly tax report with monthly breakdown. Always use for quarterly tax queries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer"},
                "quarter": {"type": "integer"}
            },
            "required": ["year", "quarter"]
        }
    },
    {
        "name": "get_monthly_sales",
        "description": "Get monthly sales report for commission calculation. Includes paid, in_payment, reversed invoices and credit notes. Always use for sales/commission queries.",
        "input_schema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer"},
                "month": {"type": "integer"}
            },
            "required": ["year", "month"]
        }
    },
    {
        "name": "get_missing_tax",
        "description": "Find CA invoices with zero tax amount. Use when asked about missing tax, invoices without tax, or CA orders that should have tax.",
        "input_schema": {
            "type": "object",
            "properties": {
                "year": {"type": "integer"},
                "month": {"type": "integer"}
            },
            "required": ["year", "month"]
        }
    }
]

SYSTEM = """You are Chumart Assistant, an enterprise AI assistant connected to Odoo 17 ERP system.
You support both English and Chinese - reply in the same language the user uses.

FINANCIAL REPORT RULES (always use dedicated tools for accuracy):
- Monthly tax -> get_monthly_tax
- Quarterly tax -> get_quarterly_tax
- Monthly sales / commission base -> get_monthly_sales
- CA invoices missing tax -> get_missing_tax
- These tools are ALWAYS more accurate than odoo_search for financial reports

GENERAL QUERY RULES:
- Always include date filters when user mentions a time period
- For date ranges: use >= start and <= end
- Invoice types: out_invoice=customer invoice, out_refund=credit note
- Always filter by state=posted unless asked otherwise
- For account.move, sale.order, purchase.order, account.payment, res.partner, crm.lead, repair.order, stock.picking: automatically filtered by company_id=1
- For product.product, stock.quant: do NOT add company_id filter
- For stock queries always add ["location_id.usage","=","internal"]
- For product search use ilike on both name and default_code with OR logic

When showing financial data: use $ with commas, be precise."""

async def run_tool(name, inp):
    if name == "odoo_search":
        model = inp["model"]
        domain = inp.get("domain", [])
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
    if name == "get_monthly_tax":
        result = await monthly_tax(inp["year"], inp["month"])
        return json.dumps(result, ensure_ascii=False)
    if name == "get_quarterly_tax":
        result = await quarterly_tax(inp["year"], inp["quarter"])
        return json.dumps(result, ensure_ascii=False)
    if name == "get_monthly_sales":
        result = await monthly_sales(inp["year"], inp["month"])
        return json.dumps(result, ensure_ascii=False)
    if name == "get_missing_tax":
        result = await missing_tax(inp["year"], inp["month"])
        return json.dumps(result, ensure_ascii=False)
    return "Unknown tool"

# ---------- File extraction ----------

@app.post("/extract-file")
async def extract_file(file: UploadFile = File(...)):
    try:
        content = await file.read()
        filename = file.filename.lower()

        # Plain text files — decode and return as-is
        if filename.endswith(('.txt', '.md', '.csv')):
            return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}

        # PDF — return raw base64 so the chat endpoint passes it directly to Claude
        if filename.endswith('.pdf'):
            b64 = base64.standard_b64encode(content).decode('utf-8')
            return {
                "text": "",
                "name": file.filename,
                "file_b64": b64,
                "file_media_type": "application/pdf"
            }

        # Images — return raw base64
        if filename.endswith(('.png', '.jpg', '.jpeg', '.webp')):
            ext = filename.split('.')[-1].replace('jpg', 'jpeg')
            media_type = f"image/{ext}"
            b64 = base64.standard_b64encode(content).decode('utf-8')
            return {
                "text": "",
                "name": file.filename,
                "file_b64": b64,
                "file_media_type": media_type
            }

        # Fallback
        return {"text": content.decode('utf-8', errors='ignore'), "name": file.filename}

    except Exception as e:
        return {"error": str(e)}

# ---------- Chat ----------

@app.post("/chat")
async def chat(req: ChatRequest):
    # Build the user message content
    # Case 1: PDF or image attached — pass directly to Claude (no text extraction needed)
    if req.file_b64 and req.file_media_type:
        doc_type = "document" if req.file_media_type == "application/pdf" else "image"
        user_message_content = [
            {
                "type": doc_type,
                "source": {
                    "type": "base64",
                    "media_type": req.file_media_type,
                    "data": req.file_b64
                }
            },
            {
                "type": "text",
                "text": f"[Attached file: {req.file_name}]\n\nUser question: {req.message}"
            }
        ]
    # Case 2: Plain text file content (txt/csv/md)
    elif req.file_content and req.file_name:
        user_message_content = (
            f"=== ATTACHED FILE: {req.file_name} ===\n"
            f"{req.file_content}\n"
            f"=== END OF FILE ===\n\n"
            f"User question: {req.message}"
        )
    # Case 3: Normal text message
    else:
        user_message_content = req.message

    messages = req.history + [{"role": "user", "content": user_message_content}]
    headers = {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    async with httpx.AsyncClient(timeout=120) as c:
        current_messages = list(messages)
        for _ in range(5):
            r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                "model": "claude-sonnet-4-5",
                "max_tokens": 4096,
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

# ---------- Health ----------

@app.get("/invoice-stats")
async def invoice_stats(year: int, month: int):
    return await monthly_tax(year, month)

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
