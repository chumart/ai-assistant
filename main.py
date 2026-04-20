from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import json

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

# ---------- Universal Odoo query ----------

async def odoo_query(model, domain, fields, limit=20, order="id desc"):
    """One function to query ANY Odoo model"""
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
            result = data.get("result", [])
            return json.dumps(result[:limit], default=str, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

async def odoo_list_fields(model):
    """Get available fields for a model"""
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
                    "kwargs": {"attributes": ["string", "type", "help"]}
                }
            }, cookies=cookies)
            data = r.json()
            if data.get("error"):
                return json.dumps({"error": data["error"].get("message","")})
            fields = data.get("result", {})
            # Return simplified field list
            summary = {k: {"label": v.get("string",""), "type": v.get("type","")} 
                       for k, v in fields.items() 
                       if v.get("type") in ["char","integer","float","monetary","date","datetime","boolean","many2one","selection","text"]}
            return json.dumps(summary, ensure_ascii=False)
    except Exception as e:
        return json.dumps({"error": str(e)})

# ---------- Tools ----------

TOOLS = [
    {
        "name": "odoo_search",
        "description": """Search ANY Odoo model. Common models:
- sale.order: Sales orders (fields: name, partner_id, state, amount_total, date_order, user_id)
- product.product: Products (fields: name, default_code, list_price, qty_available, virtual_available, barcode, categ_id)
- res.partner: Customers/contacts (fields: name, phone, email, city, street, country_id, customer_rank)
- account.move: Invoices (fields: name, partner_id, move_type, state, invoice_date, amount_untaxed, amount_tax, amount_total, payment_state)
- repair.order: Repairs (fields: name, partner_id, product_id, state)
- stock.quant: Stock levels (fields: product_id, location_id, quantity, reserved_quantity)
- purchase.order: Purchase orders (fields: name, partner_id, state, amount_total, date_order)
- stock.picking: Deliveries/receipts (fields: name, partner_id, state, scheduled_date, origin)
- account.payment: Payments (fields: name, partner_id, amount, date, state, payment_type)
- crm.lead: CRM leads/opportunities (fields: name, partner_id, expected_revenue, stage_id, user_id)
- hr.employee: Employees (fields: name, job_title, department_id, work_email)

Domain filter examples:
- [["state","=","sale"]] — confirmed sales orders
- [["invoice_date",">=","2025-01-01"],["invoice_date","<","2025-04-01"]] — Q1 2025 invoices
- [["move_type","in",["out_invoice","out_refund"]],["state","=","posted"]] — posted customer invoices
- [["default_code","ilike","PLM"]] — products with code containing PLM
- ["|",["name","ilike","keyword"],["default_code","ilike","keyword"]] — search name OR code""",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string", "description": "Odoo model name, e.g. sale.order, product.product, account.move"},
                "domain": {"type": "array", "description": "Search filter in Odoo domain format, e.g. [['state','=','sale']]"},
                "fields": {"type": "array", "items": {"type": "string"}, "description": "Fields to return"},
                "limit": {"type": "integer", "description": "Max records to return, default 20"},
                "order": {"type": "string", "description": "Sort order, e.g. 'date_order desc', 'amount_total desc'"}
            },
            "required": ["model", "domain", "fields"]
        }
    },
    {
        "name": "odoo_fields",
        "description": "List available fields for any Odoo model. Use this when you need to know what fields exist before querying.",
        "input_schema": {
            "type": "object",
            "properties": {
                "model": {"type": "string", "description": "Odoo model name, e.g. sale.order"}
            },
            "required": ["model"]
        }
    }
]

SYSTEM = """You are an enterprise AI assistant connected to Odoo 17 ERP system.
You can query ANY data in Odoo using the odoo_search tool.
You support both English and Chinese — reply in the same language the user uses.

When querying data:
- Use appropriate domain filters to get exactly what's needed
- For invoices/tax queries, filter by move_type and state
- For date ranges, use >= and < operators
- You can aggregate results (sum, count, average) from the returned data
- If unsure about field names, use odoo_fields first to check

Be concise, professional, and present data in a clear format.
When showing financial data, always format numbers with commas and currency symbols."""

async def run_tool(name, inp):
    if name == "odoo_search":
        return await odoo_query(
            inp["model"], inp["domain"], inp["fields"],
            inp.get("limit", 20), inp.get("order", "id desc"))
    if name == "odoo_fields":
        return await odoo_list_fields(inp["model"])
    return "Unknown tool"

# ---------- Chat ----------

@app.post("/chat")
async def chat(req: ChatRequest):
    messages = req.history + [{"role": "user", "content": req.message}]
    headers = {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }
    
    async with httpx.AsyncClient(timeout=90) as c:
        # Allow multiple tool call rounds (AI might need to check fields first, then query)
        current_messages = list(messages)
        
        for attempt in range(4):  # max 4 rounds of tool use
            r = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                "model": "claude-haiku-4-5-20251001",
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
                continue  # let AI process results and possibly call more tools
            else:
                break  # AI is done, has final text response
        
        reply = "".join(b.get("text","") for b in d.get("content",[]) if b.get("type")=="text")
        return {"reply": reply or "Sorry, no response generated."}

# ---------- Test & Health ----------

@app.get("/test-odoo")
async def test_odoo():
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {"db": ODOO_DB, "login": ODOO_USERNAME, "password": ODOO_PASSWORD}
            })
            return {"status": r.status_code, "url": str(r.url), "content_type": r.headers.get("content-type",""), "body_start": r.text[:500]}
    except Exception as e:
        return {"error": str(e)}

@app.get("/health")
async def health():
    return {"status": "ok"}
