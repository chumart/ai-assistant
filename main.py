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

# ---------- Odoo ----------

async def odoo_uid():
    """Login and get uid using XML-RPC style JSON-RPC"""
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {
                "model": "res.users",
                "method": "authenticate",
                "args": [ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {}],
                "kwargs": {}
            }
        })
        data = r.json()
        print("Login response:", json.dumps(data)[:200])
        return data.get("result")

async def odoo_read(uid, model, domain, fields, limit=8, order="id desc"):
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 2,
            "params": {
                "model": model,
                "method": "search_read",
                "args": [domain],
                "kwargs": {
                    "fields": fields,
                    "limit": limit,
                    "order": order,
                    "context": {"uid": uid, "db": ODOO_DB, "password": ODOO_PASSWORD}
                }
            }
        })
        data = r.json()
        print(f"Query {model}:", json.dumps(data)[:300])
        return data.get("result", [])

async def odoo_call(uid, model, domain, fields, limit=8):
    """Use session-based auth"""
    async with httpx.AsyncClient(timeout=20) as c:
        # First establish session
        login_r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {
                "db": ODOO_DB,
                "login": ODOO_USERNAME,
                "password": ODOO_PASSWORD
            }
        })
        session_data = login_r.json()
        print("Session login:", json.dumps(session_data)[:200])
        
        if session_data.get("result", {}).get("uid"):
            # Now query
            cookies = login_r.cookies
            r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", 
                json={
                    "jsonrpc": "2.0", "method": "call", "id": 2,
                    "params": {
                        "model": model,
                        "method": "search_read",
                        "args": [domain],
                        "kwargs": {"fields": fields, "limit": limit, "order": "id desc"}
                    }
                },
                cookies=cookies
            )
            result = r.json()
            print(f"Query result:", json.dumps(result)[:300])
            return result.get("result", [])
        return []

async def get_sales(keyword=""):
    try:
        domain = ["|", ["name","ilike",keyword], ["partner_id.name","ilike",keyword]] if keyword else []
        rows = await odoo_call(None, "sale.order", domain,
            ["name","partner_id","state","amount_total","date_order"])
        if not rows: return "No sales orders found."
        states = {"draft":"Quote","sent":"Sent","sale":"Confirmed","done":"Done","cancel":"Cancelled"}
        return "\n".join(
            f"• {r['name']} | {r['partner_id'][1] if r.get('partner_id') else 'N/A'} | "
            f"{states.get(r['state'], r['state'])} | ${r.get('amount_total',0):,.2f} | "
            f"{str(r.get('date_order',''))[:10]}"
            for r in rows)
    except Exception as e:
        return f"Error: {e}"

async def get_products(keyword=""):
    try:
        domain = [["name","ilike",keyword]] if keyword else [["sale_ok","=",True]]
        rows = await odoo_call(None, "product.product", domain,
            ["name","default_code","list_price","qty_available"])
        if not rows: return "No products found."
        return "\n".join(
            f"• {r['name']} [{r.get('default_code') or 'N/A'}] | "
            f"${r.get('list_price',0):,.2f} | Stock: {r.get('qty_available',0)}"
            for r in rows)
    except Exception as e:
        return f"Error: {e}"

async def get_customers(keyword=""):
    try:
        domain = [["customer_rank",">",0]]
        if keyword: domain.append(["name","ilike",keyword])
        rows = await odoo_call(None, "res.partner", domain,
            ["name","phone","email","city"])
        if not rows: return "No customers found."
        return "\n".join(
            f"• {r['name']} | {r.get('phone') or 'N/A'} | "
            f"{r.get('email') or 'N/A'} | {r.get('city') or 'N/A'}"
            for r in rows)
    except Exception as e:
        return f"Error: {e}"

async def get_repairs(keyword=""):
    try:
        domain = ["|", ["name","ilike",keyword], ["partner_id.name","ilike",keyword]] if keyword else []
        rows = await odoo_call(None, "repair.order", domain,
            ["name","partner_id","product_id","state"])
        if not rows: return "No repair orders found."
        states = {"draft":"Draft","confirmed":"Confirmed","under_repair":"In Repair","done":"Done","cancel":"Cancelled"}
        return "\n".join(
            f"• {r['name']} | {r['partner_id'][1] if r.get('partner_id') else 'N/A'} | "
            f"{r['product_id'][1] if r.get('product_id') else 'N/A'} | "
            f"{states.get(r['state'], r['state'])}"
            for r in rows)
    except Exception as e:
        return f"Error: {e}"

# ---------- Tools ----------

TOOLS = [
    {"name": "get_sales_orders", "description": "Get recent sales orders or quotes from Odoo.",
     "input_schema": {"type": "object", "properties": {"keyword": {"type": "string", "description": "Optional customer name or order number"}}}},
    {"name": "get_products", "description": "Get product list and stock levels from Odoo.",
     "input_schema": {"type": "object", "properties": {"keyword": {"type": "string", "description": "Optional product name or code"}}}},
    {"name": "get_customers", "description": "Get customer contact information from Odoo.",
     "input_schema": {"type": "object", "properties": {"keyword": {"type": "string", "description": "Optional customer name"}}}},
    {"name": "get_repair_orders", "description": "Get repair / after-sales service orders from Odoo.",
     "input_schema": {"type": "object", "properties": {"keyword": {"type": "string", "description": "Optional keyword"}}}}
]

SYSTEM = """You are an enterprise AI assistant connected to Odoo ERP.
You support both English and Chinese — always reply in the same language the user writes in.
Use tools to look up real-time Odoo data when asked. Be concise and professional."""

async def run_tool(name, inp):
    kw = inp.get("keyword", "")
    if name == "get_sales_orders":  return await get_sales(kw)
    if name == "get_products":      return await get_products(kw)
    if name == "get_customers":     return await get_customers(kw)
    if name == "get_repair_orders": return await get_repairs(kw)
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
    async with httpx.AsyncClient(timeout=60) as c:
        r1 = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
            "model": "claude-haiku-4-5-20251001",
            "max_tokens": 1024,
            "system": SYSTEM,
            "tools": TOOLS,
            "messages": messages
        })
        d1 = r1.json()
        if "error" in d1:
            return {"reply": f"API error: {d1['error'].get('message', str(d1['error']))}"}

        if d1.get("stop_reason") == "tool_use":
            tool_results = []
            for block in d1.get("content", []):
                if block.get("type") == "tool_use":
                    result = await run_tool(block["name"], block.get("input", {}))
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": result
                    })
            r2 = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 1024,
                "system": SYSTEM,
                "tools": TOOLS,
                "messages": messages + [
                    {"role": "assistant", "content": d1["content"]},
                    {"role": "user", "content": tool_results}
                ]
            })
            d1 = r2.json()

        reply = "".join(b.get("text","") for b in d1.get("content",[]) if b.get("type")=="text")
        return {"reply": reply or "Sorry, no response generated."}

@app.get("/test-odoo")
async def test_odoo():
    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
            r = await c.post(f"{ODOO_URL}/web/session/authenticate", json={
                "jsonrpc": "2.0", "method": "call", "id": 1,
                "params": {
                    "db": ODOO_DB,
                    "login": ODOO_USERNAME,
                    "password": ODOO_PASSWORD
                }
            })
            return {
                "status": r.status_code,
                "url": str(r.url),
                "content_type": r.headers.get("content-type", ""),
                "body_start": r.text[:500]
            }
    except Exception as e:
        return {"error": str(e)}
@app.get("/health")
async def health():
    return {"status": "ok"}
