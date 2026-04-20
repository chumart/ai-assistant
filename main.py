from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

ODOO_URL      = os.getenv("ODOO_URL", "")
ODOO_DB       = os.getenv("ODOO_DB", "")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

class ChatRequest(BaseModel):
    message: str
    history: list = []

# ---------- Odoo helpers ----------

async def odoo_uid():
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {
                "model": "res.users", "method": "authenticate",
                "args": [ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {}],
                "kwargs": {}
            }
        })
        return r.json().get("result")

async def odoo_search(uid, model, domain, fields, limit=5, order="id desc"):
    async with httpx.AsyncClient(timeout=15) as c:
        r = await c.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 2,
            "params": {
                "model": model, "method": "search_read",
                "args": [[ODOO_DB, uid, ODOO_PASSWORD], domain],
                "kwargs": {"fields": fields, "limit": limit, "order": order}
            }
        })
        return r.json().get("result", [])

async def get_sales(keyword=""):
    try:
        uid = await odoo_uid()
        if not uid:
            return "Odoo login failed — check credentials."
        domain = ["|", ["name","ilike",keyword], ["partner_id.name","ilike",keyword]] if keyword else []
        rows = await odoo_search(uid, "sale.order", domain,
            ["name","partner_id","state","amount_total","date_order"], limit=8)
        if not rows: return "No sales orders found."
        states = {"draft":"Quote","sent":"Sent","sale":"Order","done":"Done","cancel":"Cancelled"}
        return "\n".join(
            f"• {r['name']} | {r['partner_id'][1]} | {states.get(r['state'],r['state'])} | ${r['amount_total']:,.2f} | {str(r['date_order'])[:10]}"
            for r in rows)
    except Exception as e:
        return f"Error querying sales: {e}"

async def get_products(keyword=""):
    try:
        uid = await odoo_uid()
        if not uid: return "Odoo login failed."
        domain = [["name","ilike",keyword]] if keyword else [["sale_ok","=",True]]
        rows = await odoo_search(uid, "product.product", domain,
            ["name","default_code","list_price","qty_available"], limit=8)
        if not rows: return "No products found."
        return "\n".join(
            f"• {r['name']} [{r.get('default_code') or 'N/A'}] | Price: ${r['list_price']:,.2f} | Stock: {r['qty_available']}"
            for r in rows)
    except Exception as e:
        return f"Error querying products: {e}"

async def get_customers(keyword=""):
    try:
        uid = await odoo_uid()
        if not uid: return "Odoo login failed."
        domain = [["customer_rank",">",0]]
        if keyword: domain.append(["name","ilike",keyword])
        rows = await odoo_search(uid, "res.partner", domain,
            ["name","phone","email","city"], limit=8)
        if not rows: return "No customers found."
        return "\n".join(
            f"• {r['name']} | {r.get('phone') or 'N/A'} | {r.get('email') or 'N/A'} | {r.get('city') or 'N/A'}"
            for r in rows)
    except Exception as e:
        return f"Error querying customers: {e}"

async def get_repairs(keyword=""):
    try:
        uid = await odoo_uid()
        if not uid: return "Odoo login failed."
        domain = ["|", ["name","ilike",keyword], ["partner_id.name","ilike",keyword]] if keyword else []
        rows = await odoo_search(uid, "repair.order", domain,
            ["name","partner_id","product_id","state"], limit=8)
        if not rows: return "No repair orders found."
        states = {"draft":"Draft","confirmed":"Confirmed","under_repair":"In Repair","done":"Done","cancel":"Cancelled"}
        return "\n".join(
            f"• {r['name']} | {r['partner_id'][1] if r.get('partner_id') else 'N/A'} | {r['product_id'][1] if r.get('product_id') else 'N/A'} | {states.get(r['state'],r['state'])}"
            for r in rows)
    except Exception as e:
        return f"Error querying repairs: {e}"

# ---------- Tool definitions ----------

TOOLS = [
    {
        "name": "get_sales_orders",
        "description": "Get recent sales orders or quotes from Odoo. Can filter by customer name or order number.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Optional search keyword (customer name or order number)"}
            }
        }
    },
    {
        "name": "get_products",
        "description": "Get product list and inventory levels from Odoo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Optional product name or code to search"}
            }
        }
    },
    {
        "name": "get_customers",
        "description": "Get customer information including contact details.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Optional customer name to search"}
            }
        }
    },
    {
        "name": "get_repair_orders",
        "description": "Get repair / after-sales service orders from Odoo.",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "Optional keyword to filter repairs"}
            }
        }
    }
]

SYSTEM = """You are an enterprise AI assistant connected to Odoo. You can query real-time data.
You support both English and Chinese — always reply in the same language the user writes in.
Use the available tools to look up data before answering. Be concise and professional."""

async def run_tool(name, inp):
    kw = inp.get("keyword", "")
    if name == "get_sales_orders":  return await get_sales(kw)
    if name == "get_products":      return await get_products(kw)
    if name == "get_customers":     return await get_customers(kw)
    if name == "get_repair_orders": return await get_repairs(kw)
    return "Unknown tool"

# ---------- Chat endpoint ----------

@app.post("/chat")
async def chat(req: ChatRequest):
    messages = req.history + [{"role": "user", "content": req.message}]
    headers = {
        "x-api-key": ANTHROPIC_KEY,
        "anthropic-version": "2023-06-01",
        "content-type": "application/json"
    }

    async with httpx.AsyncClient(timeout=60) as c:
        # First call
        r1 = await c.post("https://api.anthropic.com/v1/messages", headers=headers, json={
            "model": "claude-sonnet-4-5",
            "max_tokens": 1024,
            "system": SYSTEM,
            "tools": TOOLS,
            "messages": messages
        })
        d1 = r1.json()
        print("API response:", json.dumps(d1)[:500])

        # If error
        if "error" in d1:
            return {"reply": f"API error: {d1['error'].get('message', str(d1['error']))}"}

        # If tool use needed
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
                "model": "claude-sonnet-4-5",
                "max_tokens": 1024,
                "system": SYSTEM,
                "tools": TOOLS,
                "messages": messages + [
                    {"role": "assistant", "content": d1["content"]},
                    {"role": "user", "content": tool_results}
                ]
            })
            d1 = r2.json()
            print("Final response:", json.dumps(d1)[:500])

        reply = "".join(b.get("text","") for b in d1.get("content",[]) if b.get("type")=="text")
        return {"reply": reply or "Sorry, I could not generate a response."}

@app.get("/health")
async def health():
    return {"status": "ok", "odoo": ODOO_URL}
