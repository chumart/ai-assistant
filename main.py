"""
企业AI助手 - 后端服务
连接 Claude AI + Odoo.sh
"""

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import json
import os
from typing import Optional

app = FastAPI()

# 允许前端跨域访问
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ========== 配置区域（部署时填写）==========
ODOO_URL = os.getenv("ODOO_URL", "https://yourcompany.odoo.com")
ODOO_DB = os.getenv("ODOO_DB", "yourcompany")
ODOO_USERNAME = os.getenv("ODOO_USERNAME", "admin@yourcompany.com")
ODOO_PASSWORD = os.getenv("ODOO_PASSWORD", "your_password")
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", "")
# ==========================================


class ChatRequest(BaseModel):
    message: str
    history: list = []


# ========== Odoo 查询工具 ==========

async def odoo_call(model: str, method: str, args: list, kwargs: dict = {}) -> dict:
    """通用 Odoo JSON-RPC 调用"""
    # 先登录获取 uid
    async with httpx.AsyncClient(timeout=15) as client:
        login_resp = await client.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 1,
            "params": {
                "model": "res.users",
                "method": "authenticate",
                "args": [ODOO_DB, ODOO_USERNAME, ODOO_PASSWORD, {}],
                "kwargs": {}
            }
        })
        uid = login_resp.json().get("result")
        if not uid:
            raise Exception("Odoo 登录失败，请检查账号密码")

        resp = await client.post(f"{ODOO_URL}/web/dataset/call_kw", json={
            "jsonrpc": "2.0", "method": "call", "id": 2,
            "params": {
                "model": model,
                "method": method,
                "args": [[ODOO_DB, uid, ODOO_PASSWORD]] + args,
                "kwargs": kwargs
            }
        })
        return resp.json().get("result", [])


async def search_sales_orders(keyword: str = "", limit: int = 5) -> str:
    """查询销售订单 / 报价单"""
    domain = []
    if keyword:
        domain = ["|", ["name", "ilike", keyword], ["partner_id.name", "ilike", keyword]]
    try:
        records = await odoo_call("sale.order", "search_read", [domain], {
            "fields": ["name", "partner_id", "state", "amount_total", "date_order", "user_id"],
            "limit": limit,
            "order": "date_order desc"
        })
        if not records:
            return "没有找到相关销售订单。"
        result = []
        state_map = {"draft": "报价中", "sent": "已发送", "sale": "销售订单", "done": "已完成", "cancel": "已取消"}
        for r in records:
            result.append(
                f"• 单号：{r['name']} | 客户：{r['partner_id'][1]} | "
                f"状态：{state_map.get(r['state'], r['state'])} | "
                f"金额：{r['amount_total']:,.2f} | 日期：{str(r['date_order'])[:10]}"
            )
        return "\n".join(result)
    except Exception as e:
        return f"查询订单失败：{str(e)}"


async def search_products(keyword: str = "", limit: int = 5) -> str:
    """查询产品和库存"""
    domain = [["sale_ok", "=", True]]
    if keyword:
        domain.append(["name", "ilike", keyword])
    try:
        records = await odoo_call("product.product", "search_read", [domain], {
            "fields": ["name", "default_code", "list_price", "qty_available", "virtual_available", "categ_id"],
            "limit": limit
        })
        if not records:
            return "没有找到相关产品。"
        result = []
        for r in records:
            result.append(
                f"• {r['name']} [{r.get('default_code') or '无编码'}] | "
                f"售价：{r['list_price']:,.2f} | "
                f"现有库存：{r['qty_available']} | "
                f"预计库存：{r['virtual_available']}"
            )
        return "\n".join(result)
    except Exception as e:
        return f"查询产品失败：{str(e)}"


async def search_customers(keyword: str = "", limit: int = 5) -> str:
    """查询客户信息"""
    domain = [["customer_rank", ">", 0]]
    if keyword:
        domain.append(["name", "ilike", keyword])
    try:
        records = await odoo_call("res.partner", "search_read", [domain], {
            "fields": ["name", "phone", "email", "city", "street", "customer_rank"],
            "limit": limit
        })
        if not records:
            return "没有找到相关客户。"
        result = []
        for r in records:
            result.append(
                f"• {r['name']} | 电话：{r.get('phone') or '无'} | "
                f"邮箱：{r.get('email') or '无'} | "
                f"城市：{r.get('city') or '无'}"
            )
        return "\n".join(result)
    except Exception as e:
        return f"查询客户失败：{str(e)}"


async def search_repairs(keyword: str = "", limit: int = 5) -> str:
    """查询售后/维修工单"""
    domain = []
    if keyword:
        domain = ["|", ["name", "ilike", keyword], ["partner_id.name", "ilike", keyword]]
    try:
        records = await odoo_call("repair.order", "search_read", [domain], {
            "fields": ["name", "partner_id", "product_id", "state", "date_planned_start"],
            "limit": limit,
            "order": "id desc"
        })
        if not records:
            return "没有找到相关维修工单。"
        result = []
        state_map = {
            "draft": "草稿", "confirmed": "已确认", "under_repair": "维修中",
            "done": "已完成", "cancel": "已取消", "2binvoiced": "待开票"
        }
        for r in records:
            result.append(
                f"• 工单：{r['name']} | 客户：{r['partner_id'][1] if r.get('partner_id') else '无'} | "
                f"产品：{r['product_id'][1] if r.get('product_id') else '无'} | "
                f"状态：{state_map.get(r['state'], r['state'])}"
            )
        return "\n".join(result)
    except Exception as e:
        return f"查询维修工单失败：{str(e)}"


# ========== Claude AI 工具定义 ==========

TOOLS = [
    {
        "name": "search_sales_orders",
        "description": "查询销售订单或报价单。可以按客户名、订单号等关键词搜索。",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "搜索关键词，如客户名或订单号，留空查最近订单"},
                "limit": {"type": "integer", "description": "返回条数，默认5条", "default": 5}
            }
        }
    },
    {
        "name": "search_products",
        "description": "查询产品信息和库存数量。",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "产品名称或编码关键词"},
                "limit": {"type": "integer", "description": "返回条数，默认5条", "default": 5}
            }
        }
    },
    {
        "name": "search_customers",
        "description": "查询客户信息，包括联系方式和地址。",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "客户名称关键词"},
                "limit": {"type": "integer", "description": "返回条数，默认5条", "default": 5}
            }
        }
    },
    {
        "name": "search_repairs",
        "description": "查询售后维修工单状态。",
        "input_schema": {
            "type": "object",
            "properties": {
                "keyword": {"type": "string", "description": "工单号或客户名关键词"},
                "limit": {"type": "integer", "description": "返回条数，默认5条", "default": 5}
            }
        }
    }
]

SYSTEM_PROMPT = """你是公司的内部AI助手，可以实时查询 Odoo 系统中的数据。

你能查询：
- 销售订单和报价单
- 产品信息和库存
- 客户信息
- 售后维修工单

当员工提问时，判断需要查询哪些数据，调用对应工具获取后，用清晰友好的中文回答。
如果问题不需要查询数据，直接回答即可。"""


async def call_tool(tool_name: str, tool_input: dict) -> str:
    if tool_name == "search_sales_orders":
        return await search_sales_orders(**tool_input)
    elif tool_name == "search_products":
        return await search_products(**tool_input)
    elif tool_name == "search_customers":
        return await search_customers(**tool_input)
    elif tool_name == "search_repairs":
        return await search_repairs(**tool_input)
    return "未知工具"


# ========== 主接口 ==========

@app.post("/chat")
async def chat(req: ChatRequest):
    messages = req.history + [{"role": "user", "content": req.message}]

    async with httpx.AsyncClient(timeout=60) as client:
        # 第一次调用 Claude（可能会调用工具）
        resp = await client.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json"
            },
            json={
                "model": "claude-sonnet-4-20250514",
                "max_tokens": 1024,
                "system": SYSTEM_PROMPT,
                "tools": TOOLS,
                "messages": messages
            }
        )
        data = resp.json()

        # 如果 Claude 需要调用工具
        if data.get("stop_reason") == "tool_use":
            tool_results = []
            for block in data["content"]:
                if block["type"] == "tool_use":
                    result = await call_tool(block["name"], block["input"])
                    tool_results.append({
                        "type": "tool_result",
                        "tool_use_id": block["id"],
                        "content": result
                    })

            # 把工具结果发回给 Claude 生成最终回答
            messages_with_tools = messages + [
                {"role": "assistant", "content": data["content"]},
                {"role": "user", "content": tool_results}
            ]
            resp2 = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": ANTHROPIC_API_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json"
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 1024,
                    "system": SYSTEM_PROMPT,
                    "tools": TOOLS,
                    "messages": messages_with_tools
                }
            )
            data = resp2.json()

        reply = "".join(b.get("text", "") for b in data.get("content", []) if b.get("type") == "text")
if not reply:
    print("DEBUG response:", data)
return {"reply": reply}


@app.get("/health")
async def health():
    return {"status": "ok"}
