import { useState, useRef, useEffect } from "react";

// 部署后把这里改成你的后端地址，例如：https://your-app.railway.app
const BACKEND_URL = "https://your-backend.railway.app";

export default function EnterpriseAssistant() {
  const [messages, setMessages] = useState([]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [backendUrl, setBackendUrl] = useState(BACKEND_URL);
  const [showSettings, setShowSettings] = useState(false);
  const messagesEndRef = useRef(null);

  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const SUGGESTIONS = [
    "最近有哪些销售订单？",
    "查一下产品库存情况",
    "有哪些未完成的维修工单？",
    "查询客户信息",
  ];

  const sendMessage = async (text) => {
    const msg = text || input.trim();
    if (!msg || loading) return;
    const userMsg = { role: "user", content: msg };
    const history = messages.map((m) => ({ role: m.role, content: m.content }));
    const newMessages = [...messages, { ...userMsg, id: Date.now() }];
    setMessages(newMessages);
    setInput("");
    setLoading(true);

    try {
      const resp = await fetch(`${backendUrl}/chat`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: msg, history }),
      });
      if (!resp.ok) throw new Error(`服务器错误 ${resp.status}`);
      const data = await resp.json();
      setMessages((prev) => [...prev, { role: "assistant", content: data.reply, id: Date.now() }]);
    } catch (e) {
      setMessages((prev) => [...prev, {
        role: "assistant",
        content: `⚠️ 连接失败：${e.message}\n\n请确认后端服务已启动，并在右上角设置中填写正确的后端地址。`,
        id: Date.now(),
        error: true,
      }]);
    } finally {
      setLoading(false);
    }
  };

  const formatContent = (text) => {
    return text.split("\n").map((line, i) => {
      if (line.startsWith("• ") || line.startsWith("- "))
        return <div key={i} style={{ margin: "3px 0", paddingLeft: 8 }}>{line}</div>;
      if (line === "") return <div key={i} style={{ height: 6 }} />;
      return <div key={i} style={{ margin: "1px 0" }}>{line}</div>;
    });
  };

  return (
    <div style={{
      fontFamily: "'Noto Sans SC', 'PingFang SC', sans-serif",
      background: "linear-gradient(135deg, #0f0c29 0%, #1a1a3e 50%, #24243e 100%)",
      minHeight: "100vh",
      display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center",
      padding: 16,
    }}>
      <link href="https://fonts.googleapis.com/css2?family=Noto+Sans+SC:wght@400;500;700&display=swap" rel="stylesheet" />

      <div style={{
        width: "100%", maxWidth: 860,
        background: "rgba(255,255,255,0.04)",
        backdropFilter: "blur(20px)",
        borderRadius: 24,
        border: "1px solid rgba(255,255,255,0.1)",
        overflow: "hidden",
        boxShadow: "0 40px 80px rgba(0,0,0,0.5)",
        display: "flex", flexDirection: "column",
        height: "90vh", maxHeight: 780,
      }}>

        {/* Header */}
        <div style={{
          padding: "18px 24px",
          background: "rgba(255,255,255,0.05)",
          borderBottom: "1px solid rgba(255,255,255,0.08)",
          display: "flex", alignItems: "center", justifyContent: "space-between",
        }}>
          <div style={{ display: "flex", alignItems: "center", gap: 12 }}>
            <div style={{
              width: 40, height: 40, borderRadius: 12,
              background: "linear-gradient(135deg, #667eea, #764ba2)",
              display: "flex", alignItems: "center", justifyContent: "center", fontSize: 18,
            }}>🤖</div>
            <div>
              <div style={{ color: "#fff", fontWeight: 700, fontSize: 16 }}>企业智能助手</div>
              <div style={{ color: "rgba(255,255,255,0.35)", fontSize: 11 }}>已连接 Odoo · 销售 / 库存 / 售后 / 客户</div>
            </div>
          </div>
          <button onClick={() => setShowSettings(!showSettings)} style={{
            padding: "7px 14px", borderRadius: 20, border: "1px solid rgba(255,255,255,0.15)",
            background: "rgba(255,255,255,0.07)", color: "rgba(255,255,255,0.6)",
            fontSize: 12, cursor: "pointer",
          }}>⚙️ 设置</button>
        </div>

        {/* Settings Panel */}
        {showSettings && (
          <div style={{
            padding: "14px 24px",
            background: "rgba(255,255,255,0.04)",
            borderBottom: "1px solid rgba(255,255,255,0.08)",
            display: "flex", gap: 8, alignItems: "center",
          }}>
            <div style={{ color: "rgba(255,255,255,0.5)", fontSize: 12, whiteSpace: "nowrap" }}>后端地址：</div>
            <input
              value={backendUrl}
              onChange={(e) => setBackendUrl(e.target.value)}
              placeholder="https://your-app.railway.app"
              style={{
                flex: 1, padding: "8px 12px",
                background: "rgba(255,255,255,0.07)",
                border: "1px solid rgba(255,255,255,0.15)",
                borderRadius: 8, color: "#fff", fontSize: 12, outline: "none",
              }}
            />
            <button onClick={() => setShowSettings(false)} style={{
              padding: "8px 14px", borderRadius: 8,
              background: "linear-gradient(135deg, #667eea, #764ba2)",
              border: "none", color: "#fff", fontSize: 12, cursor: "pointer",
            }}>保存</button>
          </div>
        )}

        {/* Messages */}
        <div style={{ flex: 1, overflowY: "auto", padding: "20px 24px", display: "flex", flexDirection: "column", gap: 14 }}>
          {messages.length === 0 && (
            <div style={{ flex: 1, display: "flex", flexDirection: "column", alignItems: "center", justifyContent: "center", gap: 16 }}>
              <div style={{ fontSize: 44 }}>🔗</div>
              <div style={{ color: "rgba(255,255,255,0.5)", fontSize: 14, textAlign: "center" }}>
                直接提问，助手会实时从 Odoo 查询数据
              </div>
              <div style={{ display: "flex", gap: 8, flexWrap: "wrap", justifyContent: "center" }}>
                {SUGGESTIONS.map((q) => (
                  <button key={q} onClick={() => sendMessage(q)} style={{
                    padding: "8px 16px", borderRadius: 20,
                    border: "1px solid rgba(102,126,234,0.4)",
                    background: "rgba(102,126,234,0.1)",
                    color: "rgba(255,255,255,0.7)", fontSize: 13, cursor: "pointer",
                  }}>{q}</button>
                ))}
              </div>
            </div>
          )}

          {messages.map((msg) => (
            <div key={msg.id} style={{
              display: "flex",
              justifyContent: msg.role === "user" ? "flex-end" : "flex-start",
              gap: 10, alignItems: "flex-start",
            }}>
              {msg.role === "assistant" && (
                <div style={{
                  width: 32, height: 32, borderRadius: 10,
                  background: "linear-gradient(135deg, #667eea, #764ba2)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 14, flexShrink: 0, marginTop: 2,
                }}>🤖</div>
              )}
              <div style={{
                maxWidth: "75%", padding: "12px 16px",
                borderRadius: msg.role === "user" ? "18px 18px 4px 18px" : "18px 18px 18px 4px",
                background: msg.role === "user"
                  ? "linear-gradient(135deg, #667eea, #764ba2)"
                  : msg.error ? "rgba(255,80,80,0.12)" : "rgba(255,255,255,0.08)",
                border: msg.role === "assistant" ? `1px solid ${msg.error ? "rgba(255,80,80,0.3)" : "rgba(255,255,255,0.1)"}` : "none",
                color: "#fff", fontSize: 14, lineHeight: 1.75,
              }}>
                {msg.role === "assistant" ? formatContent(msg.content) : msg.content}
              </div>
              {msg.role === "user" && (
                <div style={{
                  width: 32, height: 32, borderRadius: 10,
                  background: "rgba(255,255,255,0.12)",
                  display: "flex", alignItems: "center", justifyContent: "center",
                  fontSize: 14, flexShrink: 0, marginTop: 2,
                }}>👤</div>
              )}
            </div>
          ))}

          {loading && (
            <div style={{ display: "flex", gap: 10, alignItems: "flex-start" }}>
              <div style={{
                width: 32, height: 32, borderRadius: 10,
                background: "linear-gradient(135deg, #667eea, #764ba2)",
                display: "flex", alignItems: "center", justifyContent: "center", fontSize: 14,
              }}>🤖</div>
              <div style={{
                padding: "14px 18px",
                background: "rgba(255,255,255,0.08)",
                borderRadius: "18px 18px 18px 4px",
                border: "1px solid rgba(255,255,255,0.1)",
                display: "flex", gap: 6, alignItems: "center",
              }}>
                {[0,1,2].map((i) => (
                  <div key={i} style={{
                    width: 7, height: 7, borderRadius: "50%",
                    background: "rgba(102,126,234,0.8)",
                    animation: "pulse 1.2s ease-in-out infinite",
                    animationDelay: `${i * 0.2}s`,
                  }} />
                ))}
                <span style={{ color: "rgba(255,255,255,0.35)", fontSize: 12, marginLeft: 4 }}>查询 Odoo 中…</span>
              </div>
            </div>
          )}
          <div ref={messagesEndRef} />
        </div>

        {/* Input */}
        <div style={{ padding: "14px 24px 20px", borderTop: "1px solid rgba(255,255,255,0.08)", display: "flex", gap: 10 }}>
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && !e.shiftKey && sendMessage()}
            placeholder="输入问题，按 Enter 发送…"
            disabled={loading}
            style={{
              flex: 1, padding: "12px 18px",
              background: "rgba(255,255,255,0.07)",
              border: "1px solid rgba(255,255,255,0.12)",
              borderRadius: 14, color: "#fff", fontSize: 14, outline: "none",
            }}
          />
          <button onClick={() => sendMessage()} disabled={loading || !input.trim()} style={{
            padding: "12px 22px",
            background: "linear-gradient(135deg, #667eea, #764ba2)",
            border: "none", borderRadius: 14,
            color: "#fff", fontSize: 14, fontWeight: 600,
            cursor: loading || !input.trim() ? "not-allowed" : "pointer",
            opacity: loading || !input.trim() ? 0.5 : 1,
          }}>发送</button>
        </div>
      </div>

      <style>{`
        @keyframes pulse {
          0%, 80%, 100% { opacity: 0.3; transform: scale(0.8); }
          40% { opacity: 1; transform: scale(1); }
        }
        input::placeholder { color: rgba(255,255,255,0.25); }
        ::-webkit-scrollbar { width: 4px; }
        ::-webkit-scrollbar-track { background: transparent; }
        ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.15); border-radius: 2px; }
      `}</style>
    </div>
  );
}
