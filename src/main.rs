//! haak-acp-bridge — Bridges haakd's WebSocket to ACP protocol on stdio.
//!
//! Usage:
//!   haak-acp-bridge [--url ws://localhost:5201] [--agent bala] [--db /path/to/haak.db]

use agent_client_protocol::{
    self as acp,
    on_receive_dispatch, on_receive_notification, on_receive_request,
    schema::{
        AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, InitializeRequest,
        InitializeResponse, ListSessionsRequest, ListSessionsResponse, MaybeUndefined,
        NewSessionRequest, NewSessionResponse, PromptCapabilities, PromptRequest, PromptResponse,
        ProtocolVersion, ResumeSessionRequest, ResumeSessionResponse, SessionCapabilities,
        SessionId, SessionInfo, SessionInfoUpdate, SessionListCapabilities, SessionMode,
        SessionModeId, SessionModeState, SessionNotification, SessionResumeCapabilities,
        PermissionOption, PermissionOptionKind, RequestPermissionOutcome, RequestPermissionRequest,
        SessionConfigKind, SessionConfigOption, SessionConfigSelect, SessionConfigSelectOption,
        SessionUpdate, SetSessionConfigOptionRequest, SetSessionConfigOptionResponse,
        SetSessionModeRequest, SetSessionModeResponse, StopReason, ToolCall, ToolCallStatus,
        ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
    },
    Agent, ConnectionTo, Dispatch, Stdio,
};
use futures::channel::mpsc;
use futures::StreamExt;
use serde_json::{json, Value};
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use tungstenite::{connect, Message};

struct Bridge {
    url: String,
    db_path: Option<PathBuf>,
    ws_tx: StdMutex<Option<std::sync::mpsc::Sender<String>>>,
    ws_rx: StdMutex<Option<mpsc::UnboundedReceiver<String>>>,
    haakd_session_id: StdMutex<Option<String>>,
    current_model: StdMutex<String>,
    current_agent: StdMutex<String>,
}

impl Bridge {
    fn new(url: &str, agent: &str, db_path: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            url: url.to_string(),
            db_path,
            ws_tx: StdMutex::new(None),
            ws_rx: StdMutex::new(None),
            haakd_session_id: StdMutex::new(None),
            current_model: StdMutex::new("sonnet".to_string()),
            current_agent: StdMutex::new(agent.to_string()),
        })
    }

    fn connect(&self) -> anyhow::Result<()> {
        let (event_tx, event_rx) = mpsc::unbounded::<String>();
        let (write_tx, write_rx) = std::sync::mpsc::channel::<String>();
        let ws_url = format!("{}/ws/session", self.url.replace("http", "ws"));

        std::thread::spawn(move || {
            eprintln!("[ws-thread] connecting to {ws_url}");
            let (mut ws, _) = match connect(&ws_url) {
                Ok(pair) => { eprintln!("[ws-thread] connected"); pair }
                Err(e) => {
                    eprintln!("[ws-thread] FAILED: {e}");
                    let _ = event_tx.unbounded_send(json!({"type":"error","message":format!("{e}")}).to_string());
                    return;
                }
            };
            if let tungstenite::stream::MaybeTlsStream::Plain(s) = ws.get_ref() {
                let _ = s.set_nonblocking(true);
            }
            loop {
                match ws.read() {
                    Ok(msg) if msg.is_text() => {
                        let text = msg.to_text().map(|s| s.to_string()).unwrap_or_default();
                        if event_tx.unbounded_send(text).is_err() { break; }
                    }
                    Ok(msg) if msg.is_close() => break,
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => break,
                    _ => {}
                }
                while let Ok(payload) = write_rx.try_recv() {
                    let _ = ws.send(Message::text(payload));
                }
                std::thread::sleep(std::time::Duration::from_millis(16));
            }
        });

        *self.ws_tx.lock().unwrap() = Some(write_tx);
        *self.ws_rx.lock().unwrap() = Some(event_rx);
        Ok(())
    }

    fn send(&self, msg: &str) {
        if let Some(tx) = self.ws_tx.lock().unwrap().as_ref() {
            let _ = tx.send(msg.to_string());
        }
    }

    fn take_rx(&self) -> Option<mpsc::UnboundedReceiver<String>> { self.ws_rx.lock().unwrap().take() }
    fn put_rx(&self, rx: mpsc::UnboundedReceiver<String>) { *self.ws_rx.lock().unwrap() = Some(rx); }

    /// Query haak.db for recent sessions
    fn list_sessions(&self) -> Vec<SessionInfo> {
        let Some(db_path) = &self.db_path else { return Vec::new() };
        let Ok(conn) = rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY) else {
            return Vec::new();
        };
        let mut stmt = match conn.prepare(
            "SELECT display_name, agent, COALESCE(cwd, '/'), COALESCE(title, display_name), last_active
             FROM sessions WHERE state != 'dead'
             ORDER BY last_active DESC LIMIT 20"
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, Option<String>>(4)?,
            ))
        });
        let Ok(rows) = rows else { return Vec::new() };
        rows.filter_map(|r| r.ok())
            .map(|(name, _agent, cwd, title, updated)| {
                let mut info = SessionInfo::new(name, PathBuf::from(cwd));
                info.title = Some(title);
                info.updated_at = updated;
                info
            })
            .collect()
    }
}

fn agent_config(db_path: &Option<PathBuf>, current: &str) -> Option<SessionConfigOption> {
    let db_path = db_path.as_ref()?;
    let conn = rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    let mut stmt = conn.prepare(
        "SELECT name FROM agents WHERE status='active' AND name NOT IN ('CLAUDE','claude','claude-expert','auditor') ORDER BY name"
    ).ok()?;
    let agents: Vec<SessionConfigSelectOption> = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        Ok(SessionConfigSelectOption::new(name.clone(), name))
    }).ok()?.filter_map(|r| r.ok()).collect();
    if agents.is_empty() { return None; }
    Some(SessionConfigOption::new(
        "agent",
        "Agent",
        SessionConfigKind::Select(SessionConfigSelect::new(current.to_string(), agents)),
    ))
}

fn thinking_effort_config() -> SessionConfigOption {
    let options = vec![
        SessionConfigSelectOption::new("low", "Low").description("Fast, less reasoning"),
        SessionConfigSelectOption::new("medium", "Medium").description("Balanced"),
        SessionConfigSelectOption::new("high", "High").description("Deep reasoning"),
    ];
    SessionConfigOption::new(
        "thinking_effort",
        "Thinking Effort",
        SessionConfigKind::Select(SessionConfigSelect::new("medium", options)),
    )
}

fn model_modes() -> Vec<SessionMode> {
    vec![
        SessionMode::new("sonnet", "Sonnet").description("Balanced speed and quality"),
        SessionMode::new("opus", "Opus").description("Deep reasoning and analysis"),
        SessionMode::new("haiku", "Haiku").description("Fastest, everyday tasks"),
    ]
}

fn mode_to_model(mode_id: &str) -> &str {
    match mode_id {
        "opus" => "claude-opus-4-8",
        "haiku" => "claude-haiku-3-5",
        _ => "claude-sonnet-4-6",
    }
}

struct SessionCreated {
    id: String,
    name: String,
    agent: String,
}

/// Wait for session.created from haakd, return session info
async fn wait_for_session(rx: &mut mpsc::UnboundedReceiver<String>) -> Result<SessionCreated, acp::Error> {
    while let Some(raw) = rx.next().await {
        eprintln!("[bridge] ws: {}", &raw[..150.min(raw.len())]);
        if let Ok(msg) = serde_json::from_str::<Value>(&raw) {
            let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
            if t == "session.created" {
                return Ok(SessionCreated {
                    id: msg.get("sessionId").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    name: msg.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                    agent: msg.get("agent").and_then(|v| v.as_str()).unwrap_or("").to_string(),
                });
            }
            if t == "error" {
                return Err(acp::Error::internal_error());
            }
        }
    }
    Err(acp::Error::internal_error())
}

/// Translate a haakd display event into an ACP SessionUpdate
fn translate_event(msg: &Value) -> Option<SessionUpdate> {
    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
    match t {
        "text" => {
            let d = msg.get("delta").and_then(|v| v.as_str()).unwrap_or("");
            if d.is_empty() { return None; }
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(d.to_string()))))
        }
        "thinking" => {
            let d = msg.get("text").and_then(|v| v.as_str()).unwrap_or("");
            if d.is_empty() { return None; }
            Some(SessionUpdate::AgentThoughtChunk(ContentChunk::new(ContentBlock::from(d.to_string()))))
        }
        "tool_start" => {
            let name = msg.get("name").and_then(|v| v.as_str()).unwrap_or("tool");
            let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("?");
            Some(SessionUpdate::ToolCall(ToolCall::new(id.to_string(), name.to_string())))
        }
        "tool_input" => {
            let id = msg.get("id").and_then(|v| v.as_str()).unwrap_or("");
            let summary = msg.get("summary").and_then(|v| v.as_str()).unwrap_or("");
            if id.is_empty() || summary.is_empty() { return None; }
            let mut fields = ToolCallUpdateFields::new();
            fields.title = Some(summary.to_string());
            Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(id.to_string(), fields)))
        }
        "tool_result" => {
            let id = msg.get("tool_use_id").and_then(|v| v.as_str()).unwrap_or("");
            let err = msg.get("is_error").and_then(|v| v.as_bool()).unwrap_or(false);
            let st = if err { ToolCallStatus::Failed } else { ToolCallStatus::Completed };
            let mut fields = ToolCallUpdateFields::new();
            fields.status = Some(st);
            Some(SessionUpdate::ToolCallUpdate(ToolCallUpdate::new(id.to_string(), fields)))
        }
        "permission" => None, // handled specially in prompt loop via RequestPermissionRequest
        "board.post" => {
            let agent = msg.get("agent").and_then(|v| v.as_str()).unwrap_or("?");
            let content = msg.get("content").and_then(|v| v.as_str()).unwrap_or("");
            let scope = msg.get("scope").and_then(|v| v.as_str()).unwrap_or("");
            let text = format!("📋 **{agent}** → {scope}: {content}");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text))))
        }
        "job.update" => {
            let title = msg.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let status = msg.get("status").and_then(|v| v.as_str()).unwrap_or("");
            let text = format!("⚙ Job '{title}' → {status}");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text))))
        }
        "agent.lifecycle" => {
            let agent = msg.get("agent").and_then(|v| v.as_str()).unwrap_or("");
            let event = msg.get("event").and_then(|v| v.as_str()).unwrap_or("");
            let text = format!("👤 {agent} {event}");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text))))
        }
        "notification" | "alert" => {
            let text = msg.get("text").or(msg.get("message")).and_then(|v| v.as_str()).unwrap_or("");
            if text.is_empty() { return None; }
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(format!("🔔 {text}")))))
        }
        _ => None,
    }
}

fn is_terminal_event(t: &str) -> bool {
    matches!(t, "turn_done" | "session.ended" | "error")
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();

    let args: Vec<String> = std::env::args().collect();
    let get_arg = |name: &str| -> Option<String> {
        args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned())
    };

    let url = get_arg("--url").unwrap_or_else(|| "ws://127.0.0.1:5201".to_string());
    let agent = get_arg("--agent").unwrap_or_else(|| "bala".to_string());
    let db_path = get_arg("--db").map(PathBuf::from).or_else(|| {
        let home = std::env::var("HOME").ok()?;
        let p = PathBuf::from(format!("{home}/Projects/haak/infra/var/haak.db"));
        p.exists().then_some(p)
    });

    eprintln!("[haak-acp-bridge] url={url} agent={agent} db={}", db_path.as_ref().map(|p| p.display().to_string()).unwrap_or("none".into()));

    let bridge = Bridge::new(&url, &agent, db_path);
    bridge.connect()?;

    let b_session = bridge.clone();
    let b_prompt = bridge.clone();
    let b_mode = bridge.clone();
    let b_config = bridge.clone();
    let b_list = bridge.clone();
    let b_resume = bridge.clone();

    Agent
        .builder()
        .name("haak")

        // ── Initialize ──
        .on_receive_request(
            async move |_req: InitializeRequest, responder, _connection| {
                eprintln!("[bridge] initialize");
                let mut caps = AgentCapabilities::new()
                    .prompt_capabilities(PromptCapabilities::new().embedded_context(true));
                caps.session_capabilities = SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new());
                responder.respond(
                    InitializeResponse::new(ProtocolVersion::LATEST).agent_capabilities(caps),
                )
            },
            on_receive_request!(),
        )

        // ── New Session ──
        .on_receive_request(
            async move |req: NewSessionRequest, responder, _connection| {
                let b = &b_session;
                let agent = b.current_agent.lock().unwrap().clone();
                let model_key = b.current_model.lock().unwrap().clone();
                let model = mode_to_model(&model_key);
                let cwd = req.cwd.display().to_string();
                eprintln!("[bridge] new_session agent={agent} model={model} cwd={cwd}");

                b.send(&json!({
                    "type": "session.create",
                    "agent": agent,
                    "model": model,
                    "cwd": cwd,
                }).to_string());

                let mut rx = b.take_rx().ok_or_else(|| acp::Error::internal_error())?;
                let created = wait_for_session(&mut rx).await?;
                b.put_rx(rx);
                *b.haakd_session_id.lock().unwrap() = Some(created.id.clone());
                eprintln!("[bridge] session: {} ({})", created.name, created.agent);

                let mode_state = SessionModeState::new(model_key, model_modes());
                let mut response = NewSessionResponse::new(SessionId::new(created.id));
                response.modes = Some(mode_state);
                let mut configs = vec![thinking_effort_config()];
                if let Some(agent_cfg) = agent_config(&b.db_path, &agent) {
                    configs.insert(0, agent_cfg);
                }
                response.config_options = Some(configs);
                responder.respond(response)
            },
            on_receive_request!(),
        )

        // ── List Sessions ──
        .on_receive_request(
            async move |_req: ListSessionsRequest, responder: acp::Responder<ListSessionsResponse>, _connection| {
                eprintln!("[bridge] list_sessions");
                let sessions = b_list.list_sessions();
                eprintln!("[bridge] found {} sessions", sessions.len());
                responder.respond(ListSessionsResponse::new(sessions))
            },
            on_receive_request!(),
        )

        // ── Resume Session ──
        .on_receive_request(
            async move |req: ResumeSessionRequest, responder, _connection| {
                let sid = req.session_id.to_string();
                eprintln!("[bridge] resume session: {sid}");

                let b = &b_resume;
                b.send(&json!({"type":"session.attach","sessionId":sid}).to_string());

                // Wait for session info from haakd
                let mut rx = b.take_rx().ok_or_else(|| acp::Error::internal_error())?;
                let created = wait_for_session(&mut rx).await.unwrap_or(SessionCreated {
                    id: sid.clone(), name: sid.clone(), agent: "bala".into(),
                });
                b.put_rx(rx);
                let resumed_sid = created.id;

                let mode_state = SessionModeState::new(
                    b.current_model.lock().unwrap().clone(),
                    model_modes(),
                );
                responder.respond(ResumeSessionResponse::new().modes(mode_state))
            },
            on_receive_request!(),
        )

        // ── Set Mode (model switch) ──
        .on_receive_request(
            async move |req: SetSessionModeRequest, responder: acp::Responder<SetSessionModeResponse>, _connection| {
                let mode_id = req.mode_id.to_string();
                eprintln!("[bridge] set_mode: {mode_id}");
                *b_mode.current_model.lock().unwrap() = mode_id.clone();

                // Tell haakd to reconfigure the live session with the new model
                let hsid = b_mode.haakd_session_id.lock().unwrap().clone().unwrap_or_default();
                if !hsid.is_empty() {
                    let model = mode_to_model(&mode_id);
                    b_mode.send(&json!({
                        "type": "session.reconfigure",
                        "sessionId": hsid,
                        "model": model,
                    }).to_string());
                    eprintln!("[bridge] reconfigure: {model}");
                }

                responder.respond(SetSessionModeResponse::new())
            },
            on_receive_request!(),
        )

        // ── Prompt ──
        .on_receive_request(
            async move |req: PromptRequest, responder: acp::Responder<PromptResponse>, connection| {
                let text: String = req.prompt.iter()
                    .filter_map(|b| match b {
                        ContentBlock::Text(t) => Some(t.text.as_str()),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                    .join("\n");

                let b = &b_prompt;
                let hsid = b.haakd_session_id.lock().unwrap().clone().unwrap_or_default();
                eprintln!("[bridge] prompt sid={} len={}", &hsid[..8.min(hsid.len())], text.len());
                b.send(&json!({"type":"user.message","sessionId":hsid,"text":text}).to_string());

                let mut rx = b.take_rx().ok_or_else(|| acp::Error::internal_error())?;
                let session_id = req.session_id.clone();

                let mut stop_reason = StopReason::EndTurn;
                loop {
                    let raw = match rx.next().await { Some(r) => r, None => break };
                    let msg = match serde_json::from_str::<Value>(&raw) { Ok(m) => m, Err(_) => continue };
                    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");

                    if t == "permission" {
                        // Permission prompt — send RequestPermissionRequest to Zed
                        let rid = msg.get("request_id").and_then(|v| v.as_str()).unwrap_or("perm").to_string();
                        let tool = msg.get("tool").and_then(|v| v.as_str()).unwrap_or("tool").to_string();
                        let desc = msg.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        eprintln!("[bridge] permission: {tool} — {desc}");

                        let tool_call_id = format!("perm-{rid}");
                        let mut fields = ToolCallUpdateFields::new();
                        fields.title = Some(format!("{tool}: {desc}"));
                        fields.status = Some(ToolCallStatus::Pending);
                        let tc_update = ToolCallUpdate::new(tool_call_id.clone(), fields);

                        let options = vec![
                            PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                            PermissionOption::new("deny", "Deny", PermissionOptionKind::RejectOnce),
                        ];

                        let perm_req = RequestPermissionRequest::new(
                            session_id.clone(), tc_update, options,
                        );

                        // Send request to Zed — use channel to get result back
                        let haakd_sid = hsid.clone();
                        let bridge_ref = b.clone();
                        let rid_clone = rid.clone();
                        let result = connection.send_request(perm_req).on_receiving_result(async move |result| {
                            let behavior = match result {
                                Ok(resp) => match resp.outcome {
                                    RequestPermissionOutcome::Selected(sel) => {
                                        if sel.option_id.to_string() == "allow" { "allow" } else { "deny" }
                                    }
                                    RequestPermissionOutcome::Cancelled => "deny",
                                    _ => "deny",
                                },
                                Err(_) => "deny",
                            };
                            eprintln!("[bridge] permission response: {behavior}");
                            bridge_ref.send(&json!({
                                "type": "permission.response",
                                "sessionId": haakd_sid,
                                "requestId": rid_clone,
                                "behavior": behavior,
                            }).to_string());
                            Ok(())
                        });
                        if let Err(e) = result {
                            eprintln!("[bridge] permission send failed: {e}");
                            b.send(&json!({
                                "type": "permission.response",
                                "sessionId": hsid,
                                "requestId": rid,
                                "behavior": "allow",
                            }).to_string());
                        }
                        continue;
                    }

                    if t == "turn_done" {
                        // Extract usage stats and send as UsageUpdate
                        let input = msg.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let output = msg.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let cost_val = msg.get("total_cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        if input + output > 0 {
                            let mut usage = UsageUpdate::new(input + output, 200_000); // approximate context window
                            if cost_val > 0.0 {
                                usage.cost = Some(Cost::new(cost_val, "USD"));
                            }
                            let _ = connection.send_notification(SessionNotification::new(
                                session_id.clone(), SessionUpdate::UsageUpdate(usage),
                            ));
                        }
                        break;
                    }
                    if t == "session.ended" { break; }
                    if t == "error" {
                        stop_reason = StopReason::EndTurn; // could map to error stop
                        break;
                    }

                    if let Some(update) = translate_event(&msg) {
                        connection.send_notification(SessionNotification::new(session_id.clone(), update))?;
                    }
                }

                b.put_rx(rx);
                responder.respond(PromptResponse::new(stop_reason))
            },
            on_receive_request!(),
        )

        // ── Set Config Option (agent / thinking effort) ──
        .on_receive_request(
            async move |req: SetSessionConfigOptionRequest, responder: acp::Responder<SetSessionConfigOptionResponse>, _connection| {
                let config_id = req.config_id.to_string();
                eprintln!("[bridge] set_config: {config_id} = {:?}", req);
                // Return updated config list
                let mut configs = vec![thinking_effort_config()];
                if let Some(agent_cfg) = agent_config(&b_config.db_path, &b_config.current_agent.lock().unwrap()) {
                    configs.insert(0, agent_cfg);
                }
                responder.respond(SetSessionConfigOptionResponse::new(configs))
            },
            on_receive_request!(),
        )

        // ── Cancel ──
        .on_receive_notification(
            async move |_notif: CancelNotification, _connection| {
                eprintln!("[bridge] cancel");
                Ok(())
            },
            on_receive_notification!(),
        )

        // ── Fallback ──
        .on_receive_dispatch(
            async |message: Dispatch, connection| {
                eprintln!("[bridge] unhandled: {:?}", message.method());
                message.respond_with_error(acp::Error::method_not_found(), connection)
            },
            on_receive_dispatch!(),
        )
        .connect_to(Stdio::new().with_debug(|line, dir| {
            eprintln!("[acp {dir:?}] {}", &line[..300.min(line.len())]);
        }))
        .await?;

    Ok(())
}
