//! haak-acp-bridge — Bridges haakd's WebSocket to ACP protocol on stdio.
//!
//! Zed settings.json:
//!   "agent_servers": {
//!     "haak": {
//!       "type": "custom",
//!       "command": "haak-acp-bridge",
//!       "args": ["--url", "ws://127.0.0.1:5201"]
//!     }
//!   }
//!
//! Flags: --url <ws://host:port>  --agent <default-agent>  --db <haak.db path>

use agent_client_protocol::{
    self as acp,
    on_receive_dispatch, on_receive_notification, on_receive_request,
    schema::{
        AgentCapabilities, CancelNotification, ContentBlock, ContentChunk, Cost, InitializeRequest,
        InitializeResponse, ListSessionsRequest, ListSessionsResponse, MaybeUndefined,
        NewSessionRequest, NewSessionResponse, PermissionOption, PermissionOptionKind,
        PromptCapabilities, PromptRequest, PromptResponse, ProtocolVersion,
        RequestPermissionOutcome, RequestPermissionRequest, ResumeSessionRequest,
        ResumeSessionResponse, SessionCapabilities, SessionConfigKind, SessionConfigOption,
        SessionConfigSelect, SessionConfigSelectOption, SessionId, SessionInfo,
        SessionListCapabilities, SessionMode, SessionModeState, SessionNotification,
        SessionResumeCapabilities, SessionUpdate, SetSessionConfigOptionRequest,
        SetSessionConfigOptionResponse, SetSessionModeRequest, SetSessionModeResponse, StopReason,
        ToolCall, ToolCallStatus, ToolCallUpdate, ToolCallUpdateFields, UsageUpdate,
    },
    Agent, ConnectionTo, Dispatch, Stdio,
};
use futures::channel::mpsc;
use futures::StreamExt;
use serde_json::{json, Value};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex as StdMutex};
use tungstenite::{connect, Message};

// ── Per-session state ──

struct SessionState {
    haakd_id: String,
    event_rx: mpsc::UnboundedReceiver<String>,
    model: String,
    agent: String,
}

// ── Bridge ──

struct Bridge {
    url: String,
    db_path: Option<PathBuf>,
    ws_tx: StdMutex<Option<std::sync::mpsc::Sender<String>>>,
    /// Demuxed per-session event receivers, keyed by haakd session ID
    sessions: StdMutex<HashMap<String, SessionState>>,
    /// ACP SessionId → haakd session ID
    session_map: StdMutex<HashMap<String, String>>,
    /// Global event receiver for session.created (before we know the session ID)
    global_rx: StdMutex<Option<mpsc::UnboundedReceiver<String>>>,
    default_agent: String,
    default_model: String,
}

impl Bridge {
    fn new(url: &str, agent: &str, db_path: Option<PathBuf>) -> Arc<Self> {
        Arc::new(Self {
            url: url.to_string(),
            db_path,
            ws_tx: StdMutex::new(None),
            sessions: StdMutex::new(HashMap::new()),
            session_map: StdMutex::new(HashMap::new()),
            global_rx: StdMutex::new(None),
            default_agent: agent.to_string(),
            default_model: "sonnet".to_string(),
        })
    }

    fn connect(&self) -> anyhow::Result<()> {
        // Reject non-ws:// URLs — TLS requires tokio-tungstenite (future work)
        if self.url.starts_with("wss://") || self.url.starts_with("https://") {
            anyhow::bail!("TLS not supported yet — use ws:// URL. For remote access, use an SSH tunnel.");
        }

        let (event_tx, event_rx) = mpsc::unbounded::<String>();
        let (write_tx, write_rx) = std::sync::mpsc::channel::<String>();
        let ws_url = format!("{}/ws/session", self.url);

        // Demux state: route events to per-session channels
        let sessions_ref = Arc::new(StdMutex::new(HashMap::<String, mpsc::UnboundedSender<String>>::new()));
        let sessions_demux = sessions_ref.clone();

        // WS reader thread → demux by sessionId
        std::thread::spawn(move || {
            eprintln!("[ws] connecting to {ws_url}");
            let (mut ws, _) = match connect(&ws_url) {
                Ok(pair) => { eprintln!("[ws] connected"); pair }
                Err(e) => {
                    eprintln!("[ws] FAILED: {e}");
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
                        // Try to route to per-session channel by sessionId
                        let routed = if let Ok(v) = serde_json::from_str::<Value>(&text) {
                            if let Some(sid) = v.get("sessionId").and_then(|v| v.as_str()) {
                                let sessions = sessions_demux.lock().unwrap();
                                if let Some(tx) = sessions.get(sid) {
                                    tx.unbounded_send(text.clone()).is_ok()
                                } else {
                                    false
                                }
                            } else {
                                false
                            }
                        } else {
                            false
                        };
                        // Unrouted events go to global channel (for session.created)
                        if !routed {
                            let _ = event_tx.unbounded_send(text);
                        }
                    }
                    Ok(msg) if msg.is_close() => break,
                    Err(tungstenite::Error::Io(ref e)) if e.kind() == std::io::ErrorKind::WouldBlock => {}
                    Err(_) => break,
                    _ => {}
                }
                while let Ok(payload) = write_rx.try_recv() {
                    let _ = ws.send(Message::text(payload));
                }
                std::thread::sleep(std::time::Duration::from_millis(10));
            }
            eprintln!("[ws] disconnected");
        });

        *self.ws_tx.lock().unwrap() = Some(write_tx);
        *self.global_rx.lock().unwrap() = Some(event_rx);

        // Store the demux sender map so register_session can add channels
        // We need a way for register_session to add senders to the demux map.
        // Store it on the bridge:
        // Actually, the demux map is in the WS thread. We need shared access.
        // sessions_ref is already Arc<Mutex<HashMap>> — store it on Bridge.
        // But Bridge is already constructed. Use a separate field.
        // For now, store session channels directly and have the WS thread
        // check them. We already have sessions_ref shared between the thread
        // and the bridge — but we need to get it into the Bridge struct.
        //
        // Simpler approach: the global_rx gets ALL events. The session handlers
        // take events from their own channel. We register per-session senders
        // in the demux map before sending session.create.

        // Store the demux sender map for register_session
        // This is a bit ugly but works: we leak the Arc into a global.
        // Better: store on Bridge. But Bridge is already constructed.
        // Let's use a once-cell pattern.
        unsafe {
            DEMUX_SENDERS = Some(sessions_ref);
        }

        Ok(())
    }

    fn send(&self, msg: &str) {
        if let Some(tx) = self.ws_tx.lock().unwrap().as_ref() {
            let _ = tx.send(msg.to_string());
        }
    }

    /// Register a per-session event channel with the WS demux thread
    fn register_session_channel(&self, haakd_sid: &str) -> mpsc::UnboundedReceiver<String> {
        let (tx, rx) = mpsc::unbounded::<String>();
        unsafe {
            if let Some(ref senders) = DEMUX_SENDERS {
                senders.lock().unwrap().insert(haakd_sid.to_string(), tx);
            }
        }
        rx
    }

    fn take_global_rx(&self) -> Option<mpsc::UnboundedReceiver<String>> {
        self.global_rx.lock().unwrap().take()
    }

    fn put_global_rx(&self, rx: mpsc::UnboundedReceiver<String>) {
        *self.global_rx.lock().unwrap() = Some(rx);
    }

    fn list_sessions(&self) -> Vec<SessionInfo> {
        let Some(db_path) = &self.db_path else { return Vec::new() };
        let Ok(conn) = rusqlite::Connection::open_with_flags(db_path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY) else {
            return Vec::new();
        };
        let mut stmt = match conn.prepare(
            "SELECT display_name, COALESCE(cwd, '/'), COALESCE(title, display_name), last_active
             FROM sessions WHERE state NOT IN ('dead','ended')
             ORDER BY last_active DESC LIMIT 20"
        ) {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        stmt.query_map([], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?,
                row.get::<_, String>(2)?, row.get::<_, Option<String>>(3)?))
        }).ok().map(|rows| {
            rows.filter_map(|r| r.ok()).map(|(name, cwd, title, updated)| {
                let mut info = SessionInfo::new(name, PathBuf::from(cwd));
                info.title = Some(title);
                info.updated_at = updated;
                info
            }).collect()
        }).unwrap_or_default()
    }
}

static mut DEMUX_SENDERS: Option<Arc<StdMutex<HashMap<String, mpsc::UnboundedSender<String>>>>> = None;

// ── Helpers ──

/// Truncate a string safely at a char boundary
fn safe_truncate(s: &str, max: usize) -> &str {
    if s.len() <= max { return s; }
    match s.char_indices().nth(max) {
        Some((i, _)) => &s[..i],
        None => s,
    }
}

fn agent_config(db_path: &Option<PathBuf>, current: &str) -> Option<SessionConfigOption> {
    let conn = rusqlite::Connection::open_with_flags(db_path.as_ref()?, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY).ok()?;
    let mut stmt = conn.prepare(
        "SELECT name FROM agents WHERE status='active' AND name NOT IN ('CLAUDE','claude','claude-expert','auditor') ORDER BY name"
    ).ok()?;
    let agents: Vec<SessionConfigSelectOption> = stmt.query_map([], |row| {
        let name: String = row.get(0)?;
        Ok(SessionConfigSelectOption::new(name.clone(), name))
    }).ok()?.filter_map(|r| r.ok()).collect();
    if agents.is_empty() { return None; }
    Some(SessionConfigOption::new(
        "agent", "Agent",
        SessionConfigKind::Select(SessionConfigSelect::new(current.to_string(), agents)),
    ))
}

fn thinking_effort_config(current: &str) -> SessionConfigOption {
    SessionConfigOption::new("thinking_effort", "Thinking Effort", SessionConfigKind::Select(
        SessionConfigSelect::new(current.to_string(), vec![
            SessionConfigSelectOption::new("low", "Low").description("Fast, less reasoning"),
            SessionConfigSelectOption::new("medium", "Medium").description("Balanced"),
            SessionConfigSelectOption::new("high", "High").description("Deep reasoning"),
        ]),
    ))
}

fn model_modes() -> Vec<SessionMode> {
    vec![
        SessionMode::new("sonnet", "Sonnet").description("Balanced speed and quality"),
        SessionMode::new("opus", "Opus").description("Deep reasoning and analysis"),
        SessionMode::new("haiku", "Haiku").description("Fastest, everyday tasks"),
    ]
}

fn mode_to_model(mode_id: &str) -> &str {
    match mode_id { "opus" => "claude-opus-4-8", "haiku" => "claude-haiku-3-5", _ => "claude-sonnet-4-6" }
}

fn context_window(model: &str) -> u64 {
    if model.contains("opus") { 200_000 } else if model.contains("haiku") { 200_000 } else { 200_000 }
}

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
        "permission" => None, // handled via RequestPermissionRequest in prompt loop
        "board.post" => {
            let agent = msg.get("agent").and_then(|v| v.as_str()).unwrap_or("?");
            let content = msg.get("content").and_then(|v| v.as_str()).unwrap_or("");
            let scope = msg.get("scope").and_then(|v| v.as_str()).unwrap_or("");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                format!("**{agent}** posted to {scope}: {content}")
            ))))
        }
        "job.update" => {
            let title = msg.get("title").and_then(|v| v.as_str()).unwrap_or("");
            let status = msg.get("status").and_then(|v| v.as_str()).unwrap_or("");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                format!("Job '{title}' changed to {status}")
            ))))
        }
        "agent.lifecycle" => {
            let agent = msg.get("agent").and_then(|v| v.as_str()).unwrap_or("");
            let event = msg.get("event").and_then(|v| v.as_str()).unwrap_or("");
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(
                format!("{agent} {event}")
            ))))
        }
        "notification" | "alert" => {
            let text = msg.get("text").or(msg.get("message")).and_then(|v| v.as_str()).unwrap_or("");
            if text.is_empty() { return None; }
            Some(SessionUpdate::AgentMessageChunk(ContentChunk::new(ContentBlock::from(text.to_string()))))
        }
        _ => None,
    }
}

/// Wait for session.created from haakd on the global channel, with timeout
async fn wait_for_session(rx: &mut mpsc::UnboundedReceiver<String>) -> Result<(String, String, String), acp::Error> {
    // 15 second timeout
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if std::time::Instant::now() > deadline {
            return Err(acp::Error::internal_error().data("Timeout waiting for haakd session.created"));
        }
        match futures::future::poll_fn(|cx| rx.poll_next_unpin(cx)).await {
            Some(raw) => {
                if let Ok(msg) = serde_json::from_str::<Value>(&raw) {
                    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");
                    if t == "session.created" {
                        let sid = msg.get("sessionId").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let name = msg.get("name").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        let agent = msg.get("agent").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        return Ok((sid, name, agent));
                    }
                    if t == "error" {
                        let msg = msg.get("message").and_then(|v| v.as_str()).unwrap_or("unknown");
                        return Err(acp::Error::internal_error().data(msg.to_string()));
                    }
                    // Skip other events (session.state, etc.)
                }
            }
            None => return Err(acp::Error::internal_error().data("WebSocket closed")),
        }
    }
}

// ── Main ──

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_writer(std::io::stderr).init();

    let args: Vec<String> = std::env::args().collect();
    let get_arg = |name: &str| args.iter().position(|a| a == name).and_then(|i| args.get(i + 1).cloned());

    let url = get_arg("--url").unwrap_or_else(|| "ws://127.0.0.1:5201".to_string());
    let agent = get_arg("--agent").unwrap_or_else(|| "bala".to_string());
    let db_path = get_arg("--db").map(PathBuf::from).or_else(|| {
        let home = std::env::var("HOME").ok()?;
        let p = PathBuf::from(format!("{home}/Projects/haak/infra/var/haak.db"));
        p.exists().then_some(p)
    });

    eprintln!("[haak-acp-bridge] url={url} agent={agent}");

    let bridge = Bridge::new(&url, &agent, db_path);
    bridge.connect()?;

    let b1 = bridge.clone();
    let b2 = bridge.clone();
    let b3 = bridge.clone();
    let b4 = bridge.clone();
    let b5 = bridge.clone();
    let b6 = bridge.clone();

    Agent
        .builder()
        .name("haak")

        // ── Initialize ──
        .on_receive_request(
            async move |_req: InitializeRequest, responder, _cx| {
                eprintln!("[bridge] initialize");
                let mut caps = AgentCapabilities::new()
                    .prompt_capabilities(PromptCapabilities::new().embedded_context(true));
                caps.session_capabilities = SessionCapabilities::new()
                    .list(SessionListCapabilities::new())
                    .resume(SessionResumeCapabilities::new());
                responder.respond(InitializeResponse::new(ProtocolVersion::LATEST).agent_capabilities(caps))
            },
            on_receive_request!(),
        )

        // ── New Session ──
        .on_receive_request(
            async move |req: NewSessionRequest, responder, _cx| {
                let b = &b1;
                let agent = b.default_agent.clone();
                let model_key = b.default_model.clone();
                let model = mode_to_model(&model_key);
                let cwd = req.cwd.display().to_string();
                eprintln!("[bridge] new_session agent={agent} model={model} cwd={}", safe_truncate(&cwd, 60));

                b.send(&json!({"type":"session.create","agent":agent,"model":model,"cwd":cwd}).to_string());

                let mut rx = b.take_global_rx().ok_or_else(|| acp::Error::internal_error())?;
                let (haakd_sid, name, actual_agent) = wait_for_session(&mut rx).await?;
                b.put_global_rx(rx);

                if haakd_sid.is_empty() {
                    return Err(acp::Error::internal_error().data("Empty session ID"));
                }

                // Register per-session event channel with the demux
                let session_rx = b.register_session_channel(&haakd_sid);

                // Map ACP SessionId → haakd session ID
                let acp_sid = SessionId::new(haakd_sid.clone());
                b.session_map.lock().unwrap().insert(acp_sid.to_string(), haakd_sid.clone());
                b.sessions.lock().unwrap().insert(haakd_sid.clone(), SessionState {
                    haakd_id: haakd_sid,
                    event_rx: session_rx,
                    model: model_key.clone(),
                    agent: actual_agent.clone(),
                });

                eprintln!("[bridge] session: {name} ({actual_agent})");

                let mode_state = SessionModeState::new(model_key, model_modes());
                let mut response = NewSessionResponse::new(acp_sid);
                response.modes = Some(mode_state);
                let mut configs = vec![thinking_effort_config("medium")];
                if let Some(ac) = agent_config(&b.db_path, &actual_agent) {
                    configs.insert(0, ac);
                }
                response.config_options = Some(configs);
                responder.respond(response)
            },
            on_receive_request!(),
        )

        // ── List Sessions ──
        .on_receive_request(
            async move |_req: ListSessionsRequest, responder: acp::Responder<ListSessionsResponse>, _cx| {
                let sessions = b2.list_sessions();
                eprintln!("[bridge] list_sessions: {} found", sessions.len());
                responder.respond(ListSessionsResponse::new(sessions))
            },
            on_receive_request!(),
        )

        // ── Resume Session ──
        .on_receive_request(
            async move |req: ResumeSessionRequest, responder, _cx| {
                let sid = req.session_id.to_string();
                eprintln!("[bridge] resume: {sid}");
                let b = &b3;
                b.send(&json!({"type":"session.attach","sessionId":sid}).to_string());

                let mut rx = b.take_global_rx().ok_or_else(|| acp::Error::internal_error())?;
                let (haakd_sid, _, _) = wait_for_session(&mut rx).await
                    .unwrap_or((sid.clone(), sid.clone(), "bala".into()));
                b.put_global_rx(rx);

                let session_rx = b.register_session_channel(&haakd_sid);
                let acp_sid = SessionId::new(haakd_sid.clone());
                b.session_map.lock().unwrap().insert(acp_sid.to_string(), haakd_sid.clone());
                b.sessions.lock().unwrap().insert(haakd_sid.clone(), SessionState {
                    haakd_id: haakd_sid,
                    event_rx: session_rx,
                    model: b.default_model.clone(),
                    agent: b.default_agent.clone(),
                });

                let mode_state = SessionModeState::new(b.default_model.clone(), model_modes());
                responder.respond(ResumeSessionResponse::new().modes(mode_state))
            },
            on_receive_request!(),
        )

        // ── Set Mode (model) ──
        .on_receive_request(
            async move |req: SetSessionModeRequest, responder: acp::Responder<SetSessionModeResponse>, _cx| {
                let mode_id = req.mode_id.to_string();
                eprintln!("[bridge] set_mode: {mode_id}");
                let b = &b4;

                // Update per-session model
                let haakd_sid = b.session_map.lock().unwrap().get(&req.session_id.to_string()).cloned();
                if let Some(ref hsid) = haakd_sid {
                    if let Some(sess) = b.sessions.lock().unwrap().get_mut(hsid) {
                        sess.model = mode_id.clone();
                    }
                    let model = mode_to_model(&mode_id);
                    b.send(&json!({"type":"session.reconfigure","sessionId":hsid,"model":model}).to_string());
                }

                responder.respond(SetSessionModeResponse::new())
            },
            on_receive_request!(),
        )

        // ── Set Config (agent / thinking effort) ──
        .on_receive_request(
            async move |req: SetSessionConfigOptionRequest, responder: acp::Responder<SetSessionConfigOptionResponse>, _cx| {
                let config_id = req.config_id.to_string();
                eprintln!("[bridge] set_config: {config_id}");
                let b = &b5;

                // For now, return the current configs unchanged
                // TODO: extract value from req, update session state, send reconfigure
                let current_agent = b.default_agent.clone();
                let mut configs = vec![thinking_effort_config("medium")];
                if let Some(ac) = agent_config(&b.db_path, &current_agent) {
                    configs.insert(0, ac);
                }
                responder.respond(SetSessionConfigOptionResponse::new(configs))
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

                let b = &b6;
                let acp_sid = req.session_id.to_string();
                let haakd_sid = b.session_map.lock().unwrap().get(&acp_sid).cloned()
                    .unwrap_or_default();

                if haakd_sid.is_empty() {
                    return Err(acp::Error::internal_error().data("No haakd session for this thread"));
                }

                eprintln!("[bridge] prompt sid={} len={}", safe_truncate(&haakd_sid, 8), text.len());
                b.send(&json!({"type":"user.message","sessionId":haakd_sid,"text":text}).to_string());

                // Take this session's event receiver
                let mut rx = {
                    let mut sessions = b.sessions.lock().unwrap();
                    match sessions.get_mut(&haakd_sid) {
                        Some(sess) => {
                            // Swap out the receiver — we'll put it back after the turn
                            let (placeholder_tx, placeholder_rx) = mpsc::unbounded();
                            drop(placeholder_tx);
                            std::mem::replace(&mut sess.event_rx, placeholder_rx)
                        }
                        None => return Err(acp::Error::internal_error().data("Session not found")),
                    }
                };

                let session_id = req.session_id.clone();
                let mut stop = StopReason::EndTurn;

                loop {
                    let raw = match rx.next().await { Some(r) => r, None => {
                        eprintln!("[bridge] session stream ended");
                        break;
                    }};
                    let msg = match serde_json::from_str::<Value>(&raw) { Ok(m) => m, Err(_) => continue };
                    let t = msg.get("type").and_then(|v| v.as_str()).unwrap_or("");

                    if t == "permission" {
                        let rid = msg.get("request_id").and_then(|v| v.as_str()).unwrap_or("perm").to_string();
                        let tool = msg.get("tool").and_then(|v| v.as_str()).unwrap_or("tool").to_string();
                        let desc = msg.get("description").and_then(|v| v.as_str()).unwrap_or("").to_string();
                        eprintln!("[bridge] permission: {tool}");

                        let tc_id = format!("perm-{rid}");
                        let mut fields = ToolCallUpdateFields::new();
                        fields.title = Some(format!("{tool}: {desc}"));
                        fields.status = Some(ToolCallStatus::Pending);

                        let perm_req = RequestPermissionRequest::new(
                            session_id.clone(),
                            ToolCallUpdate::new(tc_id, fields),
                            vec![
                                PermissionOption::new("allow", "Allow", PermissionOptionKind::AllowOnce),
                                PermissionOption::new("deny", "Deny", PermissionOptionKind::RejectOnce),
                            ],
                        );

                        let bridge_ref = b.clone();
                        let hsid = haakd_sid.clone();
                        let rid_clone = rid.clone();
                        let _ = connection.send_request(perm_req).on_receiving_result(async move |result| {
                            let behavior = match result {
                                Ok(resp) => match resp.outcome {
                                    RequestPermissionOutcome::Selected(sel) => {
                                        if sel.option_id.to_string() == "allow" { "allow" } else { "deny" }
                                    }
                                    _ => "deny",
                                },
                                Err(_) => "deny",
                            };
                            eprintln!("[bridge] permission: {behavior}");
                            bridge_ref.send(&json!({
                                "type":"permission.response","sessionId":hsid,
                                "requestId":rid_clone,"behavior":behavior,
                            }).to_string());
                            Ok(())
                        });
                        continue;
                    }

                    if t == "turn_done" {
                        let input = msg.get("input_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let output = msg.get("output_tokens").and_then(|v| v.as_u64()).unwrap_or(0);
                        let cost_val = msg.get("total_cost_usd").and_then(|v| v.as_f64()).unwrap_or(0.0);
                        if input + output > 0 {
                            let mut usage = UsageUpdate::new(input + output, context_window(""));
                            if cost_val > 0.0 { usage.cost = Some(Cost::new(cost_val, "USD")); }
                            let _ = connection.send_notification(SessionNotification::new(
                                session_id.clone(), SessionUpdate::UsageUpdate(usage),
                            ));
                        }
                        break;
                    }
                    if t == "session.ended" { break; }
                    if t == "error" {
                        eprintln!("[bridge] error: {:?}", msg.get("message"));
                        stop = StopReason::EndTurn; // TODO: map to error stop when ACP supports it
                        break;
                    }

                    if let Some(update) = translate_event(&msg) {
                        let _ = connection.send_notification(SessionNotification::new(session_id.clone(), update));
                    }
                }

                // Put the receiver back
                if let Some(sess) = b.sessions.lock().unwrap().get_mut(&haakd_sid) {
                    sess.event_rx = rx;
                }

                responder.respond(PromptResponse::new(stop))
            },
            on_receive_request!(),
        )

        // ── Cancel ──
        .on_receive_notification(
            async move |_notif: CancelNotification, _cx| { eprintln!("[bridge] cancel"); Ok(()) },
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
        .connect_to(Stdio::new())
        .await?;

    Ok(())
}
