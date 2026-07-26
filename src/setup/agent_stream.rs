use std::collections::HashMap;
use std::io::{IsTerminal, Write as _};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result};
use dialoguer::console::style;
use indicatif::{ProgressBar, ProgressStyle};
use regex::Regex;
use serde::Deserialize;
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Child;

// ---------------------------------------------------------------------------
// Serde types for Claude Code / Cursor stream-json JSONL
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum StreamLine {
    #[serde(rename = "stream_event")]
    StreamEvent {
        event: StreamEvent,
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "assistant")]
    Assistant {
        #[serde(flatten)]
        _extra: Value,
    },
    // cursor-agent emits tool_call events instead of stream_event
    #[serde(rename = "tool_call")]
    ToolCall {
        subtype: String,
        tool_call: Value,
        #[serde(flatten)]
        _extra: Value,
    },
    // gemini emits tool_use / tool_result pairs
    #[serde(rename = "tool_use")]
    GeminiToolUse {
        tool_name: String,
        parameters: Value,
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "tool_result")]
    GeminiToolResult {
        #[serde(flatten)]
        _extra: Value,
    },
    // gemini streams assistant text as message events
    #[serde(rename = "message")]
    GeminiMessage {
        role: String,
        content: String,
        #[serde(default)]
        delta: bool,
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "user")]
    User {
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "result")]
    Result {
        #[serde(flatten)]
        extra: Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum StreamEvent {
    #[serde(rename = "content_block_start")]
    ContentBlockStart {
        index: u32,
        content_block: ContentBlock,
    },
    #[serde(rename = "content_block_delta")]
    ContentBlockDelta { index: u32, delta: Delta },
    #[serde(rename = "content_block_stop")]
    ContentBlockStop { index: u32 },
    #[serde(rename = "message_delta")]
    MessageDelta {
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "message_stop")]
    MessageStop,
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
enum ContentBlock {
    #[serde(rename = "text")]
    Text {
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(rename = "tool_use")]
    ToolUse {
        name: String,
        #[serde(flatten)]
        _extra: Value,
    },
    #[serde(other)]
    Unknown,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
#[allow(clippy::enum_variant_names)]
enum Delta {
    #[serde(rename = "text_delta")]
    TextDelta { text: String },
    #[serde(rename = "input_json_delta")]
    InputJsonDelta { partial_json: String },
    #[serde(other)]
    Unknown,
}

// ---------------------------------------------------------------------------
// Display state machine
// ---------------------------------------------------------------------------

enum BlockState {
    Text,
    ToolUse { name: String, partial_input: String },
}

struct AgentStreamDisplay {
    blocks: HashMap<u32, BlockState>,
    spinner: Option<ProgressBar>,
    has_text_output: bool,
    is_tty: bool,
    current_tool_desc: Option<String>,
}

impl AgentStreamDisplay {
    fn new() -> Self {
        Self {
            blocks: HashMap::new(),
            spinner: None,
            has_text_output: false,
            is_tty: std::io::stderr().is_terminal(),
            current_tool_desc: None,
        }
    }

    fn handle(&mut self, line: StreamLine) {
        match line {
            StreamLine::StreamEvent { event, .. } => self.handle_event(event),
            StreamLine::ToolCall {
                subtype, tool_call, ..
            } => {
                if subtype == "started" {
                    let desc = cursor_tool_display(&tool_call);
                    self.current_tool_desc = Some(desc.clone());
                    self.clear_spinner();
                    if self.has_text_output {
                        eprintln!();
                        self.has_text_output = false;
                    }
                    self.start_spinner(&desc);
                } else if subtype == "completed" {
                    let done = self
                        .current_tool_desc
                        .take()
                        .as_deref()
                        .map(tool_done_from_in_progress)
                        .unwrap_or_else(|| cursor_tool_done_display(&tool_call));
                    self.finish_spinner_with(&done);
                }
            }
            StreamLine::GeminiToolUse {
                tool_name,
                parameters,
                ..
            } => {
                let desc = gemini_tool_display(&tool_name, &parameters);
                self.current_tool_desc = Some(desc.clone());
                self.clear_spinner();
                if self.has_text_output {
                    eprintln!();
                    self.has_text_output = false;
                }
                self.start_spinner(&desc);
            }
            StreamLine::GeminiToolResult { .. } => {
                let done = self
                    .current_tool_desc
                    .take()
                    .as_deref()
                    .map(tool_done_from_in_progress)
                    .unwrap_or_else(|| "Done".to_string());
                self.finish_spinner_with(&done);
            }
            StreamLine::GeminiMessage {
                role,
                content,
                delta,
                ..
            } => {
                if role == "assistant" && delta && !content.is_empty() {
                    self.clear_spinner();
                    eprint!("{}", style(&content).dim());
                    let _ = std::io::stderr().flush();
                    self.has_text_output = true;
                }
            }
            StreamLine::Assistant { .. } | StreamLine::User { .. } | StreamLine::Unknown => {}
            StreamLine::Result { .. } => {}
        }
    }

    fn handle_event(&mut self, event: StreamEvent) {
        match event {
            StreamEvent::ContentBlockStart {
                index,
                content_block,
            } => match content_block {
                ContentBlock::Text { .. } => {
                    self.blocks.insert(index, BlockState::Text);
                }
                ContentBlock::ToolUse { name, .. } => {
                    self.clear_spinner();
                    if self.has_text_output {
                        eprintln!();
                        self.has_text_output = false;
                    }
                    self.start_spinner(&tool_display(&name, ""));
                    self.blocks.insert(
                        index,
                        BlockState::ToolUse {
                            name,
                            partial_input: String::new(),
                        },
                    );
                }
                ContentBlock::Unknown => {}
            },
            StreamEvent::ContentBlockDelta { index, delta } => match delta {
                Delta::TextDelta { text } => {
                    if self.spinner.is_some() {
                        self.suspend_spinner();
                    }
                    eprint!("{}", style(&text).dim());
                    let _ = std::io::stderr().flush();
                    self.has_text_output = true;
                }
                Delta::InputJsonDelta { partial_json } => {
                    if let Some(BlockState::ToolUse {
                        name,
                        partial_input,
                    }) = self.blocks.get_mut(&index)
                    {
                        partial_input.push_str(&partial_json);
                        let msg = tool_display(name, partial_input);
                        if let Some(sp) = &self.spinner {
                            sp.set_message(msg);
                        }
                    }
                }
                Delta::Unknown => {}
            },
            StreamEvent::ContentBlockStop { index } => {
                if let Some(block) = self.blocks.remove(&index) {
                    match block {
                        BlockState::Text => {
                            if self.has_text_output {
                                eprintln!();
                                self.has_text_output = false;
                            }
                        }
                        BlockState::ToolUse {
                            name,
                            partial_input,
                        } => {
                            let done_msg = tool_done_display(&name, &partial_input);
                            self.finish_spinner_with(&done_msg);
                        }
                    }
                }
            }
            StreamEvent::MessageDelta { .. } | StreamEvent::MessageStop | StreamEvent::Unknown => {}
        }
    }

    fn start_spinner(&mut self, message: &str) {
        self.clear_spinner();
        if !self.is_tty {
            eprintln!("  {} {}", style("…").dim(), style(message).dim());
            return;
        }
        let sp = ProgressBar::new_spinner();
        sp.set_style(
            ProgressStyle::default_spinner()
                .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
                .template("{spinner:.cyan} {msg}")
                .unwrap(),
        );
        sp.set_message(message.to_string());
        sp.enable_steady_tick(Duration::from_millis(80));
        self.spinner = Some(sp);
    }

    fn suspend_spinner(&mut self) {
        if let Some(sp) = &self.spinner {
            sp.set_draw_target(indicatif::ProgressDrawTarget::hidden());
        }
    }

    fn clear_spinner(&mut self) {
        if let Some(sp) = self.spinner.take() {
            sp.finish_and_clear();
        }
    }

    fn finish_spinner_with(&mut self, done_msg: &str) {
        if let Some(sp) = self.spinner.take() {
            sp.finish_and_clear();
        }
        eprintln!("  {} {}", style("✓").green(), style(done_msg).dim());
    }

    fn finish(&mut self) {
        self.clear_spinner();
        if self.has_text_output {
            eprintln!();
        }
    }
}

// ---------------------------------------------------------------------------
// Tool display helpers
// ---------------------------------------------------------------------------

fn tool_display(name: &str, partial_input: &str) -> String {
    let target = extract_target(partial_input);
    let action = match name {
        "Read" => "Reading",
        "Write" => "Writing",
        "Edit" | "MultiEdit" => "Editing",
        "Bash" => {
            return match target {
                Some(cmd) => format!("Running: {cmd}"),
                None => "Running command".to_string(),
            }
        }
        "Grep" => "Searching",
        "Glob" => "Finding files",
        "LSP" => "Analyzing",
        "Task" => "Running task",
        "WebFetch" => "Fetching",
        "WebSearch" => "Searching web",
        "NotebookEdit" => "Editing notebook",
        other => other,
    };
    match target {
        Some(t) => format!("{action} {t}"),
        None => action.to_string(),
    }
}

fn tool_done_display(name: &str, partial_input: &str) -> String {
    let target = extract_target(partial_input);
    let action = match name {
        "Read" => "Read",
        "Write" => "Wrote",
        "Edit" | "MultiEdit" => "Edited",
        "Bash" => {
            return match target {
                Some(cmd) => format!("Ran: {cmd}"),
                None => "Ran command".to_string(),
            }
        }
        "Grep" => "Searched",
        "Glob" => "Found files",
        "LSP" => "Analyzed",
        "Task" => "Ran task",
        "WebFetch" => "Fetched",
        "WebSearch" => "Searched web",
        "NotebookEdit" => "Edited notebook",
        other => other,
    };
    match target {
        Some(t) => format!("{action} {t}"),
        None => action.to_string(),
    }
}

fn extract_target(partial_json: &str) -> Option<String> {
    if let Ok(obj) = serde_json::from_str::<serde_json::Map<String, Value>>(partial_json) {
        if let Some(Value::String(p)) = obj.get("file_path") {
            return Some(short_path(p));
        }
        if let Some(Value::String(c)) = obj.get("command") {
            return Some(truncate(c, 50));
        }
        if let Some(Value::String(p)) = obj.get("pattern") {
            return Some(format!("/{}/", truncate(p, 30)));
        }
    }
    let re = Regex::new(r#""file_path"\s*:\s*"([^"]+)"#).ok()?;
    re.captures(partial_json)
        .and_then(|c| c.get(1))
        .map(|m| short_path(m.as_str()))
}

fn short_path(path: &str) -> String {
    let p = std::path::Path::new(path);
    let components: Vec<_> = p
        .components()
        .rev()
        .take(2)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    components
        .iter()
        .map(|c| c.as_os_str().to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join("/")
}

fn gemini_tool_display(tool_name: &str, parameters: &Value) -> String {
    match tool_name {
        "read_file" | "view_file" => {
            if let Some(p) = parameters.get("file_path").and_then(|v| v.as_str()) {
                return format!("Reading {}", short_path(p));
            }
            "Reading".to_string()
        }
        "write_file" | "create_file" => {
            if let Some(p) = parameters.get("file_path").and_then(|v| v.as_str()) {
                return format!("Writing {}", short_path(p));
            }
            "Writing".to_string()
        }
        "edit_file" | "replace_in_file" => {
            if let Some(p) = parameters.get("file_path").and_then(|v| v.as_str()) {
                return format!("Editing {}", short_path(p));
            }
            "Editing".to_string()
        }
        "run_shell_command" | "bash" | "shell" | "run_command" => {
            if let Some(cmd) = parameters.get("command").and_then(|v| v.as_str()) {
                return format!("Running: {}", truncate(cmd, 50));
            }
            "Running command".to_string()
        }
        "search_files" | "grep" | "search" => {
            if let Some(q) = parameters
                .get("pattern")
                .or_else(|| parameters.get("query"))
                .and_then(|v| v.as_str())
            {
                return format!("Searching {}", truncate(q, 30));
            }
            "Searching".to_string()
        }
        "list_directory" | "ls" => {
            if let Some(p) = parameters
                .get("directory_path")
                .or_else(|| parameters.get("path"))
                .and_then(|v| v.as_str())
            {
                return format!("Listing {}", short_path(p));
            }
            "Listing directory".to_string()
        }
        "glob" | "find_files" => "Finding files".to_string(),
        other => other.to_string(),
    }
}

fn tool_done_from_in_progress(desc: &str) -> String {
    for (prefix, done) in [
        ("Running: ", "Ran: "),
        ("Reading ", "Read "),
        ("Writing ", "Wrote "),
        ("Editing ", "Edited "),
        ("Searching ", "Searched "),
        ("Listing ", "Listed "),
        ("Finding ", "Found "),
    ] {
        if let Some(rest) = desc.strip_prefix(prefix) {
            return format!("{done}{rest}");
        }
    }
    desc.to_string()
}

fn cursor_tool_display(tool_call: &Value) -> String {
    if let Some(cmd) = tool_call
        .pointer("/shellToolCall/args/command")
        .and_then(|v| v.as_str())
    {
        return format!("Running: {}", truncate(cmd, 50));
    }
    cursor_tool_label(tool_call, false)
}

fn cursor_tool_done_display(tool_call: &Value) -> String {
    if let Some(cmd) = tool_call
        .pointer("/shellToolCall/args/command")
        .and_then(|v| v.as_str())
    {
        return format!("Ran: {}", truncate(cmd, 50));
    }
    if let Some(cmd) = tool_call
        .pointer("/shellToolCall/result/success/command")
        .and_then(|v| v.as_str())
    {
        return format!("Ran: {}", truncate(cmd, 50));
    }
    cursor_tool_label(tool_call, true)
}

fn cursor_tool_label(tool_call: &Value, done: bool) -> String {
    let key = tool_call
        .as_object()
        .and_then(|o| o.keys().next())
        .map(|s| s.to_lowercase())
        .unwrap_or_default();
    let k = key.as_str();
    match done {
        false if k.contains("read") || k.contains("view") => "Reading".to_string(),
        true if k.contains("read") || k.contains("view") => "Read".to_string(),
        false if k.contains("edit") || k.contains("write") => "Editing".to_string(),
        true if k.contains("edit") || k.contains("write") => "Edited".to_string(),
        false if k.contains("create") => "Creating".to_string(),
        true if k.contains("create") => "Created".to_string(),
        false if k.contains("glob") => "Finding files".to_string(),
        true if k.contains("glob") => "Found files".to_string(),
        false if k.contains("search") || k.contains("grep") => "Searching".to_string(),
        true if k.contains("search") || k.contains("grep") => "Searched".to_string(),
        true => "Done".to_string(),
        false => "Working".to_string(),
    }
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let end = s.char_indices().nth(max).map(|(i, _)| i).unwrap_or(s.len());
        format!("{}…", &s[..end])
    }
}

// ---------------------------------------------------------------------------
// Public entry point
// ---------------------------------------------------------------------------

pub async fn stream_agent_output(
    mut child: Child,
    repo_root: &Path,
) -> Result<std::process::ExitStatus> {
    let stdout = child
        .stdout
        .take()
        .context("agent process stdout not captured")?;

    if let Some(stderr) = child.stderr.take() {
        tokio::spawn(async move {
            let mut lines = BufReader::new(stderr).lines();
            while let Ok(Some(line)) = lines.next_line().await {
                eprintln!("{}", style(&line).dim());
            }
        });
    }

    let mut display = AgentStreamDisplay::new();
    let mut lines = BufReader::new(stdout).lines();
    let mut result_json: Option<Value> = None;
    let mut interrupted = false;

    loop {
        tokio::select! {
            biased;
            _ = tokio::signal::ctrl_c() => {
                interrupted = true;
                display.finish();
                eprintln!("{}", style("Stopping agent…").dim());
                let _ = child.kill().await;
                break;
            }
            line = lines.next_line() => {
                match line? {
                    Some(line) if line.is_empty() => continue,
                    Some(line) => match serde_json::from_str::<StreamLine>(&line) {
                        Ok(StreamLine::Result { extra, .. }) => {
                            result_json = Some(extra);
                        }
                        Ok(parsed) => display.handle(parsed),
                        Err(_) => {}
                    },
                    None => break,
                }
            }
        }
    }

    display.finish();

    if let Some(result) = result_json {
        let result_path = repo_root.join(".bt").join("last_instrument.json");
        if let Ok(json) = serde_json::to_string_pretty(&result) {
            let _ = std::fs::write(&result_path, json);
        }
    }

    if interrupted {
        use std::process::ExitStatus;
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            return Ok(ExitStatus::from_raw(130));
        }
        #[cfg(not(unix))]
        {
            return Ok(ExitStatus::default());
        }
    }

    child.wait().await.context("agent process failed")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_stream_event_text_delta() {
        let json = r#"{"type":"stream_event","event":{"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"hello"}},"session_id":"abc"}"#;
        let parsed: StreamLine = serde_json::from_str(json).unwrap();
        match parsed {
            StreamLine::StreamEvent {
                event: StreamEvent::ContentBlockDelta { index, delta },
                ..
            } => {
                assert_eq!(index, 0);
                match delta {
                    Delta::TextDelta { text } => assert_eq!(text, "hello"),
                    _ => panic!("expected TextDelta"),
                }
            }
            _ => panic!("expected StreamEvent"),
        }
    }

    #[test]
    fn parse_stream_event_tool_use_start() {
        let json = r#"{"type":"stream_event","event":{"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"toolu_abc","name":"Read","input":{},"caller":{"type":"direct"}}},"session_id":"abc"}"#;
        let parsed: StreamLine = serde_json::from_str(json).unwrap();
        match parsed {
            StreamLine::StreamEvent {
                event:
                    StreamEvent::ContentBlockStart {
                        index,
                        content_block,
                    },
                ..
            } => {
                assert_eq!(index, 1);
                match content_block {
                    ContentBlock::ToolUse { name, .. } => assert_eq!(name, "Read"),
                    _ => panic!("expected ToolUse"),
                }
            }
            _ => panic!("expected StreamEvent"),
        }
    }

    #[test]
    fn parse_result_line() {
        let json = r#"{"type":"result","session_id":"abc","cost":0.05}"#;
        let parsed: StreamLine = serde_json::from_str(json).unwrap();
        match parsed {
            StreamLine::Result { extra } => {
                assert_eq!(extra.get("session_id").unwrap(), "abc");
            }
            _ => panic!("expected Result"),
        }
    }

    #[test]
    fn parse_unknown_event_type() {
        let json = r#"{"type":"stream_event","event":{"type":"some_future_event","data":123},"session_id":"abc"}"#;
        let parsed: StreamLine = serde_json::from_str(json).unwrap();
        match parsed {
            StreamLine::StreamEvent {
                event: StreamEvent::Unknown,
                ..
            } => {}
            _ => panic!("expected Unknown event"),
        }
    }

    #[test]
    fn parse_unknown_top_level_type() {
        let json = r#"{"type":"system","subtype":"hook","data":123}"#;
        let parsed: StreamLine = serde_json::from_str(json).unwrap();
        assert!(matches!(parsed, StreamLine::Unknown));
    }

    #[test]
    fn extract_target_from_complete_json() {
        let json = r#"{"file_path": "/Users/parker/src/app/lib/ai/providers.ts"}"#;
        assert_eq!(extract_target(json), Some("ai/providers.ts".to_string()));
    }

    #[test]
    fn extract_target_from_partial_json() {
        let json = r#"{"file_path": "/Users/parker/src/app/lib/ai/providers.ts"#;
        assert_eq!(extract_target(json), Some("ai/providers.ts".to_string()));
    }

    #[test]
    fn extract_target_command() {
        let json = r#"{"command": "npm install braintrust"}"#;
        assert_eq!(
            extract_target(json),
            Some("npm install braintrust".to_string())
        );
    }

    #[test]
    fn extract_target_pattern() {
        let json = r#"{"pattern": "handleAuth"}"#;
        assert_eq!(extract_target(json), Some("/handleAuth/".to_string()));
    }

    #[test]
    fn short_path_extracts_last_two() {
        assert_eq!(
            short_path("/Users/parker/src/app/lib/ai/providers.ts"),
            "ai/providers.ts"
        );
    }

    #[test]
    fn short_path_single_component() {
        assert_eq!(short_path("file.rs"), "file.rs");
    }

    #[test]
    fn truncate_long_string() {
        assert_eq!(truncate("hello world this is long", 11), "hello world…");
    }

    #[test]
    fn truncate_short_string() {
        assert_eq!(truncate("short", 10), "short");
    }
}
