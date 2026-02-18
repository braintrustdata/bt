use std::collections::HashMap;
use std::io;
use std::io::IsTerminal;
use std::io::Read;
use std::time::Duration;

use anyhow::{bail, Result};
use clap::Args;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout, Rect};
use ratatui::prelude::Frame;
use ratatui::style::Style;
use ratatui::text::Line;
use ratatui::widgets::{Block, Borders, Paragraph, Wrap};
use ratatui::Terminal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use unicode_width::UnicodeWidthStr;

use crate::args::BaseArgs;
use crate::auth::login;
use crate::http::ApiClient;
use crate::ui::with_spinner;

#[derive(Debug, Clone, Args)]
pub struct SqlArgs {
    /// SQL query to execute
    pub query: Option<String>,

    /// Force non-interactive mode
    #[arg(long)]
    pub non_interactive: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SqlResponse {
    pub data: Vec<Map<String, Value>>,
    pub schema: Value,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub freshness_state: Option<FreshnessState>,
    #[serde(default)]
    pub realtime_state: Option<RealtimeState>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct FreshnessState {
    pub last_considered_xact_id: String,
    pub last_processed_xact_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct RealtimeState {
    pub actual_xact_id: String,
    pub minimum_xact_id: String,
    pub read_bytes: u64,
    #[serde(rename = "type")]
    pub state_type: String,
}

pub async fn run(base: BaseArgs, args: SqlArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    let interactive = !base.json && std::io::stdin().is_terminal() && !args.non_interactive;
    let query = read_non_interactive_query(&args.query, interactive)?;

    if let Some(query) = query {
        let response = with_spinner("Running query...", execute_query(&client, &query)).await?;
        print_response(&response, base.json)?;
        return Ok(());
    }

    if !interactive {
        bail!(
            "query is required in non-interactive mode. Pass `bt sql \"SELECT * FROM project_logs('<PROJECT_ID>') LIMIT 1\"` or pipe SQL via stdin."
        );
    }

    run_interactive(base, client).await
}

fn read_non_interactive_query(
    query_arg: &Option<String>,
    interactive: bool,
) -> Result<Option<String>> {
    if let Some(query) = query_arg.as_deref() {
        let trimmed = query.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }

    if interactive {
        return Ok(None);
    }

    let mut stdin_query = String::new();
    io::stdin().read_to_string(&mut stdin_query)?;
    let trimmed = stdin_query.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_string()))
}

async fn run_interactive(base: BaseArgs, client: ApiClient) -> Result<()> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| run_interactive_blocking(base.json, client, handle))
}

fn run_interactive_blocking(
    json_output: bool,
    client: ApiClient,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let res = run_app(&mut terminal, json_output, client, handle);

    disable_raw_mode().ok();
    terminal.backend_mut().execute(LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    res
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    json_output: bool,
    client: ApiClient,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    let mut app = App::new(json_output);

    loop {
        terminal.draw(|f| ui(f, &app))?;

        if event::poll(Duration::from_millis(200))? {
            match event::read()? {
                Event::Key(key) => {
                    if handle_key_event(&mut app, key, &client, &handle)? {
                        break;
                    }
                }
                Event::Resize(_, _) => {}
                _ => {}
            }
        }
    }

    Ok(())
}

fn handle_key_event(
    app: &mut App,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<bool> {
    match key.code {
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.clear_input();
            app.status = "Cleared input".to_string();
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        KeyCode::Esc => return Ok(true),
        KeyCode::Char('l') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.output.clear();
        }
        KeyCode::Enter => {
            let query = app.input.trim().to_string();
            if query.is_empty() {
                return Ok(false);
            }

            app.status = "Running query...".to_string();
            let result = handle.block_on(execute_query(client, &query));
            match result {
                Ok(response) => {
                    app.output = format_response(&response, app.json_output)?;
                    app.status = "OK".to_string();
                }
                Err(err) => {
                    app.output = format!("Error: {err}");
                    app.status = "Error".to_string();
                }
            }

            app.push_history(&query);
            app.clear_input();
        }
        KeyCode::Backspace => app.backspace(),
        KeyCode::Delete => app.delete(),
        KeyCode::Left => app.move_left(),
        KeyCode::Right => app.move_right(),
        KeyCode::Home => app.move_home(),
        KeyCode::End => app.move_end(),
        KeyCode::Up => app.history_prev(),
        KeyCode::Down => app.history_next(),
        KeyCode::Char(ch) if !key.modifiers.contains(KeyModifiers::CONTROL) => {
            if !key.modifiers.contains(KeyModifiers::ALT) {
                app.insert_char(ch);
            }
        }
        _ => {}
    }

    Ok(false)
}

fn ui(frame: &mut Frame<'_>, app: &App) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Min(3),
            Constraint::Length(3),
            Constraint::Length(1),
        ])
        .split(frame.area());

    let output = Paragraph::new(app.output.as_str())
        .block(Block::default().title("Results").borders(Borders::ALL))
        .wrap(Wrap { trim: false });
    frame.render_widget(output, chunks[0]);

    let (input_view, cursor_col) = app.input_view(chunks[1]);
    let input =
        Paragraph::new(input_view).block(Block::default().title("SQL").borders(Borders::ALL));
    frame.render_widget(input, chunks[1]);
    frame.set_cursor_position((chunks[1].x + 1 + cursor_col, chunks[1].y + 1));

    let status = Paragraph::new(Line::from(app.status.as_str()))
        .style(Style::default())
        .block(Block::default().borders(Borders::TOP))
        .wrap(Wrap { trim: true });
    frame.render_widget(status, chunks[2]);
}

fn format_response(response: &SqlResponse, json_output: bool) -> Result<String> {
    if json_output {
        Ok(serde_json::to_string(response)?)
    } else if let Some(table) = render_table(response) {
        Ok(table)
    } else {
        Ok(serde_json::to_string_pretty(response)?)
    }
}

async fn execute_query(client: &ApiClient, query: &str) -> Result<SqlResponse> {
    let body = json!({
        "query": query,
        "fmt": "json",
    });

    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        vec![]
    };

    client.post_with_headers("/btql", &body, &headers).await
}

fn print_response(response: &SqlResponse, json_output: bool) -> Result<()> {
    let output = format_response(response, json_output)?;
    println!("{output}");
    Ok(())
}

fn render_table(response: &SqlResponse) -> Option<String> {
    let mut headers = extract_headers(&response.schema);
    if headers.is_empty() {
        if let Some(first_row) = response.data.first() {
            headers = first_row.keys().cloned().collect();
        }
    }

    if headers.is_empty() {
        if response.data.is_empty() {
            return Some("(no rows)".to_string());
        }
        return None;
    }

    let rows: Vec<Vec<String>> = response
        .data
        .iter()
        .map(|row| {
            headers
                .iter()
                .map(|header| format_cell(row.get(header)))
                .collect()
        })
        .collect();

    Some(build_table(&headers, &rows))
}

fn extract_headers(schema: &Value) -> Vec<String> {
    let items = schema.get("items").and_then(|v| v.as_object());
    let properties = items
        .and_then(|i| i.get("properties"))
        .and_then(|v| v.as_object());
    properties
        .map(|props| props.keys().cloned().collect())
        .unwrap_or_default()
}

fn format_cell(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(v) => match v {
            Value::String(s) => s.clone(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
            other => other.to_string(),
        },
    }
}

fn build_table(headers: &[String], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|h| UnicodeWidthStr::width(h.as_str()))
        .collect();

    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            let width = UnicodeWidthStr::width(cell.as_str());
            if width > widths[idx] {
                widths[idx] = width;
            }
        }
    }

    let separator = build_separator(&widths);
    let mut out = String::new();
    out.push_str(&separator);
    out.push('\n');
    out.push_str(&build_row(headers, &widths));
    out.push('\n');
    out.push_str(&separator);

    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row, &widths));
    }

    out.push('\n');
    out.push_str(&separator);
    out
}

fn build_separator(widths: &[usize]) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&"-".repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: &[String], widths: &[usize]) -> String {
    let mut line = String::new();
    line.push('|');
    for (cell, width) in cells.iter().zip(widths) {
        line.push(' ');
        line.push_str(&pad_cell(cell, *width));
        line.push(' ');
        line.push('|');
    }
    line
}

fn pad_cell(cell: &str, width: usize) -> String {
    let current = UnicodeWidthStr::width(cell);
    if current >= width {
        return cell.to_string();
    }
    let mut out = String::with_capacity(cell.len() + (width - current));
    out.push_str(cell);
    out.extend(std::iter::repeat_n(' ', width - current));
    out
}

struct App {
    input: String,
    cursor: usize,
    output: String,
    status: String,
    history: Vec<String>,
    history_index: Option<usize>,
    json_output: bool,
}

impl App {
    fn new(json_output: bool) -> Self {
        Self {
            input: String::new(),
            cursor: 0,
            output: String::new(),
            status: "Enter SQL and press Enter. Ctrl+C to exit.".to_string(),
            history: Vec::new(),
            history_index: None,
            json_output,
        }
    }

    fn insert_char(&mut self, ch: char) {
        self.input.insert(self.cursor, ch);
        self.cursor += ch.len_utf8();
        self.history_index = None;
    }

    fn backspace(&mut self) {
        if self.cursor == 0 {
            return;
        }
        let new_cursor = prev_char_boundary(&self.input, self.cursor);
        self.input.replace_range(new_cursor..self.cursor, "");
        self.cursor = new_cursor;
        self.history_index = None;
    }

    fn delete(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        let next_cursor = next_char_boundary(&self.input, self.cursor);
        self.input.replace_range(self.cursor..next_cursor, "");
        self.history_index = None;
    }

    fn move_left(&mut self) {
        if self.cursor == 0 {
            return;
        }
        self.cursor = prev_char_boundary(&self.input, self.cursor);
    }

    fn move_right(&mut self) {
        if self.cursor >= self.input.len() {
            return;
        }
        self.cursor = next_char_boundary(&self.input, self.cursor);
    }

    fn move_home(&mut self) {
        self.cursor = 0;
    }

    fn move_end(&mut self) {
        self.cursor = self.input.len();
    }

    fn clear_input(&mut self) {
        self.input.clear();
        self.cursor = 0;
        self.history_index = None;
    }

    fn push_history(&mut self, query: &str) {
        if query.trim().is_empty() {
            return;
        }
        if self.history.last().map(String::as_str) != Some(query) {
            self.history.push(query.to_string());
        }
        self.history_index = None;
    }

    fn history_prev(&mut self) {
        if self.history.is_empty() {
            return;
        }
        let next_index = match self.history_index {
            None => self.history.len().saturating_sub(1),
            Some(0) => 0,
            Some(idx) => idx - 1,
        };
        self.history_index = Some(next_index);
        self.input = self.history[next_index].clone();
        self.cursor = self.input.len();
    }

    fn history_next(&mut self) {
        let Some(idx) = self.history_index else {
            return;
        };
        let next_index = idx + 1;
        if next_index >= self.history.len() {
            self.history_index = None;
            self.clear_input();
            return;
        }
        self.history_index = Some(next_index);
        self.input = self.history[next_index].clone();
        self.cursor = self.input.len();
    }

    fn input_view(&self, area: Rect) -> (String, u16) {
        let available_width = area.width.saturating_sub(2) as usize;
        if available_width == 0 {
            return (String::new(), 0);
        }

        let mut start = self.cursor.saturating_sub(available_width);

        while start > 0 && !self.input.is_char_boundary(start) {
            start -= 1;
        }

        let mut end = (start + available_width).min(self.input.len());
        while end < self.input.len() && !self.input.is_char_boundary(end) {
            end += 1;
        }

        let visible = self.input[start..end].to_string();
        let cursor_col = self.cursor.saturating_sub(start) as u16;
        (visible, cursor_col)
    }
}

fn prev_char_boundary(s: &str, idx: usize) -> usize {
    s[..idx].char_indices().last().map(|(i, _)| i).unwrap_or(0)
}

fn next_char_boundary(s: &str, idx: usize) -> usize {
    if idx >= s.len() {
        return s.len();
    }
    let mut iter = s[idx..].char_indices();
    iter.next();
    iter.next().map(|(i, _)| idx + i).unwrap_or_else(|| s.len())
}
