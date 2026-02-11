use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::IsTerminal;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::Args;
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use ratatui::backend::CrosstermBackend;
use ratatui::layout::{Constraint, Direction, Layout};
use ratatui::prelude::Frame;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{
    Block, BorderType, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, TableState,
    Wrap,
};
use ratatui::Terminal;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::http::ApiClient;
use crate::login::login;
use crate::ui::{fuzzy_select, with_spinner};

const MAX_TRACE_SPANS: usize = 5000;
const MAX_BTQL_PAGE_LIMIT: usize = 1000;
const THREAD_PREPROCESSOR_NAME: &str = "project_default";
const LOADER_DELAY: Duration = Duration::from_millis(250);
const TRACE_TEXT_SEARCH_FIELDS: [&str; 6] = [
    "input",
    "expected",
    "metadata",
    "tags",
    "output",
    "span_attributes",
];

#[derive(Debug, Clone, Args)]
pub struct TracesArgs {
    /// Project ID to query (overrides -p/--project)
    #[arg(long)]
    pub project_id: Option<String>,

    /// Number of traces to show in the main table
    #[arg(long, default_value_t = 50)]
    pub limit: usize,

    /// Preview length used in summary rows
    #[arg(long, default_value_t = 125)]
    pub preview_length: usize,

    /// Print each BTQL query and invoke payload before execution
    #[arg(long)]
    pub print_queries: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct BtqlResponse {
    pub data: Vec<Map<String, Value>>,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub schema: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct ProjectSelection {
    id: String,
    name: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ProjectsListResponse {
    objects: Vec<ProjectInfo>,
}

#[derive(Debug, Clone, Deserialize)]
struct ProjectInfo {
    id: String,
    name: String,
}

#[derive(Debug, Clone, Serialize)]
struct TraceSummaryRow {
    root_span_id: String,
    row: Map<String, Value>,
}

#[derive(Debug, Clone)]
struct SpanListEntry {
    id: String,
    span_id: String,
    root_span_id: String,
    depth: usize,
    label: String,
    row: Map<String, Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Screen {
    Traces,
    SpanDetail,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailView {
    Span,
    Thread,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DetailPaneFocus {
    Tree,
    Detail,
}

struct TraceViewerApp {
    project: ProjectSelection,
    traces: Vec<TraceSummaryRow>,
    selected_trace: usize,
    trace_search_query: String,
    trace_search_input: String,
    trace_search_mode: bool,
    summary_loading_search_query: String,
    summary_pending_refresh: Option<(String, Option<String>)>,
    summary_load_started_at: Option<Instant>,
    summary_spinner_tick: usize,
    summary_load_rx: Option<Receiver<(String, Option<String>, Result<Vec<Map<String, Value>>>)>>,
    trace_loading_root_span_id: Option<String>,
    trace_pending_root_span_id: Option<String>,
    trace_load_started_at: Option<Instant>,
    trace_spinner_tick: usize,
    trace_load_rx: Option<Receiver<(String, Result<Vec<Map<String, Value>>>)>>,
    spans: Vec<SpanListEntry>,
    full_span_cache: HashMap<String, Map<String, Value>>,
    full_span_loading_row_id: Option<String>,
    full_span_pending_row_id: Option<String>,
    full_span_load_started_at: Option<Instant>,
    full_span_spinner_tick: usize,
    full_span_load_rx: Option<Receiver<(String, Result<Option<Map<String, Value>>>)>>,
    selected_span: usize,
    detail_scroll: u16,
    detail_view: DetailView,
    detail_pane_focus: DetailPaneFocus,
    thread_messages: Option<Vec<Value>>,
    thread_selected_message: usize,
    thread_expanded: HashSet<usize>,
    thread_loading: bool,
    thread_spinner_tick: usize,
    thread_load_rx: Option<Receiver<Result<Vec<Value>>>>,
    loaded_root_span_id: Option<String>,
    screen: Screen,
    status: String,
    status_detail: String,
    status_is_error: bool,
    limit: usize,
    preview_length: usize,
    print_queries: bool,
}

impl TraceViewerApp {
    fn new(
        project: ProjectSelection,
        traces: Vec<TraceSummaryRow>,
        limit: usize,
        preview_length: usize,
        print_queries: bool,
    ) -> Self {
        let trace_count = traces.len();
        let status = format!(
            "Loaded {trace_count} traces. Up/Down: move  Enter: open trace  /: search  r: refresh  q: quit"
        );
        Self {
            project,
            traces,
            selected_trace: 0,
            trace_search_query: String::new(),
            trace_search_input: String::new(),
            trace_search_mode: false,
            summary_loading_search_query: String::new(),
            summary_pending_refresh: None,
            summary_load_started_at: None,
            summary_spinner_tick: 0,
            summary_load_rx: None,
            trace_loading_root_span_id: None,
            trace_pending_root_span_id: None,
            trace_load_started_at: None,
            trace_spinner_tick: 0,
            trace_load_rx: None,
            spans: Vec::new(),
            full_span_cache: HashMap::new(),
            full_span_loading_row_id: None,
            full_span_pending_row_id: None,
            full_span_load_started_at: None,
            full_span_spinner_tick: 0,
            full_span_load_rx: None,
            selected_span: 0,
            detail_scroll: 0,
            detail_view: DetailView::Span,
            detail_pane_focus: DetailPaneFocus::Tree,
            thread_messages: None,
            thread_selected_message: 0,
            thread_expanded: HashSet::new(),
            thread_loading: false,
            thread_spinner_tick: 0,
            thread_load_rx: None,
            loaded_root_span_id: None,
            screen: Screen::Traces,
            status,
            status_detail: String::new(),
            status_is_error: false,
            limit,
            preview_length,
            print_queries,
        }
    }

    fn set_status<S: Into<String>>(&mut self, status: S) {
        self.status = status.into();
        self.status_detail.clear();
        self.status_is_error = false;
    }

    fn set_error<S: Into<String>>(&mut self, status: S) {
        self.status = status.into();
        self.status_detail.clear();
        self.status_is_error = true;
    }

    fn set_error_with_query<S: Into<String>, Q: Into<String>>(&mut self, status: S, query: Q) {
        self.status = status.into();
        self.status_detail = query.into();
        self.status_is_error = true;
    }

    fn current_trace(&self) -> Option<&TraceSummaryRow> {
        self.traces.get(self.selected_trace)
    }

    fn current_span(&self) -> Option<&SpanListEntry> {
        self.spans.get(self.selected_span)
    }

    fn move_trace_up(&mut self) {
        if self.selected_trace > 0 {
            self.selected_trace -= 1;
        }
    }

    fn move_trace_down(&mut self) {
        if self.selected_trace + 1 < self.traces.len() {
            self.selected_trace += 1;
        }
    }

    fn move_span_up(&mut self) {
        if self.selected_span > 0 {
            self.selected_span -= 1;
            self.detail_scroll = 0;
        }
    }

    fn move_span_down(&mut self) {
        if self.selected_span + 1 < self.spans.len() {
            self.selected_span += 1;
            self.detail_scroll = 0;
        }
    }

    fn scroll_detail_up(&mut self, amount: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(amount);
    }

    fn scroll_detail_down(&mut self, amount: u16) {
        self.detail_scroll = self.detail_scroll.saturating_add(amount);
    }

    fn move_thread_message_up(&mut self) {
        if self.thread_selected_message > 0 {
            self.thread_selected_message -= 1;
        }
    }

    fn move_thread_message_down(&mut self) {
        let Some(messages) = &self.thread_messages else {
            return;
        };
        if self.thread_selected_message + 1 < messages.len() {
            self.thread_selected_message += 1;
        }
    }

    fn toggle_selected_thread_message(&mut self) {
        let idx = self.thread_selected_message;
        if self.thread_expanded.contains(&idx) {
            self.thread_expanded.remove(&idx);
        } else {
            self.thread_expanded.insert(idx);
        }
    }
}

pub async fn run(base: BaseArgs, args: TracesArgs) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit must be greater than 0");
    }

    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;

    let project =
        resolve_project(&client, base.project.as_deref(), args.project_id.as_deref()).await?;

    let initial_rows = with_spinner(
        "Loading traces...",
        fetch_summary_rows(
            &client,
            &project.id,
            args.preview_length,
            args.limit,
            None,
            args.print_queries,
        ),
    )
    .await?;
    let traces = parse_summary_rows(initial_rows);

    if base.json || !std::io::stdin().is_terminal() {
        let payload = json!({
            "project": project,
            "limit": args.limit,
            "preview_length": args.preview_length,
            "rows": traces,
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
        return Ok(());
    }

    run_interactive(
        project,
        traces,
        args.limit,
        args.preview_length,
        args.print_queries,
        client,
    )
    .await
}

async fn resolve_project(
    client: &ApiClient,
    project_name_from_base: Option<&str>,
    explicit_project_id: Option<&str>,
) -> Result<ProjectSelection> {
    if let Some(project_id) = explicit_project_id {
        return Ok(ProjectSelection {
            id: project_id.to_string(),
            name: None,
        });
    }

    if let Some(name) = project_name_from_base {
        let project = get_project_by_name(client, name)
            .await?
            .with_context(|| format!("project '{name}' not found"))?;

        return Ok(ProjectSelection {
            id: project.id,
            name: Some(project.name),
        });
    }

    if !std::io::stdin().is_terminal() {
        bail!("project is required. Pass --project-id <id> or -p <project-name>");
    }

    let mut projects = list_projects(client).await?;
    if projects.is_empty() {
        bail!("no projects found for this org");
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<String> = projects.iter().map(|p| p.name.clone()).collect();
    let selected = fuzzy_select("Select project", &names)?;
    let project = projects
        .get(selected)
        .context("project selection out of range")?
        .clone();

    Ok(ProjectSelection {
        id: project.id,
        name: Some(project.name),
    })
}

async fn list_projects(client: &ApiClient) -> Result<Vec<ProjectInfo>> {
    let path = format!("/v1/project?org_name={}", encode(client.org_name()));
    let response: ProjectsListResponse = client.get(&path).await?;
    Ok(response.objects)
}

async fn get_project_by_name(client: &ApiClient, name: &str) -> Result<Option<ProjectInfo>> {
    let path = format!(
        "/v1/project?org_name={}&name={}",
        encode(client.org_name()),
        encode(name)
    );
    let response: ProjectsListResponse = client.get(&path).await?;
    Ok(response.objects.into_iter().next())
}

async fn run_interactive(
    project: ProjectSelection,
    traces: Vec<TraceSummaryRow>,
    limit: usize,
    preview_length: usize,
    print_queries: bool,
    client: ApiClient,
) -> Result<()> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| {
        run_interactive_blocking(
            project,
            traces,
            limit,
            preview_length,
            print_queries,
            client,
            handle,
        )
    })
}

fn run_interactive_blocking(
    project: ProjectSelection,
    traces: Vec<TraceSummaryRow>,
    limit: usize,
    preview_length: usize,
    print_queries: bool,
    client: ApiClient,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    let mut app = TraceViewerApp::new(project, traces, limit, preview_length, print_queries);
    let result = run_app(&mut terminal, &mut app, client, handle);

    disable_raw_mode().ok();
    terminal.backend_mut().execute(LeaveAlternateScreen).ok();
    terminal.show_cursor().ok();

    result
}

fn run_app(
    terminal: &mut Terminal<CrosstermBackend<io::Stdout>>,
    app: &mut TraceViewerApp,
    client: ApiClient,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    loop {
        poll_pending_summary_load(app, &client, &handle)?;
        poll_pending_trace_load(app, &client, &handle)?;
        poll_pending_full_span_load(app, &client, &handle)?;
        poll_pending_thread_load(app);
        terminal.draw(|frame| draw_ui(frame, app))?;

        if event::poll(Duration::from_millis(200))? {
            match event::read()? {
                Event::Key(key) => {
                    if handle_key_event(app, key, &client, &handle)? {
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
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<bool> {
    match key.code {
        KeyCode::Char('q')
            if !(app.screen == Screen::Traces
                && app.trace_search_mode
                && key.modifiers.is_empty()) =>
        {
            return Ok(true)
        }
        KeyCode::Esc => {
            if app.screen == Screen::SpanDetail {
                app.screen = Screen::Traces;
                app.set_status(
                    "Back to traces. Up/Down: move  Enter: open trace  /: search  r: refresh  q: quit",
                );
                return Ok(false);
            }
            if app.screen == Screen::Traces && app.trace_search_mode {
                app.trace_search_mode = false;
                app.trace_search_input = app.trace_search_query.clone();
                app.set_status("Search canceled");
                return Ok(false);
            }
            return Ok(true);
        }
        KeyCode::Char('c') if key.modifiers.contains(KeyModifiers::CONTROL) => return Ok(true),
        _ => {}
    }

    match app.screen {
        Screen::Traces => handle_traces_key(app, key, client, handle)?,
        Screen::SpanDetail => handle_span_detail_key(app, key, client, handle)?,
    }

    Ok(false)
}

fn handle_traces_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if app.trace_search_mode {
        return handle_trace_search_input_key(app, key, client, handle);
    }

    match key.code {
        KeyCode::Char('/') => {
            app.trace_search_mode = true;
            app.trace_search_input = app.trace_search_query.clone();
            app.set_status("Search mode. Type to edit, Enter applies, Esc cancels.");
        }
        KeyCode::Up | KeyCode::Char('k') => app.move_trace_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_trace_down(),
        KeyCode::Enter | KeyCode::Right => request_selected_trace_load(app, client, handle)?,
        KeyCode::Char('r') => request_refresh_traces(app, client, handle)?,
        _ => {}
    }
    Ok(())
}

fn handle_trace_search_input_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    match key.code {
        KeyCode::Esc => {
            app.trace_search_mode = false;
            app.trace_search_input = app.trace_search_query.clone();
            app.set_status("Search canceled");
        }
        KeyCode::Enter => {
            apply_trace_search(app, client, handle)?;
        }
        KeyCode::Backspace => {
            app.trace_search_input.pop();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.trace_search_input.clear();
        }
        KeyCode::Char(c) if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT => {
            app.trace_search_input.push(c);
        }
        _ => {}
    }

    Ok(())
}

fn apply_trace_search(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let next_query = app.trace_search_input.trim().to_string();
    app.trace_search_mode = false;
    app.trace_search_query = next_query;
    app.selected_trace = 0;

    request_refresh_traces(app, client, handle)
}

fn handle_span_detail_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            if app.detail_pane_focus == DetailPaneFocus::Tree {
                app.move_span_up();
                if app.detail_view == DetailView::Span {
                    request_selected_span_load(app, client, handle, false)?;
                }
            } else if app.detail_view == DetailView::Thread {
                app.move_thread_message_up();
            } else {
                app.scroll_detail_up(1);
            }
        }
        KeyCode::Down | KeyCode::Char('j') => {
            if app.detail_pane_focus == DetailPaneFocus::Tree {
                app.move_span_down();
                if app.detail_view == DetailView::Span {
                    request_selected_span_load(app, client, handle, false)?;
                }
            } else if app.detail_view == DetailView::Thread {
                app.move_thread_message_down();
            } else {
                app.scroll_detail_down(1);
            }
        }
        KeyCode::PageUp => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                app.scroll_detail_up(10);
            }
        }
        KeyCode::PageDown => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                app.scroll_detail_down(10);
            }
        }
        KeyCode::Backspace => {
            app.screen = Screen::Traces;
            app.set_status(
                "Back to traces. Up/Down: move  Enter: open trace  /: search  r: refresh  q: quit",
            );
        }
        KeyCode::Left => {
            app.detail_pane_focus = DetailPaneFocus::Tree;
            app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
        }
        KeyCode::Right => {
            app.detail_pane_focus = DetailPaneFocus::Detail;
            app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
        }
        KeyCode::Enter => {
            if app.detail_view == DetailView::Span {
                request_selected_span_load(app, client, handle, true)?;
            } else if app.thread_loading || app.thread_messages.is_none() {
                request_thread_messages_load(app, client, handle, true)?;
            } else {
                app.toggle_selected_thread_message();
            }
        }
        KeyCode::Char('t') => {
            app.detail_scroll = 0;
            app.detail_view = if app.detail_view == DetailView::Span {
                DetailView::Thread
            } else {
                DetailView::Span
            };

            if app.detail_view == DetailView::Thread {
                app.detail_pane_focus = DetailPaneFocus::Detail;
                request_thread_messages_load(app, client, handle, false)?;
            } else {
                app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
                request_selected_span_load(app, client, handle, false)?;
            }
        }
        KeyCode::Char('r') => {
            if app.loaded_root_span_id.is_some() {
                request_loaded_trace_reload(app, client, handle)?;
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                app.scroll_detail_up(10)
            }
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                app.scroll_detail_down(10)
            }
        }
        _ => {}
    }
    Ok(())
}

fn request_refresh_traces(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let search_query = app.trace_search_query.trim().to_string();
    let previous_root = app.current_trace().map(|r| r.root_span_id.clone());

    if app.summary_load_rx.is_some() {
        app.summary_pending_refresh = Some((search_query.clone(), previous_root));
        if search_query.is_empty() {
            app.set_status("Queued trace refresh...");
        } else {
            app.set_status(format!("Queued search refresh for '{search_query}'..."));
        }
        return Ok(());
    }

    start_summary_load(app, client, handle, search_query, previous_root);
    Ok(())
}

fn start_summary_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    search_query: String,
    previous_root: Option<String>,
) {
    let (tx, rx) = mpsc::channel();
    let client = client.clone();
    let project_id = app.project.id.clone();
    let preview_length = app.preview_length;
    let limit = app.limit;
    let print_queries = app.print_queries;
    let search_for_task = search_query.clone();
    let previous_root_for_task = previous_root.clone();

    handle.spawn(async move {
        let search_opt = if search_for_task.trim().is_empty() {
            None
        } else {
            Some(search_for_task.as_str())
        };
        let result = fetch_summary_rows(
            &client,
            &project_id,
            preview_length,
            limit,
            search_opt,
            print_queries,
        )
        .await;
        let _ = tx.send((search_for_task, previous_root_for_task, result));
    });

    app.summary_loading_search_query = search_query.clone();
    app.summary_load_started_at = Some(Instant::now());
    app.summary_spinner_tick = 0;
    app.summary_load_rx = Some(rx);
    if app.screen == Screen::Traces {
        if search_query.is_empty() {
            app.set_status("Refreshing trace table...");
        } else {
            app.set_status(format!("Searching traces for '{search_query}'..."));
        }
    }
}

fn apply_summary_rows(
    app: &mut TraceViewerApp,
    search_query: &str,
    previous_root: Option<String>,
    rows: Vec<Map<String, Value>>,
) {
    app.traces = parse_summary_rows(rows);
    if app.traces.is_empty() {
        app.selected_trace = 0;
        if app.screen == Screen::Traces {
            if search_query.trim().is_empty() {
                app.set_status("No traces found for the selected project");
            } else {
                app.set_status(format!("No traces found for search '{search_query}'"));
            }
        }
        return;
    }

    if let Some(root) = previous_root {
        if let Some(idx) = app.traces.iter().position(|row| row.root_span_id == root) {
            app.selected_trace = idx;
        } else {
            app.selected_trace = 0;
        }
    } else {
        app.selected_trace = app.selected_trace.min(app.traces.len().saturating_sub(1));
    }

    if app.screen == Screen::Traces {
        if search_query.trim().is_empty() {
            app.set_status(format!(
                "Loaded {} traces. Up/Down: move  Enter: open trace  /: search  r: refresh  q: quit",
                app.traces.len()
            ));
        } else {
            app.set_status(format!(
                "Loaded {} traces for '{search_query}'. Up/Down: move  Enter: open trace  /: search  r: refresh",
                app.traces.len()
            ));
        }
    }
}

fn poll_pending_summary_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if app.summary_load_rx.is_none() {
        if let Some((search_query, previous_root)) = app.summary_pending_refresh.take() {
            start_summary_load(app, client, handle, search_query, previous_root);
        }
        return Ok(());
    }

    app.summary_spinner_tick = app.summary_spinner_tick.wrapping_add(1);

    let recv_result = app.summary_load_rx.as_ref().map(|rx| rx.try_recv());
    let Some(recv_result) = recv_result else {
        return Ok(());
    };

    match recv_result {
        Ok((search_query, previous_root, result)) => {
            app.summary_load_rx = None;
            app.summary_load_started_at = None;
            app.summary_loading_search_query.clear();

            match result {
                Ok(rows) => apply_summary_rows(app, &search_query, previous_root, rows),
                Err(err) => {
                    let detail = root_error_message(&err);
                    if app.screen == Screen::Traces {
                        if let Some(query) = query_from_error(&err) {
                            app.set_error_with_query(
                                format!("Failed to refresh traces: {detail}"),
                                query,
                            );
                        } else {
                            app.set_error(format!("Failed to refresh traces: {detail}"));
                        }
                    }
                }
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            app.summary_load_rx = None;
            app.summary_load_started_at = None;
            app.summary_loading_search_query.clear();
            if app.screen == Screen::Traces {
                app.set_error("Failed to refresh traces: request channel closed");
            }
        }
    }

    if app.summary_load_rx.is_none() {
        if let Some((search_query, previous_root)) = app.summary_pending_refresh.take() {
            start_summary_load(app, client, handle, search_query, previous_root);
        }
    }

    Ok(())
}

fn request_selected_trace_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let Some(root_span_id) = app.current_trace().map(|t| t.root_span_id.clone()) else {
        app.set_error("No trace selected");
        return Ok(());
    };

    if root_span_id.is_empty() {
        app.set_error("Selected row does not have a root_span_id");
        return Ok(());
    }

    request_trace_load_for_root_span(app, client, handle, root_span_id)
}

fn request_loaded_trace_reload(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if let Some(root_span_id) = app.loaded_root_span_id.clone() {
        request_trace_load_for_root_span(app, client, handle, root_span_id)
    } else {
        request_selected_trace_load(app, client, handle)
    }
}

fn request_trace_load_for_root_span(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    root_span_id: String,
) -> Result<()> {
    if app
        .trace_loading_root_span_id
        .as_deref()
        .map(|id| id == root_span_id.as_str())
        .unwrap_or(false)
    {
        return Ok(());
    }

    if app.trace_loading_root_span_id.is_some() {
        app.trace_pending_root_span_id = Some(root_span_id.clone());
        app.set_status(format!("Queued trace load for {root_span_id}"));
        return Ok(());
    }

    start_trace_load_for_root_span(app, client, handle, root_span_id);
    Ok(())
}

fn start_trace_load_for_root_span(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    root_span_id: String,
) {
    let (tx, rx) = mpsc::channel();
    let client = client.clone();
    let project_id = app.project.id.clone();
    let print_queries = app.print_queries;
    let root_span_id_for_task = root_span_id.clone();

    handle.spawn(async move {
        let result = fetch_trace_span_rows_for_tree(
            &client,
            &project_id,
            &root_span_id_for_task,
            MAX_TRACE_SPANS,
            print_queries,
        )
        .await;
        let _ = tx.send((root_span_id_for_task, result));
    });

    app.trace_loading_root_span_id = Some(root_span_id.clone());
    app.trace_load_started_at = Some(Instant::now());
    app.trace_spinner_tick = 0;
    app.trace_load_rx = Some(rx);
    app.set_status(format!("Opening trace {root_span_id}..."));
}

fn apply_loaded_trace_rows(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    root_span_id: String,
    rows: Vec<Map<String, Value>>,
) -> Result<()> {
    let spans = build_span_entries(rows);
    if spans.is_empty() {
        app.set_status(format!("No spans found for trace {root_span_id}"));
        return Ok(());
    }

    app.spans = spans;
    app.full_span_cache.clear();
    app.full_span_loading_row_id = None;
    app.full_span_pending_row_id = None;
    app.full_span_load_started_at = None;
    app.full_span_spinner_tick = 0;
    app.full_span_load_rx = None;
    app.selected_span = 0;
    app.detail_scroll = 0;
    app.detail_view = DetailView::Span;
    app.detail_pane_focus = DetailPaneFocus::Tree;
    app.thread_messages = None;
    app.thread_selected_message = 0;
    app.thread_expanded.clear();
    app.thread_loading = false;
    app.thread_spinner_tick = 0;
    app.thread_load_rx = None;
    app.loaded_root_span_id = Some(root_span_id.clone());
    app.screen = Screen::SpanDetail;
    app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
    request_selected_span_load(app, client, handle, false)?;

    Ok(())
}

fn poll_pending_trace_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if app.trace_loading_root_span_id.is_none() {
        if let Some(next_root_span_id) = app.trace_pending_root_span_id.take() {
            if !next_root_span_id.is_empty() {
                start_trace_load_for_root_span(app, client, handle, next_root_span_id);
            }
        }
        return Ok(());
    }

    app.trace_spinner_tick = app.trace_spinner_tick.wrapping_add(1);

    if app.screen == Screen::SpanDetail
        && !app.status_is_error
        && app
            .trace_load_started_at
            .map(|start| start.elapsed() >= LOADER_DELAY)
            .unwrap_or(false)
    {
        if let Some(root_span_id) = app.trace_loading_root_span_id.as_deref() {
            app.set_status(format!(
                "Loading trace {root_span_id} {}",
                spinner_char(app.trace_spinner_tick)
            ));
        }
    }

    let recv_result = app.trace_load_rx.as_ref().map(|rx| rx.try_recv());
    let Some(recv_result) = recv_result else {
        return Ok(());
    };

    match recv_result {
        Ok((root_span_id, result)) => {
            app.trace_load_rx = None;
            app.trace_loading_root_span_id = None;
            app.trace_load_started_at = None;

            if let Some(next_root_span_id) = app.trace_pending_root_span_id.take() {
                if !next_root_span_id.is_empty() && next_root_span_id != root_span_id {
                    start_trace_load_for_root_span(app, client, handle, next_root_span_id);
                    return Ok(());
                }
            }

            match result {
                Ok(rows) => {
                    apply_loaded_trace_rows(app, client, handle, root_span_id, rows)?;
                }
                Err(err) => {
                    let detail = root_error_message(&err);
                    if let Some(query) = query_from_error(&err) {
                        app.set_error_with_query(
                            format!("Failed to load trace {root_span_id}: {detail}"),
                            query,
                        );
                    } else {
                        app.set_error(format!("Failed to load trace {root_span_id}: {detail}"));
                    }
                }
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            app.trace_load_rx = None;
            app.trace_loading_root_span_id = None;
            app.trace_load_started_at = None;
            app.set_error("Failed to load trace: request channel closed");
        }
    }

    if app.trace_loading_root_span_id.is_none() {
        if let Some(next_root_span_id) = app.trace_pending_root_span_id.take() {
            if !next_root_span_id.is_empty() {
                start_trace_load_for_root_span(app, client, handle, next_root_span_id);
            }
        }
    }

    Ok(())
}

fn draw_ui(frame: &mut Frame<'_>, app: &TraceViewerApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),
            Constraint::Min(5),
            Constraint::Length(3),
        ])
        .split(frame.area());

    let header_title = match app.screen {
        Screen::Traces => "Trace Explorer",
        Screen::SpanDetail => "Trace Explorer / Detail",
    };
    let header = Paragraph::new(Line::from(vec![
        Span::styled(header_title, Style::default().add_modifier(Modifier::BOLD)),
        Span::styled("  ·  ", Style::default().fg(Color::DarkGray)),
        Span::styled(
            project_label(&app.project),
            Style::default().fg(Color::Gray),
        ),
    ]))
    .block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title("bt traces"),
    );
    frame.render_widget(header, chunks[0]);

    match app.screen {
        Screen::Traces => draw_traces_table(frame, chunks[1], app),
        Screen::SpanDetail => draw_span_detail_view(frame, chunks[1], app),
    }

    let status_style = if app.status_is_error {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else {
        Style::default().fg(Color::Gray)
    };

    let mut lines = vec![Line::from(Span::styled(app.status.as_str(), status_style))];
    if !app.status_detail.is_empty() {
        lines.push(Line::from(Span::styled(
            app.status_detail.as_str(),
            Style::default().fg(Color::Red),
        )));
    }

    let status = Paragraph::new(lines).block(
        Block::default()
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded),
    );

    frame.render_widget(status, chunks[2]);
}

fn draw_traces_table(frame: &mut Frame<'_>, area: ratatui::layout::Rect, app: &TraceViewerApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(3), Constraint::Min(3)])
        .split(area);
    let summary_loading = app
        .summary_load_started_at
        .map(|start| start.elapsed() >= LOADER_DELAY)
        .unwrap_or(false);
    let summary_spinner = spinner_char(app.summary_spinner_tick);

    let search_text = if app.trace_search_mode {
        app.trace_search_input.as_str()
    } else {
        app.trace_search_query.as_str()
    };
    let search_line = if search_text.trim().is_empty() {
        Line::from(Span::styled(
            "Press / to search traces",
            Style::default().fg(Color::DarkGray),
        ))
    } else if app.trace_search_mode {
        Line::from(Span::raw(format!("{search_text}█")))
    } else {
        Line::from(Span::raw(search_text))
    };
    let search_block = Block::default()
        .title(if app.trace_search_mode {
            "Search [active]".to_string()
        } else if summary_loading {
            format!("Search [loading {summary_spinner}]")
        } else {
            "Search".to_string()
        })
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(if app.trace_search_mode || summary_loading {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        })
        .title_bottom(if app.trace_search_mode {
            "Enter apply  Esc cancel  Ctrl+u clear"
        } else if summary_loading {
            let search = app.summary_loading_search_query.trim();
            if search.is_empty() {
                "Refreshing traces..."
            } else {
                "Searching..."
            }
        } else {
            "/ edit search"
        });
    let search_widget = Paragraph::new(search_line).block(search_block);
    frame.render_widget(search_widget, chunks[0]);

    let header = Row::new(vec!["Created", "Trace", "Input", "Duration"]).style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row<'_>> = app
        .traces
        .iter()
        .map(|trace| {
            let created = preview_string(trace.row.get("created"), 24);
            let trace_id = preview_string(Some(&Value::String(trace.root_span_id.clone())), 24);
            let input = preview_string(trace.row.get("input"), 80);
            let duration = format_duration(trace.row.get("metrics"));

            Row::new(vec![
                Cell::from(created),
                Cell::from(trace_id),
                Cell::from(input),
                Cell::from(duration),
            ])
        })
        .collect();

    let table = Table::new(
        rows,
        [
            Constraint::Length(22),
            Constraint::Length(24),
            Constraint::Min(30),
            Constraint::Length(12),
        ],
    )
    .header(header)
    .block(
        Block::default()
            .title(if app.trace_search_query.trim().is_empty() {
                "Recent Traces"
            } else {
                "Recent Traces [filtered]"
            })
            .borders(Borders::ALL)
            .border_type(BorderType::Rounded)
            .title_bottom(
                if app
                    .trace_load_started_at
                    .map(|start| start.elapsed() >= LOADER_DELAY)
                    .unwrap_or(false)
                {
                    if let Some(root_span_id) = app.trace_loading_root_span_id.as_deref() {
                        format!(
                            "Loading trace {root_span_id} {}",
                            spinner_char(app.trace_spinner_tick)
                        )
                    } else {
                        "Enter opens selected trace  / search  r refresh".to_string()
                    }
                } else {
                    "Enter opens selected trace  / search  r refresh".to_string()
                },
            ),
    )
    .row_highlight_style(
        Style::default()
            .bg(Color::Rgb(42, 47, 56))
            .add_modifier(Modifier::BOLD),
    );

    let mut state = TableState::default();
    if !app.traces.is_empty() {
        state.select(Some(app.selected_trace));
    }

    frame.render_stateful_widget(table, chunks[1], &mut state);
}

fn draw_span_detail_view(frame: &mut Frame<'_>, area: ratatui::layout::Rect, app: &TraceViewerApp) {
    let chunks = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    let items: Vec<ListItem<'_>> = app
        .spans
        .iter()
        .map(|span| {
            let indent = "  ".repeat(span.depth.min(16));
            ListItem::new(format!("{indent}{}", span.label))
        })
        .collect();

    let tree_is_focused = app.detail_pane_focus == DetailPaneFocus::Tree;
    let detail_is_focused = app.detail_pane_focus == DetailPaneFocus::Detail;
    let tree_title = if tree_is_focused {
        "Span Tree [active]"
    } else {
        "Span Tree"
    };

    let list = List::new(items)
        .block(
            Block::default()
                .title(tree_title)
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded)
                .border_style(if tree_is_focused {
                    Style::default()
                        .fg(Color::Cyan)
                        .add_modifier(Modifier::BOLD)
                } else {
                    Style::default()
                })
                .title_bottom("Left/Right switch pane  t toggles thread/span  Backspace/Esc back"),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(42, 47, 56))
                .add_modifier(Modifier::BOLD),
        );

    let mut list_state = ListState::default();
    if !app.spans.is_empty() {
        list_state.select(Some(app.selected_span));
    }
    frame.render_stateful_widget(list, chunks[0], &mut list_state);

    let detail_title = match app.detail_view {
        DetailView::Span => "Span Detail",
        DetailView::Thread => "Thread View",
    };
    let detail_title_with_focus = if detail_is_focused {
        format!("{detail_title} [active]")
    } else {
        detail_title.to_string()
    };
    let detail_block = Block::default()
        .title(detail_title_with_focus)
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(if detail_is_focused {
            Style::default()
                .fg(Color::Cyan)
                .add_modifier(Modifier::BOLD)
        } else {
            Style::default()
        });

    match app.detail_view {
        DetailView::Span => {
            let detail_text = app
                .current_span()
                .map(|span| {
                    let full_row = app.full_span_cache.get(&span.id);
                    let is_loading = app
                        .full_span_loading_row_id
                        .as_deref()
                        .map(|id| id == span.id)
                        .unwrap_or(false);
                    let loading_spinner = if is_loading
                        && app
                            .full_span_load_started_at
                            .map(|start| start.elapsed() >= LOADER_DELAY)
                            .unwrap_or(false)
                    {
                        Some(spinner_char(app.full_span_spinner_tick))
                    } else {
                        None
                    };
                    render_span_details(
                        span,
                        full_row.unwrap_or(&span.row),
                        full_row.is_some(),
                        is_loading,
                        loading_spinner,
                    )
                })
                .unwrap_or_else(|| "No span selected".to_string());

            let detail = Paragraph::new(detail_text)
                .block(detail_block)
                .scroll((app.detail_scroll, 0))
                .wrap(Wrap { trim: false });
            frame.render_widget(detail, chunks[1]);
        }
        DetailView::Thread => {
            if app.thread_loading {
                let detail = Paragraph::new(render_thread_loading_state(app.thread_spinner_tick))
                    .block(detail_block)
                    .scroll((app.detail_scroll, 0))
                    .wrap(Wrap { trim: false });
                frame.render_widget(detail, chunks[1]);
            } else if let Some(messages) = &app.thread_messages {
                draw_thread_messages_list(frame, chunks[1], app, messages, detail_block);
            } else {
                let detail = Paragraph::new(
                    "Thread not loaded. Press `t` to load the thread view (or Enter to retry).",
                )
                .block(detail_block)
                .scroll((app.detail_scroll, 0))
                .wrap(Wrap { trim: false });
                frame.render_widget(detail, chunks[1]);
            }
        }
    }
}

fn parse_summary_rows(rows: Vec<Map<String, Value>>) -> Vec<TraceSummaryRow> {
    rows.into_iter()
        .map(|row| {
            let root_span_id = value_as_string(row.get("root_span_id"))
                .or_else(|| value_as_string(row.get("id")))
                .unwrap_or_default();

            TraceSummaryRow { root_span_id, row }
        })
        .collect()
}

fn build_span_entries(rows: Vec<Map<String, Value>>) -> Vec<SpanListEntry> {
    #[derive(Debug, Clone)]
    struct TempSpan {
        id: String,
        span_id: String,
        root_span_id: String,
        parent_span_id: Option<String>,
        label: String,
        pagination_key: String,
        xact_id: String,
        start_time: Option<f64>,
        exec_counter: i64,
        row: Map<String, Value>,
    }

    let mut temp_spans: HashMap<String, TempSpan> = HashMap::new();
    for row in rows {
        let id = value_as_string(row.get("id")).unwrap_or_default();
        let span_id = value_as_string(row.get("span_id")).unwrap_or_else(|| id.clone());
        if span_id.is_empty() {
            continue;
        }

        let root_span_id =
            value_as_string(row.get("root_span_id")).unwrap_or_else(|| span_id.clone());
        let parent_span_id = extract_parent_span_id(&row);
        let pagination_key = value_as_string(row.get("_pagination_key"))
            .or_else(|| value_as_string(row.get("created")))
            .unwrap_or_default();
        let xact_id = value_as_string(row.get("_xact_id")).unwrap_or_default();
        let start_time = extract_start_time(&row);
        let exec_counter = extract_exec_counter(&row);

        let label = span_label(&row, &span_id);

        temp_spans.insert(
            span_id.clone(),
            TempSpan {
                id,
                span_id,
                root_span_id,
                parent_span_id,
                label,
                pagination_key,
                xact_id,
                start_time,
                exec_counter,
                row,
            },
        );
    }

    if temp_spans.is_empty() {
        return Vec::new();
    }

    let root_span_id_hint = temp_spans
        .values()
        .next()
        .map(|s| s.root_span_id.clone())
        .unwrap_or_default();

    let root_span_id = temp_spans
        .values()
        .find(|span| span.span_id == span.root_span_id)
        .map(|span| span.span_id.clone())
        .or_else(|| {
            temp_spans
                .values()
                .find(|span| span.span_id == root_span_id_hint)
                .map(|span| span.span_id.clone())
        })
        .or_else(|| {
            temp_spans
                .values()
                .find(|span| span.parent_span_id.is_none())
                .map(|span| span.span_id.clone())
        })
        .or_else(|| temp_spans.keys().next().cloned())
        .unwrap_or_default();

    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    for span in temp_spans.values() {
        if span.span_id == root_span_id {
            continue;
        }

        let parent_key = match &span.parent_span_id {
            Some(parent)
                if parent != &span.span_id
                    && temp_spans.contains_key(parent)
                    && parent != &root_span_id =>
            {
                parent.clone()
            }
            _ => root_span_id.clone(),
        };

        children
            .entry(parent_key)
            .or_default()
            .push(span.span_id.clone());
    }

    let compare_span_ids = |a: &str, b: &str| {
        let left = temp_spans.get(a);
        let right = temp_spans.get(b);

        let Some(left) = left else {
            return a.cmp(b);
        };
        let Some(right) = right else {
            return a.cmp(b);
        };

        match (left.start_time, right.start_time) {
            (Some(a_start), Some(b_start)) => {
                if let Some(ord) = a_start.partial_cmp(&b_start) {
                    if ord != Ordering::Equal {
                        return ord;
                    }
                }
            }
            (Some(_), None) => return Ordering::Less,
            (None, Some(_)) => return Ordering::Greater,
            (None, None) => {}
        }

        let exec_counter_cmp = left.exec_counter.cmp(&right.exec_counter);
        if exec_counter_cmp != Ordering::Equal {
            return exec_counter_cmp;
        }

        let pagination_cmp = left.pagination_key.cmp(&right.pagination_key);
        if pagination_cmp != Ordering::Equal {
            return pagination_cmp;
        }

        let xact_cmp = left.xact_id.cmp(&right.xact_id);
        if xact_cmp != Ordering::Equal {
            return xact_cmp;
        }

        left.span_id.cmp(&right.span_id)
    };

    for child_ids in children.values_mut() {
        child_ids.sort_by(|a, b| compare_span_ids(a, b));
    }

    let mut ordered: Vec<SpanListEntry> = Vec::new();
    let mut visited: HashSet<String> = HashSet::new();

    fn visit(
        span_id: &str,
        depth: usize,
        temp_spans: &HashMap<String, TempSpan>,
        children: &HashMap<String, Vec<String>>,
        ordered: &mut Vec<SpanListEntry>,
        visited: &mut HashSet<String>,
    ) {
        if !visited.insert(span_id.to_string()) {
            return;
        }

        let Some(span) = temp_spans.get(span_id) else {
            return;
        };

        ordered.push(SpanListEntry {
            id: span.id.clone(),
            span_id: span.span_id.clone(),
            root_span_id: span.root_span_id.clone(),
            depth,
            label: span.label.clone(),
            row: span.row.clone(),
        });

        if let Some(kids) = children.get(span_id) {
            for child in kids {
                visit(child, depth + 1, temp_spans, children, ordered, visited);
            }
        }
    }

    if !root_span_id.is_empty() {
        visit(
            &root_span_id,
            0,
            &temp_spans,
            &children,
            &mut ordered,
            &mut visited,
        );
    }

    // Handle disconnected spans/cycles by appending anything not visited.
    let mut leftovers: Vec<String> = temp_spans
        .keys()
        .filter(|span_id| !visited.contains(*span_id))
        .cloned()
        .collect();

    leftovers.sort_by(|a, b| compare_span_ids(a, b));

    for span_id in leftovers {
        visit(
            &span_id,
            0,
            &temp_spans,
            &children,
            &mut ordered,
            &mut visited,
        );
    }

    ordered
}

fn extract_start_time(row: &Map<String, Value>) -> Option<f64> {
    let metrics = value_as_object_owned(row.get("metrics"))?;
    match metrics.get("start")? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn extract_exec_counter(row: &Map<String, Value>) -> i64 {
    let span_attributes = match value_as_object_owned(row.get("span_attributes")) {
        Some(v) => v,
        None => return 0,
    };

    match span_attributes.get("exec_counter") {
        Some(Value::Number(n)) => n.as_i64().unwrap_or(0),
        Some(Value::String(s)) => s.parse::<i64>().unwrap_or(0),
        _ => 0,
    }
}

fn extract_parent_span_id(row: &Map<String, Value>) -> Option<String> {
    match row.get("span_parents") {
        Some(Value::Array(values)) => values.first().and_then(|v| value_as_string(Some(v))),
        Some(Value::String(s)) => {
            let parsed = serde_json::from_str::<Value>(s).ok()?;
            if let Value::Array(values) = parsed {
                values.first().and_then(|v| value_as_string(Some(v)))
            } else {
                None
            }
        }
        _ => None,
    }
}

fn span_label(row: &Map<String, Value>, span_id: &str) -> String {
    let span_attributes = value_as_object_owned(row.get("span_attributes")).unwrap_or_default();

    let name = value_as_string(span_attributes.get("name")).unwrap_or_else(|| span_id.to_string());
    let kind = value_as_string(span_attributes.get("type")).unwrap_or_else(|| "span".to_string());

    let duration = format_duration(row.get("metrics"));
    let has_error = row
        .get("error")
        .map(|v| match v {
            Value::Null => false,
            Value::String(s) => !s.is_empty(),
            _ => true,
        })
        .unwrap_or(false);

    if has_error {
        format!("{name} [{kind}] {duration} !")
    } else {
        format!("{name} [{kind}] {duration}")
    }
}

fn render_span_details(
    span: &SpanListEntry,
    row: &Map<String, Value>,
    is_full: bool,
    is_loading: bool,
    loading_spinner: Option<char>,
) -> String {
    let mut out = String::new();

    if !is_full {
        if is_loading {
            if let Some(spinner) = loading_spinner {
                out.push_str(&format!("FULL CONTENT: loading {spinner}\n\n"));
            } else {
                out.push_str("FULL CONTENT: loading\n\n");
            }
        } else {
            out.push_str(
                "FULL CONTENT: not loaded (loading on selection; press Enter to retry)\n\n",
            );
        }
    }

    out.push_str(&format!("span_id: {}\n", span.span_id));
    out.push_str(&format!("row_id: {}\n", span.id));
    out.push_str(&format!("root_span_id: {}\n", span.root_span_id));
    out.push_str(&format!(
        "created: {}\n",
        value_as_string(row.get("created")).unwrap_or_else(|| "-".to_string())
    ));
    out.push_str(&format!(
        "duration: {}\n",
        format_duration(row.get("metrics"))
    ));

    if let Some(attrs) = value_as_object_owned(row.get("span_attributes")) {
        if let Some(name) = value_as_string(attrs.get("name")) {
            out.push_str(&format!("name: {name}\n"));
        }
        if let Some(kind) = value_as_string(attrs.get("type")) {
            out.push_str(&format!("type: {kind}\n"));
        }
    }

    append_section(&mut out, "input", row.get("input"));
    append_section(&mut out, "output", row.get("output"));
    append_section(&mut out, "expected", row.get("expected"));
    append_section(&mut out, "error", row.get("error"));
    append_section(&mut out, "tags", row.get("tags"));
    append_section(&mut out, "scores", row.get("scores"));
    append_section(&mut out, "metrics", row.get("metrics"));
    append_section(&mut out, "metadata", row.get("metadata"));
    append_section(&mut out, "span_attributes", row.get("span_attributes"));

    out
}

fn render_thread_loading_state(tick: usize) -> String {
    let spinner = spinner_char(tick);

    format!(
        "Loading thread {spinner}\n\nRunning preprocessor `{THREAD_PREPROCESSOR_NAME}` with `mode: json`.\nLarge traces can take a few seconds."
    )
}

fn spinner_char(tick: usize) -> char {
    match tick % 4 {
        0 => '|',
        1 => '/',
        2 => '-',
        _ => '\\',
    }
}

fn role_display_name(role: &str) -> &str {
    match role {
        "user" => "User",
        "assistant" => "Assistant",
        "system" => "System",
        "tool" => "Tool",
        "developer" => "Developer",
        "model" => "Model",
        _ => "Message",
    }
}

fn render_message_content(content: &Value) -> String {
    match content {
        Value::Null => String::new(),
        Value::String(s) => s.clone(),
        Value::Array(items) => {
            let mut parts: Vec<String> = Vec::new();
            for item in items {
                match item {
                    Value::Object(obj) => {
                        let item_type = value_as_string(obj.get("type")).unwrap_or_default();
                        if item_type == "text" {
                            if let Some(text) = value_as_string(obj.get("text")) {
                                parts.push(text);
                            }
                            continue;
                        }

                        if item_type == "tool_call" {
                            let name = value_as_string(obj.get("name"))
                                .or_else(|| value_as_string(obj.get("tool_name")))
                                .unwrap_or_else(|| "tool_call".to_string());
                            let args = obj.get("arguments").or_else(|| obj.get("input"));
                            if let Some(args) = args {
                                parts.push(format!(
                                    "[Tool Call] {name}\n{}",
                                    format_pretty_value(Some(args))
                                ));
                            } else {
                                parts.push(format!("[Tool Call] {name}"));
                            }
                            continue;
                        }

                        parts.push(
                            serde_json::to_string_pretty(item).unwrap_or_else(|_| item.to_string()),
                        );
                    }
                    _ => parts.push(
                        serde_json::to_string_pretty(item).unwrap_or_else(|_| item.to_string()),
                    ),
                }
            }
            parts.join("\n\n")
        }
        other => serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string()),
    }
}

fn render_tool_calls_inline(tool_calls: &Value) -> String {
    match tool_calls {
        Value::Array(calls) => {
            let mut out = String::new();
            for call in calls {
                if let Value::Object(obj) = call {
                    let name = value_as_string(obj.get("name"))
                        .or_else(|| {
                            value_as_object_owned(obj.get("function"))
                                .and_then(|f| value_as_string(f.get("name")))
                        })
                        .or_else(|| value_as_string(obj.get("tool_name")))
                        .unwrap_or_else(|| "tool_call".to_string());
                    out.push_str(&format!("[Tool Call] {name}\n"));

                    let args = obj
                        .get("arguments")
                        .cloned()
                        .or_else(|| obj.get("input").cloned())
                        .or_else(|| {
                            value_as_object_owned(obj.get("function"))
                                .and_then(|f| f.get("arguments").cloned())
                        });

                    if let Some(args) = args.as_ref() {
                        out.push_str(&format_pretty_value(Some(args)));
                        out.push('\n');
                    }
                    out.push('\n');
                } else {
                    out.push_str(&format_pretty_value(Some(call)));
                    out.push('\n');
                }
            }
            out.trim_end().to_string()
        }
        other => format_pretty_value(Some(other)),
    }
}

fn thread_role_style(role: &str) -> Style {
    match role {
        "user" => Style::default().fg(Color::Cyan),
        "assistant" => Style::default().fg(Color::Green),
        "tool" => Style::default().fg(Color::Yellow),
        "system" | "developer" | "model" => Style::default().fg(Color::Magenta),
        _ => Style::default().fg(Color::Gray),
    }
}

fn truncate_for_preview(text: &str, max_lines: usize, max_chars: usize) -> (String, bool) {
    let mut lines_out: Vec<String> = Vec::new();
    let mut truncated = false;
    let mut seen_lines = 0usize;

    for line in text.lines() {
        seen_lines += 1;
        if lines_out.len() >= max_lines {
            truncated = true;
            break;
        }

        let clipped = truncate_chars(line, max_chars);
        if clipped.chars().count() < line.chars().count() {
            truncated = true;
        }
        lines_out.push(clipped);
    }

    if seen_lines > max_lines {
        truncated = true;
    }

    (lines_out.join("\n"), truncated)
}

fn draw_thread_messages_list(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &TraceViewerApp,
    messages: &[Value],
    detail_block: Block<'_>,
) {
    if messages.is_empty() {
        let detail = Paragraph::new("No messages returned by preprocessor.")
            .block(detail_block)
            .wrap(Wrap { trim: false });
        frame.render_widget(detail, area);
        return;
    }

    let items: Vec<ListItem<'_>> = messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let (role, content, tool_calls) = match message {
                Value::Object(obj) => (
                    value_as_string(obj.get("role")).unwrap_or_else(|| "message".to_string()),
                    obj.get("content"),
                    obj.get("tool_calls"),
                ),
                _ => ("message".to_string(), Some(message), None),
            };

            let expanded = app.thread_expanded.contains(&index);
            let arrow = if expanded { "▾" } else { "▸" };
            let role_display = role_display_name(&role);

            let mut lines: Vec<Line<'_>> = vec![Line::from(vec![
                Span::styled(
                    format!("{arrow}  #{:<3}", index + 1),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    role_display.to_string(),
                    thread_role_style(&role).add_modifier(Modifier::BOLD),
                ),
            ])];

            let content_text = content.map(render_message_content).unwrap_or_default();
            let normalized_content = if content_text.trim().is_empty() {
                "(empty)".to_string()
            } else {
                content_text.trim_end().to_string()
            };
            let (shown_content, content_truncated) = if expanded {
                (normalized_content, false)
            } else {
                truncate_for_preview(&normalized_content, 4, 160)
            };

            for line in shown_content.lines() {
                lines.push(Line::from(Span::raw(format!("    {line}"))));
            }
            if content_truncated {
                lines.push(Line::from(Span::styled(
                    "    ...",
                    Style::default().fg(Color::DarkGray),
                )));
            }

            if let Some(calls) = tool_calls {
                let tool_calls_text = render_tool_calls_inline(calls);
                if !tool_calls_text.trim().is_empty() {
                    lines.push(Line::from(Span::styled(
                        "    Tool calls:",
                        Style::default().fg(Color::DarkGray),
                    )));
                    let (shown_tools, tools_truncated) = if expanded {
                        (tool_calls_text, false)
                    } else {
                        truncate_for_preview(&tool_calls_text, 3, 140)
                    };
                    for line in shown_tools.lines() {
                        lines.push(Line::from(Span::raw(format!("      {line}"))));
                    }
                    if tools_truncated {
                        lines.push(Line::from(Span::styled(
                            "      ...",
                            Style::default().fg(Color::DarkGray),
                        )));
                    }
                }
            }

            if !expanded {
                lines.push(Line::from(Span::styled(
                    "    Enter to expand",
                    Style::default().fg(Color::DarkGray),
                )));
            } else {
                lines.push(Line::from(Span::styled(
                    "    Enter to collapse",
                    Style::default().fg(Color::DarkGray),
                )));
            }
            lines.push(Line::from(""));

            ListItem::new(lines)
        })
        .collect();

    let list = List::new(items)
        .block(detail_block.title_bottom(
            "Up/Down: select message  Enter: expand/collapse  Left: span tree  t: span/thread",
        ))
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▌ ");

    let mut state = ListState::default();
    state.select(Some(
        app.thread_selected_message
            .min(messages.len().saturating_sub(1)),
    ));
    frame.render_stateful_widget(list, area, &mut state);
}

fn span_detail_status(view: DetailView, pane_focus: DetailPaneFocus) -> &'static str {
    match (view, pane_focus) {
        (DetailView::Span, DetailPaneFocus::Tree) => {
            "Span view (tree focus). Right: detail pane  Up/Down: select span  t: thread  Backspace/Esc: back"
        }
        (DetailView::Span, DetailPaneFocus::Detail) => {
            "Span view (detail focus). Left: tree pane  Up/Down: scroll  Enter: load span  t: thread  Backspace/Esc: back"
        }
        (DetailView::Thread, DetailPaneFocus::Tree) => {
            "Thread view (tree focus). Right: detail pane  Up/Down: select span  Enter: toggle/retry  t: span  Backspace/Esc: back"
        }
        (DetailView::Thread, DetailPaneFocus::Detail) => {
            "Thread view (detail focus). Left: tree pane  Up/Down: select message  Enter: expand/collapse (or retry)  t: span  Backspace/Esc: back"
        }
    }
}

fn append_section(out: &mut String, title: &str, value: Option<&Value>) {
    out.push('\n');
    out.push_str(&title.to_uppercase());
    out.push('\n');
    out.push_str(&"-".repeat(title.len().max(8)));
    out.push('\n');
    out.push_str(&format_pretty_value(value));
    out.push('\n');
}

fn format_pretty_value(value: Option<&Value>) -> String {
    match value {
        None => "-".to_string(),
        Some(Value::Null) => "null".to_string(),
        Some(Value::String(s)) => {
            if s.is_empty() {
                "".to_string()
            } else if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                serde_json::to_string_pretty(&parsed).unwrap_or_else(|_| s.clone())
            } else {
                s.clone()
            }
        }
        Some(other) => serde_json::to_string_pretty(other).unwrap_or_else(|_| other.to_string()),
    }
}

fn preview_string(value: Option<&Value>, max_chars: usize) -> String {
    let mut s = match value {
        None => "-".to_string(),
        Some(Value::String(text)) => text.clone(),
        Some(other) => serde_json::to_string(other).unwrap_or_else(|_| other.to_string()),
    };

    s = s.replace('\n', " ");
    truncate_chars(&s, max_chars)
}

fn truncate_chars(s: &str, max_chars: usize) -> String {
    if max_chars == 0 {
        return String::new();
    }

    let total = s.chars().count();
    if total <= max_chars {
        return s.to_string();
    }

    let take = max_chars.saturating_sub(1);
    let mut out = String::new();
    for ch in s.chars().take(take) {
        out.push(ch);
    }
    out.push('…');
    out
}

fn format_duration(metrics: Option<&Value>) -> String {
    let Some(metrics_obj) = value_as_object_owned(metrics) else {
        return "-".to_string();
    };

    let Some(duration) = metrics_obj.get("duration") else {
        return "-".to_string();
    };

    if let Some(seconds) = duration.as_f64() {
        return format!("{seconds:.3}s");
    }

    if let Some(text) = duration.as_str() {
        return format!("{text}s");
    }

    "-".to_string()
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    let value = value?;
    match value {
        Value::String(s) => Some(s.clone()),
        Value::Number(_) | Value::Bool(_) => Some(value.to_string()),
        _ => None,
    }
}

fn value_as_object_owned(value: Option<&Value>) -> Option<Map<String, Value>> {
    match value? {
        Value::Object(map) => Some(map.clone()),
        Value::String(s) => match serde_json::from_str::<Value>(s).ok()? {
            Value::Object(map) => Some(map),
            _ => None,
        },
        _ => None,
    }
}

fn project_label(project: &ProjectSelection) -> String {
    match &project.name {
        Some(name) => format!("{} ({name})", project.id),
        None => project.id.clone(),
    }
}

fn build_summary_query(
    project_id: &str,
    preview_length: usize,
    limit: usize,
    cursor: Option<&str>,
    search_query: Option<&str>,
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    let filter_clause = build_summary_filter(search_query);
    format!(
        "select: * | from: project_logs({}) summary | filter: {} | preview_length: {} | sort: _pagination_key DESC | limit: {}{}",
        sql_quote(project_id),
        filter_clause,
        preview_length,
        limit,
        cursor_clause,
    )
}

fn build_summary_filter(search_query: Option<&str>) -> String {
    let base = "created >= NOW() - INTERVAL 3 DAY";
    let Some(search) = search_query.map(str::trim).filter(|s| !s.is_empty()) else {
        return base.to_string();
    };

    let match_term = sql_quote(search);
    let match_clause = TRACE_TEXT_SEARCH_FIELDS
        .iter()
        .map(|field| format!("{field} MATCH {match_term}"))
        .collect::<Vec<_>>()
        .join(" OR ");

    format!("{base} AND ({match_clause})")
}

fn build_spans_query(
    project_id: &str,
    root_span_id: &str,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    format!(
        "select: id, span_id, root_span_id, _pagination_key, _xact_id, created, span_parents, span_attributes, error, scores, metrics | from: project_logs({}) spans | filter: root_span_id = {} | preview_length: 125 | sort: _pagination_key ASC | limit: {}{}",
        sql_quote(project_id),
        sql_quote(root_span_id),
        limit,
        cursor_clause,
    )
}

fn build_full_span_query(project_id: &str, span_row_id: &str) -> String {
    format!(
        "select: * | from: project_logs({}) spans | filter: id = {} | preview_length: -1 | limit: 1",
        sql_quote(project_id),
        sql_quote(span_row_id),
    )
}

fn build_thread_invoke_request(project_id: &str, root_span_id: &str) -> Value {
    json!({
        "global_function": THREAD_PREPROCESSOR_NAME,
        "function_type": "preprocessor",
        "input": {
            "trace_ref": {
                "object_type": "project_logs",
                "object_id": project_id,
                "root_span_id": root_span_id,
            }
        },
        "mode": "json",
    })
}

fn compact_json(value: &Value) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| value.to_string())
}

fn maybe_print_query(enabled: bool, label: &str, query: &str) {
    if enabled {
        eprintln!("bt traces [{label}] BTQL:\n{query}\n");
    }
}

fn maybe_print_invoke(enabled: bool, label: &str, request: &Value) {
    if enabled {
        let formatted =
            serde_json::to_string_pretty(request).unwrap_or_else(|_| compact_json(request));
        eprintln!("bt traces [{label}] INVOKE /function/invoke:\n{formatted}\n");
    }
}

fn root_error_message(err: &anyhow::Error) -> String {
    err.chain()
        .last()
        .map(|cause| cause.to_string())
        .unwrap_or_else(|| err.to_string())
}

fn query_from_error(err: &anyhow::Error) -> Option<String> {
    for cause in err.chain() {
        let msg = cause.to_string();
        if let Some(query) = msg.strip_prefix("BTQL query failed: ") {
            return Some(format!("BTQL: {query}"));
        }
    }
    None
}

fn invoke_from_error(err: &anyhow::Error) -> Option<String> {
    for cause in err.chain() {
        let msg = cause.to_string();
        if let Some(request) = msg.strip_prefix("Invoke request failed: ") {
            return Some(format!("Invoke: {request}"));
        }
    }
    None
}

fn btql_quote(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| format!("\"{}\"", value.replace('\\', "\\\\").replace('\"', "\\\"")))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

async fn fetch_summary_rows(
    client: &ApiClient,
    project_id: &str,
    preview_length: usize,
    total_limit: usize,
    search_query: Option<&str>,
    print_queries: bool,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut cursor: Option<String> = None;

    while rows.len() < total_limit {
        let page_limit = (total_limit - rows.len()).min(MAX_BTQL_PAGE_LIMIT);
        let query = build_summary_query(
            project_id,
            preview_length,
            page_limit,
            cursor.as_deref(),
            search_query,
        );
        maybe_print_query(print_queries, "summary", &query);

        let response = execute_query(client, &query)
            .await
            .with_context(|| format!("BTQL query failed: {query}"))?;

        if response.data.is_empty() {
            break;
        }

        rows.extend(response.data);

        let next_cursor = response.cursor.filter(|c| !c.is_empty());
        if next_cursor.is_none() {
            break;
        }
        cursor = next_cursor;
    }

    rows.truncate(total_limit);
    Ok(rows)
}

async fn fetch_trace_span_rows_for_tree(
    client: &ApiClient,
    project_id: &str,
    root_span_id: &str,
    total_limit: usize,
    print_queries: bool,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut cursor: Option<String> = None;

    while rows.len() < total_limit {
        let page_limit = (total_limit - rows.len()).min(MAX_BTQL_PAGE_LIMIT);
        let query = build_spans_query(project_id, root_span_id, page_limit, cursor.as_deref());
        maybe_print_query(print_queries, "spans-tree", &query);

        let response = execute_query(client, &query)
            .await
            .with_context(|| format!("BTQL query failed: {query}"))?;

        if response.data.is_empty() {
            break;
        }

        rows.extend(response.data);

        let next_cursor = response.cursor.filter(|c| !c.is_empty());
        if next_cursor.is_none() {
            break;
        }
        cursor = next_cursor;
    }

    rows.truncate(total_limit);
    Ok(rows)
}

async fn fetch_full_span_row(
    client: &ApiClient,
    project_id: &str,
    span_row_id: &str,
    print_queries: bool,
) -> Result<Option<Map<String, Value>>> {
    let query = build_full_span_query(project_id, span_row_id);
    maybe_print_query(print_queries, "span-full", &query);

    let response = execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))?;

    Ok(response.data.into_iter().next())
}

fn extract_thread_messages(value: Value) -> Vec<Value> {
    match value {
        Value::Array(messages) => messages,
        Value::Object(obj) => {
            if let Some(Value::Array(messages)) = obj.get("output") {
                return messages.clone();
            }
            if let Some(Value::Array(messages)) = obj.get("messages") {
                return messages.clone();
            }
            if let Some(Value::Array(messages)) = obj.get("thread") {
                return messages.clone();
            }
            Vec::new()
        }
        _ => Vec::new(),
    }
}

async fn fetch_thread_messages(
    client: &ApiClient,
    project_id: &str,
    root_span_id: &str,
    print_queries: bool,
) -> Result<Vec<Value>> {
    let request = build_thread_invoke_request(project_id, root_span_id);
    maybe_print_invoke(print_queries, "thread-preprocessor", &request);

    let response = execute_invoke(client, &request)
        .await
        .with_context(|| format!("Invoke request failed: {}", compact_json(&request)))?;

    Ok(extract_thread_messages(response))
}

fn span_display_id_for_row_id(app: &TraceViewerApp, row_id: &str) -> String {
    app.spans
        .iter()
        .find(|span| span.id == row_id)
        .map(|span| span.span_id.clone())
        .unwrap_or_else(|| row_id.to_string())
}

fn start_full_span_load_for_row_id(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    row_id: String,
) {
    let (tx, rx) = mpsc::channel();
    let client = client.clone();
    let project_id = app.project.id.clone();
    let print_queries = app.print_queries;
    let row_id_for_task = row_id.clone();

    handle.spawn(async move {
        let result =
            fetch_full_span_row(&client, &project_id, &row_id_for_task, print_queries).await;
        let _ = tx.send((row_id_for_task, result));
    });

    app.full_span_loading_row_id = Some(row_id.clone());
    app.full_span_load_started_at = Some(Instant::now());
    app.full_span_spinner_tick = 0;
    app.full_span_load_rx = Some(rx);

    if app.detail_view == DetailView::Span {
        let span_label = span_display_id_for_row_id(app, &row_id);
        app.set_status(format!("Loading span {span_label} in background..."));
    }
}

fn request_selected_span_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    force_refresh: bool,
) -> Result<()> {
    let Some(span) = app.current_span().cloned() else {
        return Ok(());
    };

    if span.id.is_empty() {
        return Ok(());
    }

    if !force_refresh && app.full_span_cache.contains_key(&span.id) {
        return Ok(());
    }

    if app
        .full_span_loading_row_id
        .as_deref()
        .map(|id| id == span.id)
        .unwrap_or(false)
    {
        return Ok(());
    }

    if app.full_span_loading_row_id.is_some() {
        app.full_span_pending_row_id = Some(span.id.clone());
        return Ok(());
    }

    start_full_span_load_for_row_id(app, client, handle, span.id.clone());
    Ok(())
}

fn poll_pending_full_span_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if app.full_span_loading_row_id.is_none() {
        if let Some(next_row_id) = app.full_span_pending_row_id.take() {
            if !next_row_id.is_empty() && !app.full_span_cache.contains_key(&next_row_id) {
                start_full_span_load_for_row_id(app, client, handle, next_row_id);
            }
        }
        return Ok(());
    }

    app.full_span_spinner_tick = app.full_span_spinner_tick.wrapping_add(1);

    let recv_result = app.full_span_load_rx.as_ref().map(|rx| rx.try_recv());
    let Some(recv_result) = recv_result else {
        return Ok(());
    };

    match recv_result {
        Ok((row_id, result)) => {
            app.full_span_load_rx = None;
            app.full_span_loading_row_id = None;
            app.full_span_load_started_at = None;

            match result {
                Ok(Some(row)) => {
                    app.full_span_cache.insert(row_id.clone(), row);
                    let current_selected_row_id = app
                        .current_span()
                        .map(|span| span.id.as_str())
                        .unwrap_or("");
                    if app.detail_view == DetailView::Span && current_selected_row_id == row_id {
                        app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
                    }
                }
                Ok(None) => {
                    if app.detail_view == DetailView::Span {
                        let span_label = span_display_id_for_row_id(app, &row_id);
                        app.set_status(format!("No full row found for span {span_label}"));
                    }
                }
                Err(err) => {
                    let detail = root_error_message(&err);
                    let span_label = span_display_id_for_row_id(app, &row_id);
                    if let Some(query) = query_from_error(&err) {
                        app.set_error_with_query(
                            format!("Failed to load full span {span_label}: {detail}"),
                            query,
                        );
                    } else {
                        app.set_error(format!("Failed to load full span {span_label}: {detail}"));
                    }
                }
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            app.full_span_load_rx = None;
            app.full_span_loading_row_id = None;
            app.full_span_load_started_at = None;
            app.set_error("Failed to load full span: request channel closed");
        }
    }

    if app.full_span_loading_row_id.is_none() {
        if let Some(next_row_id) = app.full_span_pending_row_id.take() {
            if !next_row_id.is_empty() && !app.full_span_cache.contains_key(&next_row_id) {
                start_full_span_load_for_row_id(app, client, handle, next_row_id);
            }
        }
    }

    Ok(())
}

fn request_thread_messages_load(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    force_refresh: bool,
) -> Result<()> {
    let Some(root_span_id) = app.loaded_root_span_id.clone() else {
        app.set_error("No trace loaded");
        return Ok(());
    };

    if !force_refresh && (app.thread_messages.is_some() || app.thread_loading) {
        app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
        return Ok(());
    }

    if app.thread_loading {
        return Ok(());
    }

    app.thread_messages = None;
    app.thread_selected_message = 0;
    app.thread_expanded.clear();
    app.thread_loading = true;
    app.thread_spinner_tick = 0;
    app.set_status(format!("Loading thread for trace {root_span_id}..."));

    let (tx, rx) = mpsc::channel();
    let client = client.clone();
    let project_id = app.project.id.clone();
    let print_queries = app.print_queries;
    handle.spawn(async move {
        let result =
            fetch_thread_messages(&client, &project_id, &root_span_id, print_queries).await;
        let _ = tx.send(result);
    });
    app.thread_load_rx = Some(rx);

    Ok(())
}

fn poll_pending_thread_load(app: &mut TraceViewerApp) {
    if !app.thread_loading {
        return;
    }

    app.thread_spinner_tick = app.thread_spinner_tick.wrapping_add(1);

    let recv_result = app.thread_load_rx.as_ref().map(|rx| rx.try_recv());
    let Some(recv_result) = recv_result else {
        return;
    };

    match recv_result {
        Ok(Ok(messages)) => {
            app.thread_loading = false;
            app.thread_load_rx = None;
            app.thread_messages = Some(messages);
            if let Some(messages) = &app.thread_messages {
                if messages.is_empty() {
                    app.thread_selected_message = 0;
                    app.thread_expanded.clear();
                } else {
                    app.thread_selected_message = app
                        .thread_selected_message
                        .min(messages.len().saturating_sub(1));
                    app.thread_expanded.retain(|idx| *idx < messages.len());
                }
            }
            if app.detail_view == DetailView::Thread {
                app.set_status(span_detail_status(app.detail_view, app.detail_pane_focus));
            }
        }
        Ok(Err(err)) => {
            app.thread_loading = false;
            app.thread_load_rx = None;
            app.thread_messages = None;
            let root_span_id = app
                .loaded_root_span_id
                .as_deref()
                .unwrap_or("<unknown-trace>");
            let detail = root_error_message(&err);
            if let Some(request) = invoke_from_error(&err) {
                app.set_error_with_query(
                    format!("Failed to load thread for trace {root_span_id}: {detail}"),
                    request,
                );
            } else {
                app.set_error(format!(
                    "Failed to load thread for trace {root_span_id}: {detail}"
                ));
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            app.thread_loading = false;
            app.thread_load_rx = None;
            app.thread_messages = None;
            app.set_error("Failed to load thread: request channel closed");
        }
    }
}

async fn execute_query(client: &ApiClient, query: &str) -> Result<BtqlResponse> {
    let body = json!({
        "query": query,
        "fmt": "json",
    });

    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        Vec::new()
    };

    client.post_with_headers("/btql", &body, &headers).await
}

async fn execute_invoke(client: &ApiClient, request: &Value) -> Result<Value> {
    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        Vec::new()
    };

    client
        .post_with_headers("/function/invoke", request, &headers)
        .await
}
