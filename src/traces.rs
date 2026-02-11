use std::collections::{HashMap, HashSet};
use std::io;
use std::io::IsTerminal;
use std::time::Duration;

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
    Block, Borders, Cell, List, ListItem, ListState, Paragraph, Row, Table, TableState, Wrap,
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

    /// Print each SQL query before execution
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

struct TraceViewerApp {
    project: ProjectSelection,
    traces: Vec<TraceSummaryRow>,
    selected_trace: usize,
    spans: Vec<SpanListEntry>,
    full_span_cache: HashMap<String, Map<String, Value>>,
    selected_span: usize,
    detail_scroll: u16,
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
            "Loaded {trace_count} traces. Up/Down: move  Enter: open trace  r: refresh  q: quit"
        );
        Self {
            project,
            traces,
            selected_trace: 0,
            spans: Vec::new(),
            full_span_cache: HashMap::new(),
            selected_span: 0,
            detail_scroll: 0,
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
        KeyCode::Char('q') => return Ok(true),
        KeyCode::Esc => {
            if app.screen == Screen::SpanDetail {
                app.screen = Screen::Traces;
                app.set_status(
                    "Back to traces. Up/Down: move  Enter: open trace  r: refresh  q: quit",
                );
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
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => app.move_trace_up(),
        KeyCode::Down | KeyCode::Char('j') => app.move_trace_down(),
        KeyCode::Enter | KeyCode::Right => load_selected_trace(app, client, handle)?,
        KeyCode::Char('r') => refresh_traces(app, client, handle)?,
        _ => {}
    }
    Ok(())
}

fn handle_span_detail_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    match key.code {
        KeyCode::Up | KeyCode::Char('k') => {
            app.move_span_up();
            ensure_selected_span_loaded(app, client, handle)?;
        }
        KeyCode::Down | KeyCode::Char('j') => {
            app.move_span_down();
            ensure_selected_span_loaded(app, client, handle)?;
        }
        KeyCode::PageUp => app.scroll_detail_up(10),
        KeyCode::PageDown => app.scroll_detail_down(10),
        KeyCode::Left | KeyCode::Backspace => {
            app.screen = Screen::Traces;
            app.set_status("Back to traces. Up/Down: move  Enter: open trace  r: refresh  q: quit");
        }
        KeyCode::Enter => {
            ensure_selected_span_loaded(app, client, handle)?;
        }
        KeyCode::Char('r') => {
            if app.loaded_root_span_id.is_some() {
                load_selected_trace(app, client, handle)?;
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_detail_up(10)
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.scroll_detail_down(10)
        }
        _ => {}
    }
    Ok(())
}

fn refresh_traces(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let previous_root = app.current_trace().map(|r| r.root_span_id.clone());
    app.set_status("Refreshing trace table...");

    let response = handle.block_on(fetch_summary_rows(
        client,
        &app.project.id,
        app.preview_length,
        app.limit,
        app.print_queries,
    ));

    match response {
        Ok(rows) => {
            app.traces = parse_summary_rows(rows);
            if app.traces.is_empty() {
                app.selected_trace = 0;
                app.set_status("No traces found for the selected project");
                return Ok(());
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

            app.set_status(format!(
                "Loaded {} traces. Up/Down: move  Enter: open trace  r: refresh  q: quit",
                app.traces.len()
            ));
        }
        Err(err) => {
            let detail = root_error_message(&err);
            if let Some(query) = query_from_error(&err) {
                app.set_error_with_query(format!("Failed to refresh traces: {detail}"), query);
            } else {
                app.set_error(format!("Failed to refresh traces: {detail}"));
            }
        }
    }

    Ok(())
}

fn load_selected_trace(
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

    app.set_status(format!("Loading trace {root_span_id}..."));

    let result = handle.block_on(fetch_trace_span_rows_for_tree(
        client,
        &app.project.id,
        &root_span_id,
        MAX_TRACE_SPANS,
        app.print_queries,
    ));

    match result {
        Ok(rows) => {
            let spans = build_span_entries(rows);
            if spans.is_empty() {
                app.set_status(format!("No spans found for trace {root_span_id}"));
                return Ok(());
            }

            app.spans = spans;
            app.full_span_cache.clear();
            app.selected_span = 0;
            app.detail_scroll = 0;
            app.loaded_root_span_id = Some(root_span_id.clone());
            app.screen = Screen::SpanDetail;
            app.set_status(
                "Span view. Up/Down: span  PgUp/PgDn: scroll  Esc: back  r: reload  q: quit",
            );
            ensure_selected_span_loaded(app, client, handle)?;
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

    Ok(())
}

fn draw_ui(frame: &mut Frame<'_>, app: &TraceViewerApp) {
    let chunks = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(5), Constraint::Length(3)])
        .split(frame.area());

    match app.screen {
        Screen::Traces => draw_traces_table(frame, chunks[0], app),
        Screen::SpanDetail => draw_span_detail_view(frame, chunks[0], app),
    }

    let status_style = if app.status_is_error {
        Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)
    } else {
        Style::default()
    };

    let mut lines = vec![Line::from(vec![
        Span::styled("Project: ", Style::default().fg(Color::DarkGray)),
        Span::raw(project_label(&app.project)),
        Span::raw("  |  "),
        Span::styled(app.status.as_str(), status_style),
    ])];
    if !app.status_detail.is_empty() {
        lines.push(Line::from(Span::styled(
            app.status_detail.as_str(),
            status_style,
        )));
    }

    let status = Paragraph::new(lines).block(Block::default().borders(Borders::TOP));

    frame.render_widget(status, chunks[1]);
}

fn draw_traces_table(frame: &mut Frame<'_>, area: ratatui::layout::Rect, app: &TraceViewerApp) {
    let header = Row::new(vec!["Created", "Trace", "Input", "Duration"]).style(
        Style::default()
            .fg(Color::Gray)
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
            .title("Traces")
            .borders(Borders::ALL)
            .title_bottom("Enter opens selected trace"),
    )
    .row_highlight_style(
        Style::default()
            .bg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );

    let mut state = TableState::default();
    if !app.traces.is_empty() {
        state.select(Some(app.selected_trace));
    }

    frame.render_stateful_widget(table, area, &mut state);
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

    let list = List::new(items)
        .block(
            Block::default()
                .title("Span Tree")
                .borders(Borders::ALL)
                .title_bottom("Esc returns to trace table"),
        )
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        );

    let mut list_state = ListState::default();
    if !app.spans.is_empty() {
        list_state.select(Some(app.selected_span));
    }
    frame.render_stateful_widget(list, chunks[0], &mut list_state);

    let detail_text = app
        .current_span()
        .map(|span| {
            let full_row = app.full_span_cache.get(&span.id);
            render_span_details(span, full_row.unwrap_or(&span.row), full_row.is_some())
        })
        .unwrap_or_else(|| "No span selected".to_string());

    let detail = Paragraph::new(detail_text)
        .block(Block::default().title("Span Detail").borders(Borders::ALL))
        .scroll((app.detail_scroll, 0))
        .wrap(Wrap { trim: false });

    frame.render_widget(detail, chunks[1]);
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
        sort_key: String,
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
        let sort_key = value_as_string(row.get("_pagination_key"))
            .or_else(|| value_as_string(row.get("created")))
            .unwrap_or_default();

        let label = span_label(&row, &span_id);

        temp_spans.insert(
            span_id.clone(),
            TempSpan {
                id,
                span_id,
                root_span_id,
                parent_span_id,
                label,
                sort_key,
                row,
            },
        );
    }

    if temp_spans.is_empty() {
        return Vec::new();
    }

    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots: Vec<String> = Vec::new();

    for span in temp_spans.values() {
        if let Some(parent) = &span.parent_span_id {
            if temp_spans.contains_key(parent) {
                children
                    .entry(parent.clone())
                    .or_default()
                    .push(span.span_id.clone());
                continue;
            }
        }

        roots.push(span.span_id.clone());
    }

    for child_ids in children.values_mut() {
        child_ids.sort_by(|a, b| {
            let a_key = temp_spans.get(a).map(|s| s.sort_key.as_str()).unwrap_or("");
            let b_key = temp_spans.get(b).map(|s| s.sort_key.as_str()).unwrap_or("");
            a_key.cmp(b_key)
        });
    }

    roots.sort_by(|a, b| {
        let a_key = temp_spans.get(a).map(|s| s.sort_key.as_str()).unwrap_or("");
        let b_key = temp_spans.get(b).map(|s| s.sort_key.as_str()).unwrap_or("");
        a_key.cmp(b_key)
    });

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

    for root in &roots {
        visit(root, 0, &temp_spans, &children, &mut ordered, &mut visited);
    }

    // Handle disconnected spans/cycles by appending anything not visited.
    let mut leftovers: Vec<String> = temp_spans
        .keys()
        .filter(|span_id| !visited.contains(*span_id))
        .cloned()
        .collect();

    leftovers.sort_by(|a, b| {
        let a_key = temp_spans.get(a).map(|s| s.sort_key.as_str()).unwrap_or("");
        let b_key = temp_spans.get(b).map(|s| s.sort_key.as_str()).unwrap_or("");
        a_key.cmp(b_key)
    });

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

fn render_span_details(span: &SpanListEntry, row: &Map<String, Value>, is_full: bool) -> String {
    let mut out = String::new();

    if !is_full {
        out.push_str("FULL CONTENT: not loaded (loading on selection; press Enter to retry)\n\n");
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
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    format!(
        "select: * | from: project_logs({}) summary | filter: created >= NOW() - INTERVAL 3 DAY | preview_length: {} | sort: _pagination_key DESC | limit: {}{}",
        sql_quote(project_id),
        preview_length,
        limit,
        cursor_clause,
    )
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
        "select: id, span_id, root_span_id, _pagination_key, created, span_parents, span_attributes, error, scores, metrics | from: project_logs({}) spans | filter: root_span_id = {} | preview_length: 125 | sort: _pagination_key ASC | limit: {}{}",
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

fn maybe_print_query(enabled: bool, label: &str, query: &str) {
    if enabled {
        eprintln!("bt traces [{label}] BTQL:\n{query}\n");
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
    print_queries: bool,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut cursor: Option<String> = None;

    while rows.len() < total_limit {
        let page_limit = (total_limit - rows.len()).min(MAX_BTQL_PAGE_LIMIT);
        let query = build_summary_query(project_id, preview_length, page_limit, cursor.as_deref());
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

fn ensure_selected_span_loaded(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let Some(span) = app.current_span().cloned() else {
        return Ok(());
    };

    if span.id.is_empty() || app.full_span_cache.contains_key(&span.id) {
        return Ok(());
    }

    app.set_status(format!("Loading full span {}...", span.span_id));
    let result = handle.block_on(fetch_full_span_row(
        client,
        &app.project.id,
        &span.id,
        app.print_queries,
    ));

    match result {
        Ok(Some(row)) => {
            app.full_span_cache.insert(span.id.clone(), row);
            app.set_status(
                "Span view. Up/Down: span  PgUp/PgDn: scroll  Esc: back  r: reload  q: quit",
            );
        }
        Ok(None) => {
            app.set_status(format!("No full row found for span {}", span.span_id));
        }
        Err(err) => {
            let detail = root_error_message(&err);
            if let Some(query) = query_from_error(&err) {
                app.set_error_with_query(
                    format!("Failed to load full span {}: {detail}", span.span_id),
                    query,
                );
            } else {
                app.set_error(format!(
                    "Failed to load full span {}: {detail}",
                    span.span_id
                ));
            }
        }
    }

    Ok(())
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
