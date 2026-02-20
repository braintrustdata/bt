use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::io;
use std::io::IsTerminal;
use std::sync::mpsc::{self, Receiver, TryRecvError};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use crossterm::event::{self, Event, KeyCode, KeyEvent, KeyModifiers};
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use crossterm::ExecutableCommand;
use lingua::processing::import::{import_messages_from_spans, Span as LinguaSpan};
use lingua::universal::{
    AssistantContent, AssistantContentPart, Message as LinguaMessage, ToolCallArguments,
    ToolContentPart, UserContent, UserContentPart,
};
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
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use unicode_width::UnicodeWidthStr;
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::auth::login;
use crate::http::ApiClient;
use crate::ui::{fuzzy_select, with_spinner};

const MAX_TRACE_SPANS: usize = 5000;
const MAX_BTQL_PAGE_LIMIT: usize = 1000;
const THREAD_PREPROCESSOR_NAME: &str = "project_default";
const LOADER_DELAY: Duration = Duration::from_millis(250);
const DOUBLE_G_TIMEOUT: Duration = Duration::from_millis(700);
const MAX_SAFE_PARAGRAPH_SCROLL: u16 = 60_000;
const MAX_RENDER_LINE_CHARS: usize = 220;
const MAX_PREVIEW_RENDER_LINES: usize = 24;
const MAX_EXPANDED_RENDER_LINES: usize = 120;
const MAX_EXPANDED_TOOL_RENDER_LINES: usize = 60;
const MAX_INLINE_EXPAND_CHARS: usize = 20_000;
const INLINE_EXPAND_ENABLED: bool = false;
const TRACE_TEXT_SEARCH_FIELDS: [&str; 6] = [
    "input",
    "expected",
    "metadata",
    "tags",
    "output",
    "span_attributes",
];

type SummaryLoadResult = (String, Option<String>, Result<Vec<Map<String, Value>>>);
type TraceLoadResult = (String, Result<Vec<Map<String, Value>>>);
type FullSpanLoadResult = (String, Result<Option<Map<String, Value>>>);

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    #[command(subcommand)]
    command: Option<ViewCommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum ViewCommand {
    /// List logs (summary or spans mode)
    Logs(LogsArgs),
    /// Fetch spans for a single trace (root span id)
    Trace(TraceArgs),
    /// Fetch a single span by row id (or from a URL)
    Span(SpanArgs),
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum ListMode {
    Summary,
    Spans,
}

#[derive(Debug, Clone, Args)]
struct LogsArgs {
    /// Project ID to query (overrides -p/--project)
    #[arg(long)]
    project_id: Option<String>,

    /// Object reference, format: object_type:object_selector (project_logs|experiment|dataset)
    #[arg(long)]
    object_ref: Option<String>,

    /// Braintrust app URL to open directly (parses org/project/r/s/tvt)
    #[arg(long)]
    url: Option<String>,

    /// Braintrust app URL to open directly (same as --url)
    #[arg(value_name = "URL")]
    url_arg: Option<String>,

    /// Number of rows to fetch
    #[arg(long, default_value_t = 50)]
    limit: usize,

    /// Cursor returned from a previous list call
    #[arg(long)]
    cursor: Option<String>,

    /// Preview length for list rows
    #[arg(long, default_value_t = 125)]
    preview_length: usize,

    /// List mode
    #[arg(long, value_enum, default_value = "summary")]
    list_mode: ListMode,

    /// Free-text search term
    #[arg(long)]
    search: Option<String>,

    /// Additional BTQL filter expression
    #[arg(long)]
    filter: Option<String>,

    /// Relative time window (e.g. 1h, 30m, 1d)
    #[arg(long, default_value = "1h")]
    window: String,

    /// Absolute lower bound timestamp (overrides --window)
    #[arg(long)]
    since: Option<String>,

    /// Force non-interactive mode
    #[arg(long)]
    non_interactive: bool,

    /// Print each BTQL query and invoke payload before execution
    #[arg(long)]
    print_queries: bool,
}

impl Default for LogsArgs {
    fn default() -> Self {
        Self {
            project_id: None,
            object_ref: None,
            url: None,
            url_arg: None,
            limit: 50,
            cursor: None,
            preview_length: 125,
            list_mode: ListMode::Summary,
            search: None,
            filter: None,
            window: "1h".to_string(),
            since: None,
            non_interactive: false,
            print_queries: false,
        }
    }
}

#[derive(Debug, Clone, Args)]
struct TraceArgs {
    /// Object reference, format: object_type:object_selector (project_logs|experiment|dataset)
    #[arg(long)]
    object_ref: Option<String>,

    /// Project ID to query (overrides -p/--project for project_logs)
    #[arg(long)]
    project_id: Option<String>,

    /// Root span id for the trace
    #[arg(long = "trace-id", alias = "root-span-id")]
    trace_id: Option<String>,

    /// Braintrust app URL to resolve to a trace
    #[arg(long)]
    url: Option<String>,

    /// Braintrust app URL (same as --url)
    #[arg(value_name = "URL")]
    url_arg: Option<String>,

    /// Number of spans to fetch
    #[arg(long, default_value_t = 100)]
    limit: usize,

    /// Cursor returned from a previous trace fetch
    #[arg(long)]
    cursor: Option<String>,

    /// Preview length for span rows
    #[arg(long, default_value_t = 125)]
    preview_length: usize,

    /// Print each BTQL query and invoke payload before execution
    #[arg(long)]
    print_queries: bool,

    /// Force non-interactive mode
    #[arg(long)]
    non_interactive: bool,
}

#[derive(Debug, Clone, Args)]
struct SpanArgs {
    /// Object reference, format: object_type:object_selector (project_logs|experiment|dataset)
    #[arg(long)]
    object_ref: Option<String>,

    /// Project ID to query (overrides -p/--project for project_logs)
    #[arg(long)]
    project_id: Option<String>,

    /// Span row id
    #[arg(long)]
    id: Option<String>,

    /// Braintrust app URL to resolve to a span
    #[arg(long)]
    url: Option<String>,

    /// Braintrust app URL (same as --url)
    #[arg(value_name = "URL")]
    url_arg: Option<String>,

    /// Print each BTQL query before execution
    #[arg(long)]
    print_queries: bool,

    /// Force non-interactive mode
    #[arg(long)]
    non_interactive: bool,
}

#[derive(Debug, Clone, Serialize)]
struct ObjectRef {
    object_type: String,
    object_name: String,
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

#[derive(Debug, Clone)]
struct ParsedTraceUrl {
    org: Option<String>,
    project: Option<String>,
    page: Option<String>,
    row_ref: Option<String>,
    span_id: Option<String>,
    trace_view_type: Option<String>,
}

#[derive(Debug, Clone)]
struct ResolvedTraceTarget {
    project: ProjectSelection,
    root_span_id: String,
    span_id: Option<String>,
    detail_view: DetailView,
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
    OpeningTrace,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SpanDetailTab {
    Messages,
    Raw,
}

struct TraceViewerApp {
    project: ProjectSelection,
    source_expr: String,
    list_mode: ListMode,
    base_filter: String,
    traces: Vec<TraceSummaryRow>,
    selected_trace: usize,
    trace_search_query: String,
    trace_search_input: String,
    trace_search_mode: bool,
    pending_g_prefix_at: Option<Instant>,
    url_open_mode: bool,
    url_open_input: String,
    url_open_loading_input: String,
    url_open_started_at: Option<Instant>,
    url_open_spinner_tick: usize,
    url_open_rx: Option<Receiver<(String, Result<ResolvedTraceTarget>)>>,
    summary_loading_search_query: String,
    summary_pending_refresh: Option<(String, Option<String>)>,
    summary_load_started_at: Option<Instant>,
    summary_spinner_tick: usize,
    summary_load_rx: Option<Receiver<SummaryLoadResult>>,
    trace_loading_root_span_id: Option<String>,
    trace_pending_root_span_id: Option<String>,
    trace_load_started_at: Option<Instant>,
    trace_spinner_tick: usize,
    trace_load_rx: Option<Receiver<TraceLoadResult>>,
    spans: Vec<SpanListEntry>,
    collapsed_span_row_ids: HashSet<String>,
    full_span_cache: HashMap<String, Map<String, Value>>,
    full_span_loading_row_id: Option<String>,
    full_span_pending_row_id: Option<String>,
    full_span_load_started_at: Option<Instant>,
    full_span_spinner_tick: usize,
    full_span_load_rx: Option<Receiver<FullSpanLoadResult>>,
    selected_span: usize,
    detail_scroll: u16,
    detail_view: DetailView,
    detail_pane_focus: DetailPaneFocus,
    span_detail_tab: SpanDetailTab,
    span_detail_messages: Option<Vec<Value>>,
    span_detail_message_selected: usize,
    span_detail_message_expanded: HashSet<usize>,
    message_viewer_open: bool,
    message_viewer_scroll: u16,
    thread_messages: Option<Vec<Value>>,
    thread_selected_message: usize,
    thread_expanded: HashSet<usize>,
    thread_loading: bool,
    thread_spinner_tick: usize,
    thread_load_rx: Option<Receiver<Result<Vec<Value>>>>,
    loaded_root_span_id: Option<String>,
    pending_open_span_id: Option<String>,
    pending_open_detail_view: Option<DetailView>,
    screen: Screen,
    status: String,
    status_detail: String,
    status_is_error: bool,
    limit: usize,
    preview_length: usize,
    print_queries: bool,
}

struct TraceViewerInit {
    project: ProjectSelection,
    source_expr: String,
    list_mode: ListMode,
    base_filter: String,
    traces: Vec<TraceSummaryRow>,
    limit: usize,
    preview_length: usize,
    print_queries: bool,
}

struct InteractiveRunArgs {
    init: TraceViewerInit,
    initial_search_query: String,
    startup_trace_url: Option<ParsedTraceUrl>,
}

#[derive(Debug, Clone)]
struct LogsQueryContext {
    source_expr: String,
    list_mode: ListMode,
    preview_length: usize,
    base_filter: String,
    print_queries: bool,
}

impl TraceViewerApp {
    fn new(init: TraceViewerInit) -> Self {
        let TraceViewerInit {
            project,
            source_expr,
            list_mode,
            base_filter,
            traces,
            limit,
            preview_length,
            print_queries,
        } = init;
        let trace_count = traces.len();
        let noun = if list_mode == ListMode::Spans {
            "spans"
        } else {
            "traces"
        };
        let status = format!(
            "Loaded {trace_count} {noun}. Up/Down: move  Enter: open trace  /: search  Ctrl+k: open URL  r: refresh  q: quit"
        );
        Self {
            project,
            source_expr,
            list_mode,
            base_filter,
            traces,
            selected_trace: 0,
            trace_search_query: String::new(),
            trace_search_input: String::new(),
            trace_search_mode: false,
            pending_g_prefix_at: None,
            url_open_mode: false,
            url_open_input: String::new(),
            url_open_loading_input: String::new(),
            url_open_started_at: None,
            url_open_spinner_tick: 0,
            url_open_rx: None,
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
            collapsed_span_row_ids: HashSet::new(),
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
            span_detail_tab: SpanDetailTab::Messages,
            span_detail_messages: None,
            span_detail_message_selected: 0,
            span_detail_message_expanded: HashSet::new(),
            message_viewer_open: false,
            message_viewer_scroll: 0,
            thread_messages: None,
            thread_selected_message: 0,
            thread_expanded: HashSet::new(),
            thread_loading: false,
            thread_spinner_tick: 0,
            thread_load_rx: None,
            loaded_root_span_id: None,
            pending_open_span_id: None,
            pending_open_detail_view: None,
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

    fn visible_span_indices(&self) -> Vec<usize> {
        let mut visible = Vec::new();
        let mut collapsed_depth_stack: Vec<usize> = Vec::new();

        for (idx, span) in self.spans.iter().enumerate() {
            while collapsed_depth_stack
                .last()
                .copied()
                .map(|depth| span.depth <= depth)
                .unwrap_or(false)
            {
                collapsed_depth_stack.pop();
            }

            if collapsed_depth_stack.is_empty() {
                visible.push(idx);
            }

            if self.collapsed_span_row_ids.contains(&span.id) {
                collapsed_depth_stack.push(span.depth);
            }
        }

        visible
    }

    fn selected_visible_span_position(&self, visible_indices: &[usize]) -> Option<usize> {
        visible_indices
            .iter()
            .position(|idx| *idx == self.selected_span)
            .or_else(|| {
                visible_indices
                    .iter()
                    .rposition(|idx| *idx <= self.selected_span)
            })
            .or_else(|| (!visible_indices.is_empty()).then_some(0))
    }

    fn span_has_children_at_index(&self, index: usize) -> bool {
        let Some(current) = self.spans.get(index) else {
            return false;
        };
        self.spans
            .get(index + 1)
            .map(|next| next.depth > current.depth)
            .unwrap_or(false)
    }

    fn is_span_collapsed_at_index(&self, index: usize) -> bool {
        self.spans
            .get(index)
            .map(|span| self.collapsed_span_row_ids.contains(&span.id))
            .unwrap_or(false)
    }

    fn set_default_tree_collapse_state(&mut self) {
        self.collapsed_span_row_ids = self
            .spans
            .iter()
            .filter(|span| span_purpose_is_scorer(&span.row))
            .map(|span| span.id.clone())
            .collect();
    }

    fn expand_ancestor_chain_for_selected_span(&mut self) {
        let Some(selected) = self.spans.get(self.selected_span) else {
            return;
        };
        let mut target_depth = selected.depth;
        for idx in (0..self.selected_span).rev() {
            let Some(candidate) = self.spans.get(idx) else {
                continue;
            };
            if candidate.depth < target_depth {
                self.collapsed_span_row_ids.remove(&candidate.id);
                target_depth = candidate.depth;
                if target_depth == 0 {
                    break;
                }
            }
        }
    }

    fn ensure_selected_span_visible(&mut self) {
        let visible = self.visible_span_indices();
        if visible.is_empty() {
            self.selected_span = 0;
            return;
        }
        if visible.contains(&self.selected_span) {
            return;
        }
        if let Some(pos) = self.selected_visible_span_position(&visible) {
            self.selected_span = visible[pos];
        } else {
            self.selected_span = visible[0];
        }
        self.refresh_span_detail_messages(true);
    }

    fn toggle_selected_span_collapsed(&mut self) -> bool {
        if !self.span_has_children_at_index(self.selected_span) {
            return false;
        }

        let Some(span) = self.current_span().cloned() else {
            return false;
        };
        if self.collapsed_span_row_ids.contains(&span.id) {
            self.collapsed_span_row_ids.remove(&span.id);
        } else {
            self.collapsed_span_row_ids.insert(span.id);
        }
        self.ensure_selected_span_visible();
        true
    }

    fn move_trace_up(&mut self) {
        if self.selected_trace > 0 {
            self.selected_trace -= 1;
        }
    }

    fn move_trace_top(&mut self) {
        self.selected_trace = 0;
    }

    fn move_trace_down(&mut self) {
        if self.selected_trace + 1 < self.traces.len() {
            self.selected_trace += 1;
        }
    }

    fn move_trace_bottom(&mut self) {
        if !self.traces.is_empty() {
            self.selected_trace = self.traces.len().saturating_sub(1);
        }
    }

    fn move_span_up(&mut self) {
        let visible = self.visible_span_indices();
        let Some(pos) = self.selected_visible_span_position(&visible) else {
            return;
        };
        if pos > 0 {
            self.selected_span = visible[pos - 1];
            self.detail_scroll = 0;
            self.close_message_viewer();
            self.refresh_span_detail_messages(true);
        }
    }

    fn move_span_top(&mut self) {
        let visible = self.visible_span_indices();
        if let Some(first) = visible.first().copied() {
            self.selected_span = first;
            self.detail_scroll = 0;
            self.close_message_viewer();
            self.refresh_span_detail_messages(true);
        }
    }

    fn move_span_down(&mut self) {
        let visible = self.visible_span_indices();
        let Some(pos) = self.selected_visible_span_position(&visible) else {
            return;
        };
        if pos + 1 < visible.len() {
            self.selected_span = visible[pos + 1];
            self.detail_scroll = 0;
            self.close_message_viewer();
            self.refresh_span_detail_messages(true);
        }
    }

    fn move_span_bottom(&mut self) {
        let visible = self.visible_span_indices();
        if let Some(last) = visible.last().copied() {
            self.selected_span = last;
            self.detail_scroll = 0;
            self.close_message_viewer();
            self.refresh_span_detail_messages(true);
        }
    }

    fn scroll_detail_up(&mut self, amount: u16) {
        self.detail_scroll = self.detail_scroll.saturating_sub(amount);
    }

    fn scroll_detail_top(&mut self) {
        self.detail_scroll = 0;
    }

    fn move_thread_message_up(&mut self) {
        if self.thread_selected_message > 0 {
            self.thread_selected_message -= 1;
            self.close_message_viewer();
        }
    }

    fn move_thread_message_top(&mut self) {
        self.thread_selected_message = 0;
        self.close_message_viewer();
    }

    fn move_thread_message_down(&mut self) {
        let Some(messages) = &self.thread_messages else {
            return;
        };
        if self.thread_selected_message + 1 < messages.len() {
            self.thread_selected_message += 1;
            self.close_message_viewer();
        }
    }

    fn move_thread_message_bottom(&mut self) {
        let Some(messages) = &self.thread_messages else {
            return;
        };
        if !messages.is_empty() {
            self.thread_selected_message = messages.len().saturating_sub(1);
            self.close_message_viewer();
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

    fn move_span_detail_message_up(&mut self) {
        if self.span_detail_message_selected > 0 {
            self.span_detail_message_selected -= 1;
            self.close_message_viewer();
        }
    }

    fn move_span_detail_message_down(&mut self) {
        let Some(messages) = &self.span_detail_messages else {
            return;
        };
        if self.span_detail_message_selected + 1 < messages.len() {
            self.span_detail_message_selected += 1;
            self.close_message_viewer();
        }
    }

    fn move_span_detail_message_top(&mut self) {
        self.span_detail_message_selected = 0;
        self.close_message_viewer();
    }

    fn move_span_detail_message_bottom(&mut self) {
        let Some(messages) = &self.span_detail_messages else {
            return;
        };
        if !messages.is_empty() {
            self.span_detail_message_selected = messages.len().saturating_sub(1);
            self.close_message_viewer();
        }
    }

    fn span_detail_has_message_list(&self) -> bool {
        self.span_detail_messages
            .as_ref()
            .map(|messages| !messages.is_empty())
            .unwrap_or(false)
    }

    fn span_detail_shows_messages(&self) -> bool {
        self.detail_view == DetailView::Span
            && self.span_detail_tab == SpanDetailTab::Messages
            && self.span_detail_has_message_list()
    }

    fn span_detail_status_text(&self) -> &'static str {
        if self.message_viewer_open {
            return "Message view. Up/Down: scroll  PgUp/PgDn: page  gg/G: top/bottom  v: close  Esc: close";
        }
        span_detail_status(
            self.detail_view,
            self.detail_pane_focus,
            self.span_detail_shows_messages(),
        )
    }

    fn toggle_span_detail_tab(&mut self) {
        self.span_detail_tab = match self.span_detail_tab {
            SpanDetailTab::Messages => SpanDetailTab::Raw,
            SpanDetailTab::Raw => SpanDetailTab::Messages,
        };
        self.detail_scroll = 0;
        self.close_message_viewer();
    }

    fn open_message_viewer(&mut self) {
        self.message_viewer_open = true;
        self.message_viewer_scroll = 0;
    }

    fn close_message_viewer(&mut self) {
        self.message_viewer_open = false;
        self.message_viewer_scroll = 0;
    }

    fn refresh_span_detail_messages(&mut self, reset_selection: bool) {
        let Some(span) = self.current_span().cloned() else {
            self.span_detail_messages = None;
            self.span_detail_message_selected = 0;
            self.span_detail_message_expanded.clear();
            return;
        };

        let row = self.full_span_cache.get(&span.id).unwrap_or(&span.row);
        let messages = extract_span_detail_messages(row);
        if messages.is_empty() {
            self.span_detail_messages = None;
            self.span_detail_message_selected = 0;
            self.span_detail_message_expanded.clear();
            return;
        }

        let message_count = messages.len();
        self.span_detail_messages = Some(messages);
        if reset_selection {
            self.span_detail_message_selected = 0;
            self.span_detail_message_expanded.clear();
        } else {
            self.span_detail_message_selected = self
                .span_detail_message_selected
                .min(message_count.saturating_sub(1));
            self.span_detail_message_expanded
                .retain(|idx| *idx < message_count);
        }
    }
}

pub async fn run(base: BaseArgs, args: ViewArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;

    match args
        .command
        .unwrap_or(ViewCommand::Logs(LogsArgs::default()))
    {
        ViewCommand::Logs(logs) => run_logs_command(base, client, logs).await,
        ViewCommand::Trace(trace) => run_trace_command(base, client, trace).await,
        ViewCommand::Span(span) => run_span_command(base, client, span).await,
    }
}

async fn run_logs_command(base: BaseArgs, client: ApiClient, args: LogsArgs) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit must be greater than 0");
    }
    if args.preview_length == 0 {
        bail!("--preview-length must be greater than 0");
    }
    if args.object_ref.is_some() && args.project_id.is_some() {
        bail!("--object-ref and --project-id are mutually exclusive");
    }

    let startup_url = select_startup_url(args.url.as_deref(), args.url_arg.as_deref())?;
    let parsed_startup_url = startup_url.as_deref().map(parse_trace_url).transpose()?;
    let project_from_url = parsed_startup_url
        .as_ref()
        .and_then(|u| u.project.as_deref());

    let (object_ref, project_for_ui) = resolve_object_ref_for_view(
        &client,
        &base,
        args.object_ref.as_deref(),
        args.project_id.as_deref(),
        project_from_url,
    )
    .await?;
    let source_expr = btql_source_expr(&object_ref)?;
    let base_filter = build_base_filter_clause(
        args.since.as_deref(),
        args.window.as_str(),
        args.filter.as_deref(),
    )?;
    let logs_query_ctx = LogsQueryContext {
        source_expr: source_expr.clone(),
        list_mode: args.list_mode,
        preview_length: args.preview_length,
        base_filter: base_filter.clone(),
        print_queries: args.print_queries,
    };
    let search_query = args.search.unwrap_or_default();

    let interactive = !base.json && std::io::stdin().is_terminal() && !args.non_interactive;

    if interactive {
        if args.cursor.is_some() {
            bail!("--cursor is only available in non-interactive mode");
        }
        if object_ref.object_type != "project_logs" {
            bail!("interactive logs view currently supports only project_logs sources");
        }

        let project = project_for_ui.unwrap_or(ProjectSelection {
            id: object_ref.object_name.clone(),
            name: None,
        });
        let skip_initial_rows = parsed_startup_url.is_some();
        let initial_rows = if skip_initial_rows {
            Vec::new()
        } else {
            with_spinner(
                "Loading logs...",
                fetch_summary_rows(
                    &client,
                    &logs_query_ctx,
                    args.limit,
                    if search_query.trim().is_empty() {
                        None
                    } else {
                        Some(search_query.as_str())
                    },
                ),
            )
            .await?
        };
        let traces = parse_summary_rows(initial_rows);
        return run_interactive(
            InteractiveRunArgs {
                init: TraceViewerInit {
                    project,
                    source_expr,
                    list_mode: args.list_mode,
                    base_filter,
                    traces,
                    limit: args.limit,
                    preview_length: args.preview_length,
                    print_queries: args.print_queries,
                },
                initial_search_query: search_query,
                startup_trace_url: parsed_startup_url,
            },
            client,
        )
        .await;
    }

    let response = with_spinner(
        "Loading logs...",
        fetch_logs_page(
            &client,
            &logs_query_ctx,
            args.limit,
            args.cursor.as_deref(),
            if search_query.trim().is_empty() {
                None
            } else {
                Some(search_query.as_str())
            },
        ),
    )
    .await?;
    let next_cursor = response.cursor.clone().filter(|c| !c.is_empty());
    let has_more = next_cursor.is_some();
    let rows = parse_summary_rows(response.data);
    let object_ref_arg = format_object_ref_arg(&object_ref);
    let profile_flag = base.profile.as_deref();

    if base.json {
        let payload = json!({
            "meta": {
                "command": "view logs",
                "source": object_ref,
                "list_mode": args.list_mode,
                "limit": args.limit,
                "preview_length": args.preview_length,
                "truncated": true,
            },
            "paging": {
                "cursor_in": args.cursor,
                "next_cursor": next_cursor.clone(),
                "has_more": has_more,
            },
            "items": rows,
            "hints": logs_hints(
                has_more,
                &object_ref_arg,
                next_cursor.as_deref(),
                profile_flag,
            ),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_logs_text(
            &rows,
            args.list_mode,
            &object_ref_arg,
            profile_flag,
            args.limit,
            args.preview_length,
            next_cursor.as_deref(),
        );
    }

    Ok(())
}

async fn run_trace_command(base: BaseArgs, client: ApiClient, args: TraceArgs) -> Result<()> {
    if args.limit == 0 {
        bail!("--limit must be greater than 0");
    }
    if args.preview_length == 0 {
        bail!("--preview-length must be greater than 0");
    }
    if args.object_ref.is_some() && args.project_id.is_some() {
        bail!("--object-ref and --project-id are mutually exclusive");
    }
    let startup_url = select_startup_url(args.url.as_deref(), args.url_arg.as_deref())?;
    let parsed_startup_url = startup_url.as_deref().map(parse_trace_url).transpose()?;
    let project_from_url = parsed_startup_url
        .as_ref()
        .and_then(|u| u.project.as_deref());

    let (mut object_ref, project_for_url) = resolve_object_ref_for_view(
        &client,
        &base,
        args.object_ref.as_deref(),
        args.project_id.as_deref(),
        project_from_url,
    )
    .await?;

    let mut trace_id = args
        .trace_id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);

    let interactive = !base.json && std::io::stdin().is_terminal() && !args.non_interactive;
    if interactive {
        if object_ref.object_type != "project_logs" {
            bail!("interactive trace view currently supports only project_logs sources");
        }

        let project = project_for_url.unwrap_or(ProjectSelection {
            id: object_ref.object_name.clone(),
            name: None,
        });
        let startup_target = if let Some(parsed) = parsed_startup_url.clone() {
            parsed
        } else {
            let trace_id = trace_id.clone().context(
                "trace id is required. Pass --trace-id <root-span-id> or --url <trace-url>",
            )?;
            ParsedTraceUrl {
                org: Some(client.org_name().to_string()),
                project: Some(project.id.clone()),
                page: Some("logs".to_string()),
                row_ref: Some(trace_id.clone()),
                span_id: Some(trace_id),
                trace_view_type: None,
            }
        };
        let source_expr = btql_source_expr(&object_ref)?;
        let base_filter = build_base_filter_clause(None, "1h", None)?;
        return run_interactive(
            InteractiveRunArgs {
                init: TraceViewerInit {
                    project,
                    source_expr,
                    list_mode: ListMode::Summary,
                    base_filter,
                    traces: Vec::new(),
                    limit: args.limit,
                    preview_length: args.preview_length,
                    print_queries: args.print_queries,
                },
                initial_search_query: String::new(),
                startup_trace_url: Some(startup_target),
            },
            client,
        )
        .await;
    }

    if trace_id.is_none() {
        let parsed = parsed_startup_url.as_ref().with_context(|| {
            "trace id is required. Pass --trace-id <root-span-id> or --url <trace-url>"
        })?;

        if object_ref.object_type != "project_logs" {
            bail!("URL trace resolution currently supports only project_logs sources");
        }
        let current_project = project_for_url.clone().unwrap_or(ProjectSelection {
            id: object_ref.object_name.clone(),
            name: None,
        });
        let target = with_spinner(
            "Resolving trace URL...",
            resolve_trace_target_for_url(&client, &current_project, parsed, args.print_queries),
        )
        .await?;
        trace_id = Some(target.root_span_id.clone());
        object_ref = ObjectRef {
            object_type: "project_logs".to_string(),
            object_name: target.project.id.clone(),
        };
    }

    let trace_id = trace_id.context("trace id resolution failed")?;
    let source_expr = btql_source_expr(&object_ref)?;
    let response = with_spinner(
        "Loading trace spans...",
        fetch_trace_spans_page(
            &client,
            &source_expr,
            &trace_id,
            args.preview_length,
            args.limit,
            args.cursor.as_deref(),
            args.print_queries,
        ),
    )
    .await?;
    let next_cursor = response.cursor.clone().filter(|c| !c.is_empty());
    let has_more = next_cursor.is_some();
    let object_ref_arg = format_object_ref_arg(&object_ref);
    let profile_flag = base.profile.as_deref();

    if base.json {
        let payload = json!({
            "meta": {
                "command": "view trace",
                "source": object_ref,
                "trace_id": trace_id,
                "limit": args.limit,
                "preview_length": args.preview_length,
                "truncated": true,
            },
            "paging": {
                "cursor_in": args.cursor,
                "next_cursor": next_cursor.clone(),
                "has_more": has_more,
            },
            "items": response.data,
            "hints": trace_hints(
                has_more,
                &object_ref_arg,
                &trace_id,
                next_cursor.as_deref(),
                profile_flag,
            ),
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_trace_text(
            &trace_id,
            &response.data,
            &object_ref_arg,
            profile_flag,
            args.limit,
            args.preview_length,
            next_cursor.as_deref(),
        );
    }

    Ok(())
}

async fn run_span_command(base: BaseArgs, client: ApiClient, args: SpanArgs) -> Result<()> {
    if args.object_ref.is_some() && args.project_id.is_some() {
        bail!("--object-ref and --project-id are mutually exclusive");
    }
    let startup_url = select_startup_url(args.url.as_deref(), args.url_arg.as_deref())?;
    let parsed_startup_url = startup_url.as_deref().map(parse_trace_url).transpose()?;
    let project_from_url = parsed_startup_url
        .as_ref()
        .and_then(|u| u.project.as_deref());

    let (object_ref, _project_for_url) = resolve_object_ref_for_view(
        &client,
        &base,
        args.object_ref.as_deref(),
        args.project_id.as_deref(),
        project_from_url,
    )
    .await?;

    let mut row_id = args
        .id
        .as_deref()
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(ToString::to_string);
    let mut span_id_from_url: Option<String> = None;

    if row_id.is_none() {
        if let Some(parsed) = parsed_startup_url.as_ref() {
            if let Some(s) = parsed.span_id.as_deref().filter(|v| !v.is_empty()) {
                span_id_from_url = Some(s.to_string());
            } else if let Some(r) = parsed.row_ref.as_deref().filter(|v| !v.is_empty()) {
                row_id = Some(r.to_string());
            }
        }
    }
    if row_id.is_none() && span_id_from_url.is_none() {
        bail!("span selector required. Pass --id <row-id> or --url <trace-url>");
    }

    let interactive = !base.json && std::io::stdin().is_terminal() && !args.non_interactive;
    if interactive {
        if object_ref.object_type != "project_logs" {
            bail!("interactive span view currently supports only project_logs sources");
        }

        let project = ProjectSelection {
            id: object_ref.object_name.clone(),
            name: None,
        };
        let startup_target = if let Some(mut parsed) = parsed_startup_url.clone() {
            if parsed.span_id.is_none() {
                parsed.span_id = parsed.row_ref.clone();
            }
            parsed
        } else {
            ParsedTraceUrl {
                org: Some(client.org_name().to_string()),
                project: Some(project.id.clone()),
                page: Some("logs".to_string()),
                row_ref: row_id.clone(),
                span_id: row_id.clone().or(span_id_from_url.clone()),
                trace_view_type: None,
            }
        };
        let source_expr = btql_source_expr(&object_ref)?;
        let base_filter = build_base_filter_clause(None, "1h", None)?;
        return run_interactive(
            InteractiveRunArgs {
                init: TraceViewerInit {
                    project,
                    source_expr,
                    list_mode: ListMode::Summary,
                    base_filter,
                    traces: Vec::new(),
                    limit: 50,
                    preview_length: 125,
                    print_queries: args.print_queries,
                },
                initial_search_query: String::new(),
                startup_trace_url: Some(startup_target),
            },
            client,
        )
        .await;
    }

    let source_expr = btql_source_expr(&object_ref)?;
    let item = if let Some(id) = row_id.as_deref() {
        let query = build_full_span_query_by_id(&source_expr, id);
        maybe_print_query(args.print_queries, "span-full-id", &query);
        let response = with_spinner("Loading span...", async {
            execute_query(&client, &query)
                .await
                .with_context(|| format!("BTQL query failed: {query}"))
        })
        .await?;
        response.data.into_iter().next()
    } else {
        let query = build_full_span_query_by_span_id(
            &source_expr,
            span_id_from_url
                .as_deref()
                .expect("URL span id should be present when row_id is absent"),
        );
        maybe_print_query(args.print_queries, "span-full-span-id", &query);
        let response = with_spinner("Loading span...", async {
            execute_query(&client, &query)
                .await
                .with_context(|| format!("BTQL query failed: {query}"))
        })
        .await?;
        response.data.into_iter().next()
    };

    if base.json {
        let payload = json!({
            "meta": {
                "command": "view span",
                "source": object_ref,
                "id": row_id,
                "span_id": span_id_from_url,
                "full": true,
                "truncated": false,
            },
            "item": item,
            "hints": ["Single span fetch returns full content. Write to a file if needed."],
        });
        println!("{}", serde_json::to_string_pretty(&payload)?);
    } else {
        print_span_text(item.as_ref());
    }

    Ok(())
}

async fn resolve_object_ref_for_view(
    client: &ApiClient,
    base: &BaseArgs,
    object_ref: Option<&str>,
    project_id: Option<&str>,
    project_name_from_url: Option<&str>,
) -> Result<(ObjectRef, Option<ProjectSelection>)> {
    if let Some(raw) = object_ref {
        let parsed = parse_object_ref(raw)?;
        let project_for_ui = if parsed.object_type == "project_logs" {
            Some(ProjectSelection {
                id: parsed.object_name.clone(),
                name: None,
            })
        } else {
            None
        };
        return Ok((parsed, project_for_ui));
    }

    let cfg_project = crate::config::load().ok().and_then(|c| c.project);
    let project = resolve_project(
        client,
        base.project.as_deref().or(cfg_project.as_deref()),
        project_id,
        project_name_from_url,
    )
    .await?;
    Ok((
        ObjectRef {
            object_type: "project_logs".to_string(),
            object_name: project.id.clone(),
        },
        Some(project),
    ))
}

async fn resolve_project(
    client: &ApiClient,
    project_name_from_base: Option<&str>,
    explicit_project_id: Option<&str>,
    project_name_from_url: Option<&str>,
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

    if let Some(name) = project_name_from_url {
        if is_uuid_like(name) {
            return Ok(ProjectSelection {
                id: name.to_string(),
                name: None,
            });
        }
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
    let selected = fuzzy_select("Select project", &names, 0)?;
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

async fn run_interactive(args: InteractiveRunArgs, client: ApiClient) -> Result<()> {
    let InteractiveRunArgs {
        init,
        initial_search_query,
        startup_trace_url,
    } = args;
    let handle = tokio::runtime::Handle::current();
    let mut app = TraceViewerApp::new(init);
    app.trace_search_query = initial_search_query.clone();
    app.trace_search_input = initial_search_query;
    tokio::task::block_in_place(|| run_interactive_blocking(app, startup_trace_url, client, handle))
}

fn run_interactive_blocking(
    mut app: TraceViewerApp,
    startup_trace_url: Option<ParsedTraceUrl>,
    client: ApiClient,
    handle: tokio::runtime::Handle,
) -> Result<()> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    stdout.execute(EnterAlternateScreen)?;
    let backend = CrosstermBackend::new(stdout);
    let mut terminal = Terminal::new(backend)?;

    if let Some(parsed_url) = startup_trace_url {
        request_open_trace_for_parsed_url(&mut app, &client, &handle, parsed_url)?;
    }
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
        poll_pending_url_open(app, &client, &handle)?;
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
    if app
        .pending_g_prefix_at
        .map(|at| at.elapsed() > DOUBLE_G_TIMEOUT)
        .unwrap_or(false)
    {
        app.pending_g_prefix_at = None;
    }

    if matches!(key.code, KeyCode::Char('k') | KeyCode::Char('K'))
        && key.modifiers.contains(KeyModifiers::CONTROL)
    {
        app.url_open_mode = true;
        app.url_open_input.clear();
        app.trace_search_mode = false;
        app.set_status("Open URL mode. Paste a Braintrust URL and press Enter (Esc cancels).");
        return Ok(false);
    }

    if app.url_open_mode {
        handle_url_open_input_key(app, key, client, handle)?;
        return Ok(false);
    }

    let is_plain_g = matches!(key.code, KeyCode::Char('g')) && key.modifiers.is_empty();
    if !is_plain_g {
        app.pending_g_prefix_at = None;
    }

    match key.code {
        KeyCode::Char('q')
            if !(app.screen == Screen::Traces
                && app.trace_search_mode
                && key.modifiers.is_empty()) =>
        {
            return Ok(true)
        }
        KeyCode::Esc => {
            if app.screen == Screen::SpanDetail && app.message_viewer_open {
                app.close_message_viewer();
                app.set_status(app.span_detail_status_text());
                return Ok(false);
            }
            if app.screen == Screen::SpanDetail {
                app.screen = Screen::Traces;
                app.set_status(
                    "Back to traces. Up/Down: move  Enter: open trace  /: search  Ctrl+k: open URL  r: refresh  q: quit",
                );
                ensure_traces_loaded_after_detail_exit(app, client, handle)?;
                return Ok(false);
            }
            if app.screen == Screen::OpeningTrace {
                app.set_status("Opening trace in progress. Press q to quit.");
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
        Screen::OpeningTrace => {}
    }

    Ok(false)
}

fn ensure_traces_loaded_after_detail_exit(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let opened_root = app.loaded_root_span_id.clone();

    let mut root_found_in_table = false;
    if let Some(root_span_id) = opened_root.as_deref() {
        if let Some(idx) = app
            .traces
            .iter()
            .position(|row| row.root_span_id == root_span_id)
        {
            app.selected_trace = idx;
            root_found_in_table = true;
        }
    }

    let should_refresh = app.traces.is_empty() || !root_found_in_table;
    if !should_refresh {
        return Ok(());
    }

    let search_query = app.trace_search_query.trim().to_string();
    let previous_root = opened_root;
    if app.summary_load_rx.is_some() {
        app.summary_pending_refresh = Some((search_query, previous_root));
    } else {
        start_summary_load(app, client, handle, search_query, previous_root);
    }

    Ok(())
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
        KeyCode::Char('g') if key.modifiers.is_empty() => {
            if app.pending_g_prefix_at.take().is_some() {
                app.move_trace_top();
            } else {
                app.pending_g_prefix_at = Some(Instant::now());
            }
        }
        KeyCode::Char('G') => app.move_trace_bottom(),
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

fn handle_url_open_input_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    match key.code {
        KeyCode::Esc => {
            app.url_open_mode = false;
            app.url_open_input.clear();
            app.set_status("URL open canceled");
        }
        KeyCode::Enter => {
            let input = app.url_open_input.trim().to_string();
            app.url_open_mode = false;
            if input.is_empty() {
                app.set_status("Open URL canceled");
            } else if let Err(err) = request_open_trace_for_url(app, client, handle, input) {
                app.set_error(format!("Failed to open URL: {}", root_error_message(&err)));
            }
        }
        KeyCode::Backspace => {
            app.url_open_input.pop();
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            app.url_open_input.clear();
        }
        KeyCode::Char(c) if key.modifiers.is_empty() || key.modifiers == KeyModifiers::SHIFT => {
            app.url_open_input.push(c);
        }
        _ => {}
    }

    if app.url_open_mode {
        let preview = if app.url_open_input.is_empty() {
            "<empty>".to_string()
        } else {
            truncate_chars(&app.url_open_input, 120)
        };
        app.set_status(format!("Open URL mode: {preview}"));
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

fn request_open_trace_for_url(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    url: String,
) -> Result<()> {
    let parsed = parse_trace_url(&url)?;
    request_open_trace_for_parsed_url(app, client, handle, parsed)
}

fn request_open_trace_for_parsed_url(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
    parsed: ParsedTraceUrl,
) -> Result<()> {
    if app.url_open_rx.is_some() {
        app.set_status("A URL open is already in progress");
        return Ok(());
    }

    let url_for_status = parsed
        .page
        .as_deref()
        .map(|page| format!("{}/{}", project_label(&app.project), page))
        .unwrap_or_else(|| project_label(&app.project));
    let (tx, rx) = mpsc::channel();
    let client = client.clone();
    let current_project = app.project.clone();
    let print_queries = app.print_queries;
    let parsed_for_task = parsed.clone();
    handle.spawn(async move {
        let result = resolve_trace_target_for_url(
            &client,
            &current_project,
            &parsed_for_task,
            print_queries,
        )
        .await;
        let _ = tx.send((url_for_status, result));
    });

    app.url_open_loading_input = parsed
        .row_ref
        .clone()
        .or_else(|| parsed.span_id.clone())
        .unwrap_or_else(|| "<url>".to_string());
    app.url_open_started_at = Some(Instant::now());
    app.url_open_spinner_tick = 0;
    app.url_open_rx = Some(rx);
    app.screen = Screen::OpeningTrace;
    app.set_status(format!(
        "Resolving URL for {}...",
        app.url_open_loading_input
    ));
    Ok(())
}

fn handle_span_detail_key(
    app: &mut TraceViewerApp,
    key: KeyEvent,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    let detail_message_list_mode = app.detail_pane_focus == DetailPaneFocus::Detail
        && (app.span_detail_shows_messages()
            || (app.detail_view == DetailView::Thread
                && !app.thread_loading
                && app.thread_messages.is_some()));
    let message_viewer_mode = app.message_viewer_open && detail_message_list_mode;

    if message_viewer_mode {
        match key.code {
            KeyCode::Char('g') if key.modifiers.is_empty() => {
                if app.pending_g_prefix_at.take().is_some() {
                    app.message_viewer_scroll = 0;
                } else {
                    app.pending_g_prefix_at = Some(Instant::now());
                }
            }
            KeyCode::Char('G') => {
                scroll_message_viewer_to_bottom(app);
            }
            KeyCode::Up | KeyCode::Char('k') => {
                app.message_viewer_scroll = app.message_viewer_scroll.saturating_sub(1)
            }
            KeyCode::Down | KeyCode::Char('j') => scroll_message_viewer_down_bounded(app, 1),
            KeyCode::PageUp => {
                app.message_viewer_scroll = app.message_viewer_scroll.saturating_sub(10)
            }
            KeyCode::PageDown => scroll_message_viewer_down_bounded(app, 10),
            KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                app.message_viewer_scroll = app.message_viewer_scroll.saturating_sub(10)
            }
            KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
                scroll_message_viewer_down_bounded(app, 10)
            }
            KeyCode::Esc | KeyCode::Char('v') => {
                app.close_message_viewer();
                app.set_status(app.span_detail_status_text());
            }
            KeyCode::Backspace => {
                app.close_message_viewer();
                app.set_status(app.span_detail_status_text());
            }
            _ => {}
        }
        return Ok(());
    }

    match key.code {
        KeyCode::Char('g') if key.modifiers.is_empty() => {
            if app.pending_g_prefix_at.take().is_some() {
                if app.detail_pane_focus == DetailPaneFocus::Tree {
                    app.move_span_top();
                    if app.detail_view == DetailView::Span {
                        request_selected_span_load(app, client, handle, false)?;
                    }
                } else if app.detail_view == DetailView::Thread {
                    app.move_thread_message_top();
                } else if detail_message_list_mode {
                    app.move_span_detail_message_top();
                } else {
                    app.scroll_detail_top();
                }
            } else {
                app.pending_g_prefix_at = Some(Instant::now());
            }
        }
        KeyCode::Char('G') => {
            if app.detail_pane_focus == DetailPaneFocus::Tree {
                app.move_span_bottom();
                if app.detail_view == DetailView::Span {
                    request_selected_span_load(app, client, handle, false)?;
                }
            } else if app.detail_view == DetailView::Thread {
                app.move_thread_message_bottom();
            } else if detail_message_list_mode {
                app.move_span_detail_message_bottom();
            } else {
                scroll_detail_to_bottom(app);
            }
        }
        KeyCode::Up | KeyCode::Char('k') => {
            if app.detail_pane_focus == DetailPaneFocus::Tree {
                app.move_span_up();
                if app.detail_view == DetailView::Span {
                    request_selected_span_load(app, client, handle, false)?;
                }
            } else if app.detail_view == DetailView::Thread {
                app.move_thread_message_up();
            } else if detail_message_list_mode {
                app.move_span_detail_message_up();
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
            } else if detail_message_list_mode {
                app.move_span_detail_message_down();
            } else {
                scroll_detail_down_bounded(app, 1);
            }
        }
        KeyCode::PageUp => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                if detail_message_list_mode {
                    if app.detail_view == DetailView::Thread {
                        for _ in 0..10 {
                            app.move_thread_message_up();
                        }
                    } else {
                        for _ in 0..10 {
                            app.move_span_detail_message_up();
                        }
                    }
                } else {
                    app.scroll_detail_up(10);
                }
            }
        }
        KeyCode::PageDown => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                if detail_message_list_mode {
                    if app.detail_view == DetailView::Thread {
                        for _ in 0..10 {
                            app.move_thread_message_down();
                        }
                    } else {
                        for _ in 0..10 {
                            app.move_span_detail_message_down();
                        }
                    }
                } else {
                    scroll_detail_down_bounded(app, 10);
                }
            }
        }
        KeyCode::Backspace => {
            app.screen = Screen::Traces;
            app.set_status(
                "Back to traces. Up/Down: move  Enter: open trace  /: search  Ctrl+k: open URL  r: refresh  q: quit",
            );
        }
        KeyCode::Left | KeyCode::Char('h') => {
            app.detail_pane_focus = DetailPaneFocus::Tree;
            app.set_status(app.span_detail_status_text());
        }
        KeyCode::Right | KeyCode::Char('l') => {
            app.detail_pane_focus = DetailPaneFocus::Detail;
            app.set_status(app.span_detail_status_text());
        }
        KeyCode::Enter => {
            if detail_message_list_mode {
                if selected_detail_message(app).is_some() {
                    app.open_message_viewer();
                    app.set_status(app.span_detail_status_text());
                }
                return Ok(());
            }

            if app.detail_view == DetailView::Span {
                request_selected_span_load(app, client, handle, true)?;
            } else if app.thread_loading || app.thread_messages.is_none() {
                request_thread_messages_load(app, client, handle, true)?;
            } else {
                app.toggle_selected_thread_message();
            }
        }
        KeyCode::Char('v') => {
            if detail_message_list_mode {
                app.open_message_viewer();
                app.set_status(app.span_detail_status_text());
            }
        }
        KeyCode::Char('t') => {
            app.detail_scroll = 0;
            app.close_message_viewer();
            app.detail_view = if app.detail_view == DetailView::Span {
                DetailView::Thread
            } else {
                DetailView::Span
            };

            if app.detail_view == DetailView::Thread {
                app.detail_pane_focus = DetailPaneFocus::Detail;
                request_thread_messages_load(app, client, handle, false)?;
            } else {
                app.set_status(app.span_detail_status_text());
                request_selected_span_load(app, client, handle, false)?;
            }
        }
        KeyCode::Tab => {
            if app.detail_view == DetailView::Span {
                app.toggle_span_detail_tab();
                app.set_status(app.span_detail_status_text());
            }
        }
        KeyCode::Char('r') => {
            if app.loaded_root_span_id.is_some() {
                request_loaded_trace_reload(app, client, handle)?;
            }
        }
        KeyCode::Char(' ') => {
            if app.detail_pane_focus == DetailPaneFocus::Tree
                && app.toggle_selected_span_collapsed()
            {
                app.set_status(app.span_detail_status_text());
            }
        }
        KeyCode::Char('u') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                if detail_message_list_mode {
                    if app.detail_view == DetailView::Thread {
                        for _ in 0..10 {
                            app.move_thread_message_up();
                        }
                    } else {
                        for _ in 0..10 {
                            app.move_span_detail_message_up();
                        }
                    }
                } else {
                    app.scroll_detail_up(10)
                }
            }
        }
        KeyCode::Char('d') if key.modifiers.contains(KeyModifiers::CONTROL) => {
            if app.detail_pane_focus == DetailPaneFocus::Detail {
                if detail_message_list_mode {
                    if app.detail_view == DetailView::Thread {
                        for _ in 0..10 {
                            app.move_thread_message_down();
                        }
                    } else {
                        for _ in 0..10 {
                            app.move_span_detail_message_down();
                        }
                    }
                } else {
                    scroll_detail_down_bounded(app, 10)
                }
            }
        }
        _ => {}
    }
    Ok(())
}

fn scroll_detail_down_bounded(app: &mut TraceViewerApp, amount: u16) {
    let max_scroll = current_span_detail_max_scroll(app).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL);
    app.detail_scroll = app.detail_scroll.saturating_add(amount).min(max_scroll);
}

fn scroll_detail_to_bottom(app: &mut TraceViewerApp) {
    let max_scroll = current_span_detail_max_scroll(app).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL);
    app.detail_scroll = max_scroll;
}

fn current_span_detail_max_scroll(app: &TraceViewerApp) -> Option<u16> {
    if app.detail_view != DetailView::Span || app.span_detail_shows_messages() {
        return None;
    }

    let span = app.current_span()?;
    let full_row = app.full_span_cache.get(&span.id);
    let is_loading = app
        .full_span_loading_row_id
        .as_deref()
        .map(|id| id == span.id)
        .unwrap_or(false);

    let detail_text = render_span_details(
        span,
        full_row.unwrap_or(&span.row),
        full_row.is_some(),
        is_loading,
        None,
    );

    let (content_width, content_height) = span_detail_content_size()?;
    let wrapped_lines = wrapped_line_count(&detail_text, content_width);
    let max_scroll = wrapped_lines.saturating_sub(content_height);
    Some(u16::try_from(max_scroll).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL))
}

fn selected_detail_message(app: &TraceViewerApp) -> Option<&Value> {
    match app.detail_view {
        DetailView::Thread => app
            .thread_messages
            .as_ref()
            .and_then(|messages| messages.get(app.thread_selected_message)),
        DetailView::Span if app.span_detail_shows_messages() => app
            .span_detail_messages
            .as_ref()
            .and_then(|messages| messages.get(app.span_detail_message_selected)),
        _ => None,
    }
}

fn selected_message_viewer_max_scroll(app: &TraceViewerApp) -> Option<u16> {
    if !app.message_viewer_open {
        return None;
    }

    let message = selected_detail_message(app)?;
    let text = render_full_message_text(message);
    let (content_width, content_height) = message_viewer_content_size(app)?;
    let wrapped_lines = wrapped_line_count(&text, content_width);
    let max_scroll = wrapped_lines.saturating_sub(content_height);
    Some(u16::try_from(max_scroll).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL))
}

fn message_viewer_content_size(app: &TraceViewerApp) -> Option<(usize, usize)> {
    let (terminal_width, terminal_height) = crossterm::terminal::size().ok()?;
    let center_height = terminal_height.saturating_sub(6);
    let detail_width = ((u32::from(terminal_width) * 65) / 100) as u16;

    // Inside border for the detail panel.
    let content_width = detail_width.saturating_sub(2).max(1);
    let content_height = match app.detail_view {
        DetailView::Span => center_height.saturating_sub(5).saturating_sub(2).max(1),
        DetailView::Thread => center_height.saturating_sub(2).max(1),
    };
    Some((usize::from(content_width), usize::from(content_height)))
}

fn scroll_message_viewer_down_bounded(app: &mut TraceViewerApp, amount: u16) {
    let max_scroll = selected_message_viewer_max_scroll(app).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL);
    app.message_viewer_scroll = app
        .message_viewer_scroll
        .saturating_add(amount)
        .min(max_scroll);
}

fn scroll_message_viewer_to_bottom(app: &mut TraceViewerApp) {
    let max_scroll = selected_message_viewer_max_scroll(app).unwrap_or(MAX_SAFE_PARAGRAPH_SCROLL);
    app.message_viewer_scroll = max_scroll;
}

fn span_detail_content_size() -> Option<(usize, usize)> {
    let (terminal_width, terminal_height) = crossterm::terminal::size().ok()?;
    let center_height = terminal_height.saturating_sub(6);
    let detail_width = ((u32::from(terminal_width) * 65) / 100) as u16;

    let content_width = detail_width.saturating_sub(2).max(1);
    let content_height = center_height.saturating_sub(2).max(1);
    Some((usize::from(content_width), usize::from(content_height)))
}

fn wrapped_line_count(text: &str, width: usize) -> usize {
    if width == 0 {
        return text.lines().count().max(1);
    }

    let mut total = 0usize;
    for line in text.split('\n') {
        let line_width = UnicodeWidthStr::width(line);
        total += ((line_width + width.saturating_sub(1)) / width).max(1);
    }
    total.max(1)
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
    let logs_query_ctx = LogsQueryContext {
        source_expr: app.source_expr.clone(),
        list_mode: app.list_mode,
        preview_length: app.preview_length,
        base_filter: app.base_filter.clone(),
        print_queries: app.print_queries,
    };
    let limit = app.limit;
    let search_for_task = search_query.clone();
    let previous_root_for_task = previous_root.clone();

    handle.spawn(async move {
        let search_opt = if search_for_task.trim().is_empty() {
            None
        } else {
            Some(search_for_task.as_str())
        };
        let result = fetch_summary_rows(&client, &logs_query_ctx, limit, search_opt).await;
        let _ = tx.send((search_for_task, previous_root_for_task, result));
    });

    app.summary_loading_search_query = search_query.clone();
    app.summary_load_started_at = Some(Instant::now());
    app.summary_spinner_tick = 0;
    app.summary_load_rx = Some(rx);
    if app.screen == Screen::Traces {
        let noun = if app.list_mode == ListMode::Spans {
            "span"
        } else {
            "trace"
        };
        if search_query.is_empty() {
            app.set_status(format!("Refreshing {noun} table..."));
        } else {
            app.set_status(format!("Searching {noun}s for '{search_query}'..."));
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
            let noun = if app.list_mode == ListMode::Spans {
                "spans"
            } else {
                "traces"
            };
            if search_query.trim().is_empty() {
                app.set_status(format!("No {noun} found for the selected project"));
            } else {
                app.set_status(format!("No {noun} found for search '{search_query}'"));
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
        let noun = if app.list_mode == ListMode::Spans {
            "spans"
        } else {
            "traces"
        };
        if search_query.trim().is_empty() {
            app.set_status(format!(
                "Loaded {} {}. Up/Down: move  Enter: open trace  /: search  Ctrl+k: open URL  r: refresh  q: quit",
                app.traces.len(),
                noun
            ));
        } else {
            app.set_status(format!(
                "Loaded {} {} for '{search_query}'. Up/Down: move  Enter: open trace  /: search  Ctrl+k: open URL  r: refresh",
                app.traces.len(),
                noun
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

fn poll_pending_url_open(
    app: &mut TraceViewerApp,
    client: &ApiClient,
    handle: &tokio::runtime::Handle,
) -> Result<()> {
    if app.url_open_rx.is_none() {
        return Ok(());
    }

    app.url_open_spinner_tick = app.url_open_spinner_tick.wrapping_add(1);

    if app
        .url_open_started_at
        .map(|start| start.elapsed() >= LOADER_DELAY)
        .unwrap_or(false)
    {
        app.set_status(format!(
            "Resolving URL {} {}",
            app.url_open_loading_input,
            spinner_char(app.url_open_spinner_tick)
        ));
    }

    let recv_result = app.url_open_rx.as_ref().map(|rx| rx.try_recv());
    let Some(recv_result) = recv_result else {
        return Ok(());
    };

    match recv_result {
        Ok((url_label, result)) => {
            app.url_open_rx = None;
            app.url_open_started_at = None;
            app.url_open_loading_input.clear();
            match result {
                Ok(target) => {
                    if target.project.id != app.project.id {
                        app.project = target.project.clone();
                        app.screen = Screen::Traces;
                        app.close_message_viewer();
                        app.traces.clear();
                        app.selected_trace = 0;
                        app.summary_load_rx = None;
                        app.summary_load_started_at = None;
                        app.summary_loading_search_query.clear();
                        app.summary_pending_refresh = None;
                        app.trace_load_rx = None;
                        app.trace_loading_root_span_id = None;
                        app.trace_pending_root_span_id = None;
                        app.trace_load_started_at = None;
                        app.full_span_load_rx = None;
                        app.full_span_loading_row_id = None;
                        app.full_span_pending_row_id = None;
                        app.full_span_load_started_at = None;
                        app.spans.clear();
                        app.collapsed_span_row_ids.clear();
                        app.full_span_cache.clear();
                        app.span_detail_messages = None;
                        app.span_detail_message_selected = 0;
                        app.span_detail_message_expanded.clear();
                        app.loaded_root_span_id = None;
                        request_refresh_traces(app, client, handle)?;
                    }

                    let target_root = target.root_span_id.clone();
                    app.pending_open_span_id = target.span_id.clone();
                    app.pending_open_detail_view = Some(target.detail_view);
                    request_trace_load_for_root_span(app, client, handle, target_root.clone())?;
                    if app.screen == Screen::Traces {
                        if let Some(idx) = app
                            .traces
                            .iter()
                            .position(|row| row.root_span_id == target_root)
                        {
                            app.selected_trace = idx;
                        }
                    }
                    app.set_status(format!("Opening trace from URL {url_label}..."));
                }
                Err(err) => {
                    app.screen = Screen::Traces;
                    let detail = root_error_message(&err);
                    if let Some(query) = query_from_error(&err) {
                        app.set_error_with_query(
                            format!("Failed to resolve trace URL: {detail}"),
                            query,
                        );
                    } else {
                        app.set_error(format!("Failed to resolve trace URL: {detail}"));
                    }
                }
            }
        }
        Err(TryRecvError::Empty) => {}
        Err(TryRecvError::Disconnected) => {
            app.url_open_rx = None;
            app.url_open_started_at = None;
            app.url_open_loading_input.clear();
            app.set_error("Failed to resolve URL: request channel closed");
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
    let preview_length = app.preview_length;
    let print_queries = app.print_queries;
    let root_span_id_for_task = root_span_id.clone();

    handle.spawn(async move {
        let result = fetch_trace_span_rows_for_tree(
            &client,
            &project_id,
            &root_span_id_for_task,
            preview_length,
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
    app.set_default_tree_collapse_state();
    app.full_span_cache.clear();
    app.full_span_loading_row_id = None;
    app.full_span_pending_row_id = None;
    app.full_span_load_started_at = None;
    app.full_span_spinner_tick = 0;
    app.full_span_load_rx = None;
    app.selected_span = 0;
    app.detail_scroll = 0;
    if let Some(target_span_id) = app.pending_open_span_id.take() {
        if let Some(idx) = app
            .spans
            .iter()
            .position(|span| span.span_id == target_span_id || span.id == target_span_id)
        {
            app.selected_span = idx;
        }
    }
    app.expand_ancestor_chain_for_selected_span();
    app.ensure_selected_span_visible();
    app.detail_view = app
        .pending_open_detail_view
        .take()
        .unwrap_or(DetailView::Span);
    app.detail_pane_focus = if app.detail_view == DetailView::Thread {
        DetailPaneFocus::Detail
    } else {
        DetailPaneFocus::Tree
    };
    app.thread_messages = None;
    app.thread_selected_message = 0;
    app.thread_expanded.clear();
    app.thread_loading = false;
    app.thread_spinner_tick = 0;
    app.thread_load_rx = None;
    app.span_detail_messages = None;
    app.span_detail_message_selected = 0;
    app.span_detail_message_expanded.clear();
    app.close_message_viewer();
    app.loaded_root_span_id = Some(root_span_id.clone());
    app.screen = Screen::SpanDetail;
    app.refresh_span_detail_messages(true);
    app.set_status(app.span_detail_status_text());
    request_selected_span_load(app, client, handle, false)?;
    if app.detail_view == DetailView::Thread {
        request_thread_messages_load(app, client, handle, false)?;
    }

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
                    if app.screen == Screen::OpeningTrace {
                        app.screen = Screen::Traces;
                    }
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
        Screen::OpeningTrace => "Trace Explorer / Opening",
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
            .title("bt view logs"),
    );
    frame.render_widget(header, chunks[0]);

    match app.screen {
        Screen::Traces => draw_traces_table(frame, chunks[1], app),
        Screen::SpanDetail => draw_span_detail_view(frame, chunks[1], app),
        Screen::OpeningTrace => draw_opening_trace_view(frame, chunks[1], app),
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
    let url_loading = app
        .url_open_started_at
        .map(|start| start.elapsed() >= LOADER_DELAY)
        .unwrap_or(false);
    let url_spinner = spinner_char(app.url_open_spinner_tick);

    let top_text = if app.url_open_mode {
        app.url_open_input.as_str()
    } else if app.trace_search_mode {
        app.trace_search_input.as_str()
    } else {
        app.trace_search_query.as_str()
    };
    let search_line = if top_text.trim().is_empty() {
        Line::from(Span::styled(
            if app.url_open_mode {
                "Paste Braintrust trace URL"
            } else {
                "Press / to search traces"
            },
            Style::default().fg(Color::DarkGray),
        ))
    } else if app.trace_search_mode || app.url_open_mode {
        Line::from(Span::raw(format!("{top_text}█")))
    } else {
        Line::from(Span::raw(top_text))
    };
    let search_block = Block::default()
        .title(if app.url_open_mode {
            "Open URL [active]".to_string()
        } else if url_loading {
            format!("Open URL [loading {url_spinner}]")
        } else if app.trace_search_mode {
            "Search [active]".to_string()
        } else if summary_loading {
            format!("Search [loading {summary_spinner}]")
        } else {
            "Search".to_string()
        })
        .borders(Borders::ALL)
        .border_type(BorderType::Rounded)
        .border_style(
            if app.url_open_mode || url_loading || app.trace_search_mode || summary_loading {
                Style::default()
                    .fg(Color::Cyan)
                    .add_modifier(Modifier::BOLD)
            } else {
                Style::default()
            },
        )
        .title_bottom(if app.url_open_mode {
            "Enter open  Esc cancel  Ctrl+u clear"
        } else if url_loading {
            "Resolving URL..."
        } else if app.trace_search_mode {
            "Enter apply  Esc cancel  Ctrl+u clear"
        } else if summary_loading {
            let search = app.summary_loading_search_query.trim();
            if search.is_empty() {
                "Refreshing traces..."
            } else {
                "Searching..."
            }
        } else {
            "/ search  Ctrl+k open URL"
        });
    let search_widget = Paragraph::new(search_line).block(search_block);
    frame.render_widget(search_widget, chunks[0]);

    let id_header = if app.list_mode == ListMode::Spans {
        "Span"
    } else {
        "Trace"
    };
    let header = Row::new(vec!["Created", id_header, "Input", "Duration"]).style(
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    );

    let rows: Vec<Row<'_>> = app
        .traces
        .iter()
        .map(|trace| {
            let created = preview_string(trace.row.get("created"), 24);
            let trace_id = if app.list_mode == ListMode::Spans {
                preview_string(trace.row.get("span_id"), 24)
            } else {
                preview_string(Some(&Value::String(trace.root_span_id.clone())), 24)
            };
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
                if app.list_mode == ListMode::Spans {
                    "Recent Spans"
                } else {
                    "Recent Traces"
                }
            } else if app.list_mode == ListMode::Spans {
                "Recent Spans [filtered]"
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
                        "Enter opens selected trace  / search  Ctrl+k open URL  r refresh"
                            .to_string()
                    }
                } else {
                    "Enter opens selected trace  / search  Ctrl+k open URL  r refresh".to_string()
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

    let visible_span_indices = app.visible_span_indices();
    let items: Vec<ListItem<'_>> = visible_span_indices
        .iter()
        .filter_map(|idx| app.spans.get(*idx).map(|span| (*idx, span)))
        .map(|(idx, span)| {
            let indent = "  ".repeat(span.depth.min(16));
            let marker = if app.span_has_children_at_index(idx) {
                if app.is_span_collapsed_at_index(idx) {
                    "▸"
                } else {
                    "▾"
                }
            } else {
                "•"
            };
            ListItem::new(format!("{indent}{marker} {}", span.label))
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
                .title_bottom(
                    "Left/Right switch pane  Space collapse/expand  Tab raw/messages  t thread/span  Backspace/Esc back",
                ),
        )
        .highlight_style(
            Style::default()
                .bg(Color::Rgb(42, 47, 56))
                .add_modifier(Modifier::BOLD),
        );

    let mut list_state = ListState::default();
    if let Some(selected_visible_pos) = app.selected_visible_span_position(&visible_span_indices) {
        list_state.select(Some(selected_visible_pos));
    }
    frame.render_stateful_widget(list, chunks[0], &mut list_state);

    let detail_title = match app.detail_view {
        DetailView::Span => match app.span_detail_tab {
            SpanDetailTab::Messages => "Span Detail [messages]",
            SpanDetailTab::Raw => "Span Detail [raw]",
        },
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
            let detail_chunks = Layout::default()
                .direction(Direction::Vertical)
                .constraints([Constraint::Length(5), Constraint::Min(0)])
                .split(chunks[1]);

            let summary_text = app
                .current_span()
                .map(|span| {
                    let full_row = app.full_span_cache.get(&span.id);
                    render_span_summary(span, full_row.unwrap_or(&span.row))
                })
                .unwrap_or_else(|| "No span selected".to_string());

            let summary = Paragraph::new(summary_text)
                .block(
                    Block::default()
                        .title("Summary")
                        .borders(Borders::ALL)
                        .border_type(BorderType::Rounded),
                )
                .wrap(Wrap { trim: false });
            frame.render_widget(summary, detail_chunks[0]);

            if app.span_detail_shows_messages() {
                if app.message_viewer_open {
                    if let Some(message) = selected_detail_message(app) {
                        draw_message_viewer(frame, detail_chunks[1], app, message, detail_block);
                    } else {
                        let detail = Paragraph::new("No message selected.")
                            .block(detail_block)
                            .wrap(Wrap { trim: false });
                        frame.render_widget(detail, detail_chunks[1]);
                    }
                } else if let Some(messages) = &app.span_detail_messages {
                    draw_span_messages_list(frame, detail_chunks[1], app, messages, detail_block);
                } else {
                    let detail = Paragraph::new(
                        "No parseable messages found. Press Tab for raw span input/output.",
                    )
                    .block(detail_block)
                    .scroll((app.detail_scroll, 0))
                    .wrap(Wrap { trim: false });
                    frame.render_widget(detail, detail_chunks[1]);
                }
            } else {
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
                frame.render_widget(detail, detail_chunks[1]);
            }
        }
        DetailView::Thread => {
            if app.thread_loading {
                let detail = Paragraph::new(render_thread_loading_state(app.thread_spinner_tick))
                    .block(detail_block)
                    .scroll((app.detail_scroll, 0))
                    .wrap(Wrap { trim: false });
                frame.render_widget(detail, chunks[1]);
            } else if app.message_viewer_open {
                if let Some(message) = selected_detail_message(app) {
                    draw_message_viewer(frame, chunks[1], app, message, detail_block);
                } else {
                    let detail = Paragraph::new("No message selected.")
                        .block(detail_block)
                        .wrap(Wrap { trim: false });
                    frame.render_widget(detail, chunks[1]);
                }
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

fn draw_opening_trace_view(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &TraceViewerApp,
) {
    let spinner = spinner_char(
        app.url_open_spinner_tick
            .wrapping_add(app.trace_spinner_tick)
            .wrapping_add(app.summary_spinner_tick),
    );

    let detail = if app.url_open_rx.is_some() {
        format!(
            "Opening trace {spinner}\n\nResolving URL reference: {}",
            if app.url_open_loading_input.is_empty() {
                "<url>"
            } else {
                app.url_open_loading_input.as_str()
            }
        )
    } else if let Some(root_span_id) = app.trace_loading_root_span_id.as_deref() {
        format!("Opening trace {spinner}\n\nLoading spans for root_span_id:\n{root_span_id}")
    } else if app.summary_load_rx.is_some() {
        "Opening trace...\n\nRefreshing project traces...".to_string()
    } else {
        "Opening trace...".to_string()
    };

    let panel = Paragraph::new(detail)
        .block(
            Block::default()
                .title("Trace Open")
                .borders(Borders::ALL)
                .border_type(BorderType::Rounded),
        )
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, area);
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

    let fallback_name =
        value_as_string(span_attributes.get("name")).unwrap_or_else(|| span_id.to_string());
    let kind = value_as_string(span_attributes.get("type")).unwrap_or_else(|| "span".to_string());
    let has_error = row
        .get("error")
        .map(|v| match v {
            Value::Null => false,
            Value::String(s) => !s.is_empty(),
            _ => true,
        })
        .unwrap_or(false);

    let model_name = if kind.eq_ignore_ascii_case("llm") {
        extract_model_name(row)
    } else {
        None
    };

    let mut details: Vec<String> = Vec::new();
    if let Some(duration_seconds) = extract_duration_seconds(row.get("metrics")) {
        details.push(format_compact_duration(duration_seconds));
    }
    if let Some(total_tokens) = extract_total_tokens(row) {
        details.push(format!("{} tok", format_u64_with_commas(total_tokens)));
    }

    let mut label = if kind.eq_ignore_ascii_case("llm") {
        model_name.unwrap_or(fallback_name)
    } else if kind == "span" {
        fallback_name
    } else {
        format!("{fallback_name} [{kind}]")
    };
    if !details.is_empty() {
        label.push_str(" | ");
        label.push_str(&details.join(" | "));
    }
    if has_error {
        label.push_str(" !");
    }
    label
}

fn span_purpose_is_scorer(row: &Map<String, Value>) -> bool {
    let span_attributes = value_as_object_owned(row.get("span_attributes")).unwrap_or_default();
    value_as_string(span_attributes.get("purpose"))
        .map(|purpose| purpose.eq_ignore_ascii_case("scorer"))
        .unwrap_or(false)
}

fn extract_duration_seconds(metrics: Option<&Value>) -> Option<f64> {
    let metrics_obj = value_as_object_owned(metrics)?;
    parse_f64ish(metrics_obj.get("duration"))
        .filter(|value| value.is_finite() && *value >= 0.0)
        .or_else(|| {
            let start = parse_f64ish(metrics_obj.get("start"))?;
            let end = parse_f64ish(metrics_obj.get("end"))?;
            let delta = end - start;
            (delta.is_finite() && delta >= 0.0).then_some(delta)
        })
}

fn extract_ttft_seconds(metrics: &Map<String, Value>) -> Option<f64> {
    let keys = [
        "time_to_first_token",
        "ttft",
        "time_to_first_token_seconds",
        "first_token_latency",
    ];
    keys.iter().find_map(|key| {
        parse_f64ish(metrics.get(*key)).and_then(|value| {
            if value.is_finite() && value >= 0.0 {
                Some(value)
            } else {
                None
            }
        })
    })
}

fn format_compact_duration(seconds: f64) -> String {
    if seconds < 10.0 {
        format!("{seconds:.2}s")
    } else if seconds < 100.0 {
        format!("{seconds:.1}s")
    } else {
        format!("{seconds:.0}s")
    }
}

fn extract_total_tokens(row: &Map<String, Value>) -> Option<u64> {
    let metrics = value_as_object_owned(row.get("metrics"))?;
    let total = parse_u64ish(metrics.get("total_tokens"))
        .or_else(|| parse_u64ish(metrics.get("tokens")))
        .or_else(|| {
            let prompt = parse_u64ish(metrics.get("prompt_tokens"))
                .or_else(|| parse_u64ish(metrics.get("input_tokens")));
            let completion = parse_u64ish(metrics.get("completion_tokens"))
                .or_else(|| parse_u64ish(metrics.get("output_tokens")));
            match (prompt, completion) {
                (Some(p), Some(c)) => Some(p.saturating_add(c)),
                _ => None,
            }
        })?;
    if total == 0 {
        None
    } else {
        Some(total)
    }
}

fn extract_model_name(row: &Map<String, Value>) -> Option<String> {
    let span_attributes = value_as_object_owned(row.get("span_attributes")).unwrap_or_default();

    let model = value_as_string(row.get("metadata.model"))
        .or_else(|| value_as_string(row.get("metadata_model")))
        .or_else(|| value_as_string(row.get("model")))
        .or_else(|| {
            let metadata = value_as_object_owned(row.get("metadata"))?;
            value_as_string(metadata.get("model"))
        })
        .or_else(|| {
            let metadata = value_as_object_owned(row.get("metadata"))?;
            value_as_string(metadata.get("model_name"))
        })
        .or_else(|| {
            let metadata = value_as_object_owned(row.get("metadata"))?;
            let model_obj = value_as_object_owned(metadata.get("model"))?;
            value_as_string(model_obj.get("name"))
                .or_else(|| value_as_string(model_obj.get("id")))
                .or_else(|| value_as_string(model_obj.get("model")))
        })
        .or_else(|| value_as_string(span_attributes.get("model")));

    model.and_then(|m| {
        let trimmed = m.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn parse_u64ish(value: Option<&Value>) -> Option<u64> {
    match value? {
        Value::Number(n) => n
            .as_u64()
            .or_else(|| n.as_i64().and_then(|v| u64::try_from(v).ok())),
        Value::String(s) => {
            let s = s.trim();
            if s.is_empty() {
                return None;
            }
            if let Ok(v) = s.parse::<u64>() {
                return Some(v);
            }
            s.parse::<f64>().ok().and_then(|v| {
                if v.is_finite() && v >= 0.0 {
                    Some(v.round() as u64)
                } else {
                    None
                }
            })
        }
        _ => None,
    }
}

fn parse_f64ish(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(n) => n.as_f64(),
        Value::String(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

fn format_u64_with_commas(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::new();
    let len = digits.len();
    for (idx, ch) in digits.chars().enumerate() {
        out.push(ch);
        let remaining = len.saturating_sub(idx + 1);
        if remaining > 0 && remaining % 3 == 0 {
            out.push(',');
        }
    }
    out
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

    append_message_aware_section(&mut out, "input", row.get("input"));
    append_message_aware_section(&mut out, "output", row.get("output"));
    append_section(&mut out, "expected", row.get("expected"));
    append_section(&mut out, "error", row.get("error"));
    append_section(&mut out, "tags", row.get("tags"));
    append_section(&mut out, "scores", row.get("scores"));
    append_section(&mut out, "metrics", row.get("metrics"));
    append_section(&mut out, "metadata", row.get("metadata"));
    append_section(&mut out, "span_attributes", row.get("span_attributes"));

    out
}

fn render_span_summary(span: &SpanListEntry, row: &Map<String, Value>) -> String {
    let span_attributes = value_as_object_owned(row.get("span_attributes")).unwrap_or_default();
    let name = value_as_string(span_attributes.get("name")).unwrap_or_else(|| span.span_id.clone());
    let kind = value_as_string(span_attributes.get("type")).unwrap_or_else(|| "span".to_string());
    let model = extract_model_name(row).unwrap_or_else(|| "-".to_string());

    let metrics = value_as_object_owned(row.get("metrics")).unwrap_or_default();
    let duration_seconds = extract_duration_seconds(row.get("metrics"));
    let ttft_seconds = extract_ttft_seconds(&metrics);
    let total_tokens = parse_u64ish(metrics.get("total_tokens"))
        .or_else(|| parse_u64ish(metrics.get("tokens")))
        .or_else(|| extract_total_tokens(row));
    let prompt_tokens = parse_u64ish(metrics.get("prompt_tokens"))
        .or_else(|| parse_u64ish(metrics.get("input_tokens")));
    let completion_tokens = parse_u64ish(metrics.get("completion_tokens"))
        .or_else(|| parse_u64ish(metrics.get("output_tokens")));
    let cached_tokens = parse_u64ish(metrics.get("prompt_cached_tokens"))
        .or_else(|| parse_u64ish(metrics.get("cache_creation_input_tokens")));
    let estimated_cost =
        parse_f64ish(metrics.get("estimated_cost")).or_else(|| parse_f64ish(metrics.get("cost")));

    let duration_part = match (duration_seconds, ttft_seconds) {
        (Some(duration), Some(ttft)) => {
            format!(
                "dur {} (ttft {})",
                format_compact_duration(duration),
                format_compact_duration(ttft)
            )
        }
        (Some(duration), None) => format!("dur {}", format_compact_duration(duration)),
        (None, Some(ttft)) => format!("ttft {}", format_compact_duration(ttft)),
        (None, None) => "dur -".to_string(),
    };

    let mut metrics_parts = vec![duration_part];
    if let Some(total) = total_tokens {
        metrics_parts.push(format!("tok {}", format_u64_with_commas(total)));
    }
    if let Some(prompt) = prompt_tokens {
        metrics_parts.push(format!("in {}", format_u64_with_commas(prompt)));
    }
    if let Some(completion) = completion_tokens {
        metrics_parts.push(format!("out {}", format_u64_with_commas(completion)));
    }
    if let Some(cached) = cached_tokens {
        metrics_parts.push(format!("cached {}", format_u64_with_commas(cached)));
    }
    if let Some(cost) = estimated_cost {
        metrics_parts.push(format!("cost ${cost:.4}"));
    }

    let params = extract_span_param_summary(row, 6);
    let params_text = if params.is_empty() {
        "params -".to_string()
    } else {
        format!("params {}", params.join(" "))
    };

    format!(
        "{name} [{kind}]  model {model}\n{}  |  {params_text}",
        metrics_parts.join("  |  "),
    )
}

fn extract_span_param_summary(row: &Map<String, Value>, max_items: usize) -> Vec<String> {
    let metadata = value_as_object_owned(row.get("metadata")).unwrap_or_default();
    let mut maps: Vec<Map<String, Value>> = vec![metadata.clone()];
    if let Some(params) = value_as_object_owned(metadata.get("params")) {
        maps.push(params);
    }
    if let Some(params) = value_as_object_owned(metadata.get("model_params")) {
        maps.push(params);
    }
    if let Some(params) = value_as_object_owned(metadata.get("llm_params")) {
        maps.push(params);
    }

    let preferred_keys = [
        ("temperature", "temp"),
        ("top_p", "top_p"),
        ("max_tokens", "max_tok"),
        ("frequency_penalty", "freq_pen"),
        ("presence_penalty", "pres_pen"),
        ("reasoning_effort", "effort"),
        ("tool_choice", "tool"),
        ("parallel_tool_calls", "ptools"),
    ];

    let mut out: Vec<String> = Vec::new();
    for (key, label) in preferred_keys {
        let value = maps
            .iter()
            .find_map(|map| map.get(key))
            .and_then(compact_scalar_value);
        if let Some(value) = value {
            out.push(format!("{label}={value}"));
            if out.len() >= max_items {
                break;
            }
        }
    }
    out
}

fn compact_scalar_value(value: &Value) -> Option<String> {
    let raw = match value {
        Value::String(s) => s.trim().to_string(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => return None,
    };
    if raw.is_empty() {
        None
    } else {
        Some(truncate_chars(&raw, 18))
    }
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

                        if item_type == "tool_call" || item_type == "tool-call" {
                            let name = value_as_string(obj.get("name"))
                                .or_else(|| value_as_string(obj.get("tool_name")))
                                .or_else(|| value_as_string(obj.get("toolName")))
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
                        .or_else(|| value_as_string(obj.get("toolName")))
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

fn sanitize_terminal_text(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '\n' | '\t' => out.push(ch),
            '\r' => out.push('\n'),
            '\x1b' => {}
            c if c.is_control() => out.push(' '),
            c => out.push(c),
        }
    }
    out
}

fn push_text_lines_limited(
    lines: &mut Vec<Line<'_>>,
    text: &str,
    indent: &str,
    max_line_chars: usize,
    max_lines: usize,
) -> (usize, bool) {
    if max_lines == 0 || max_line_chars == 0 {
        return (0, !text.is_empty());
    }

    let mut added = 0usize;
    let mut line_iter = text.lines().peekable();

    while let Some(line) = line_iter.next() {
        if added >= max_lines {
            return (added, true);
        }

        if line.is_empty() {
            lines.push(Line::from(Span::raw(indent.to_string())));
            added += 1;
            if added >= max_lines && line_iter.peek().is_some() {
                return (added, true);
            }
            continue;
        }

        let mut chunk = String::new();
        let mut chunk_chars = 0usize;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            chunk.push(ch);
            chunk_chars += 1;

            if chunk_chars >= max_line_chars {
                if added >= max_lines {
                    return (added, true);
                }
                lines.push(Line::from(Span::raw(format!("{indent}{chunk}"))));
                added += 1;
                chunk.clear();
                chunk_chars = 0;

                if added >= max_lines {
                    if chars.peek().is_some() || line_iter.peek().is_some() {
                        return (added, true);
                    }
                    break;
                }
            }
        }

        if !chunk.is_empty() {
            if added >= max_lines {
                return (added, true);
            }
            lines.push(Line::from(Span::raw(format!("{indent}{chunk}"))));
            added += 1;
            if added >= max_lines && line_iter.peek().is_some() {
                return (added, true);
            }
        }
    }

    (added, false)
}

#[derive(Clone, Copy)]
struct MessagesListText<'a> {
    empty_state_text: &'a str,
    controls_text: &'a str,
}

fn draw_thread_messages_list(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &TraceViewerApp,
    messages: &[Value],
    detail_block: Block<'_>,
) {
    draw_messages_list(
        frame,
        area,
        messages,
        detail_block,
        app.thread_selected_message,
        &app.thread_expanded,
        MessagesListText {
            empty_state_text: "No messages returned by preprocessor.",
            controls_text:
                "Up/Down: select message  Enter: full view  v: full view  Left: span tree  t: span/thread",
        },
    );
}

fn draw_span_messages_list(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &TraceViewerApp,
    messages: &[Value],
    detail_block: Block<'_>,
) {
    draw_messages_list(
        frame,
        area,
        messages,
        detail_block,
        app.span_detail_message_selected,
        &app.span_detail_message_expanded,
        MessagesListText {
            empty_state_text: "No parseable messages found in span input/output.",
            controls_text:
                "Up/Down: select message  Enter: full view  v: full view  Left: span tree  Tab: raw",
        },
    );
}

fn draw_message_viewer(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    app: &TraceViewerApp,
    message: &Value,
    detail_block: Block<'_>,
) {
    let text = render_full_message_text(message);
    let panel = Paragraph::new(text)
        .block(
            detail_block
                .title_bottom("Up/Down: scroll  PgUp/PgDn: page  gg/G: top/bottom  v/Esc: close"),
        )
        .scroll((app.message_viewer_scroll, 0))
        .wrap(Wrap { trim: false });
    frame.render_widget(panel, area);
}

fn render_full_message_text(message: &Value) -> String {
    let (role, section, content, tool_calls) = match message {
        Value::Object(obj) => (
            value_as_string(obj.get("role")).unwrap_or_else(|| "message".to_string()),
            value_as_string(obj.get("_section")),
            obj.get("content"),
            obj.get("tool_calls"),
        ),
        _ => ("message".to_string(), None, Some(message), None),
    };

    let mut out = String::new();
    out.push_str(&format!("Role: {}", role_display_name(&role)));
    if let Some(section) = section {
        out.push_str(&format!(" [{section}]"));
    }
    out.push_str("\n\nContent\n-------\n");

    let content_text = content
        .map(render_message_content)
        .map(|text| sanitize_terminal_text(&text))
        .unwrap_or_default();
    if content_text.trim().is_empty() {
        out.push_str("(empty)\n");
    } else {
        out.push_str(content_text.trim_end());
        out.push('\n');
    }

    if let Some(tool_calls) = tool_calls {
        let tool_text = sanitize_terminal_text(&render_tool_calls_inline(tool_calls));
        if !tool_text.trim().is_empty() {
            out.push_str("\nTool Calls\n----------\n");
            out.push_str(tool_text.trim_end());
            out.push('\n');
        }
    }

    out
}

fn message_inline_render_char_count(message: &Value) -> usize {
    let (_, _, content, tool_calls) = match message {
        Value::Object(obj) => (
            value_as_string(obj.get("role")).unwrap_or_else(|| "message".to_string()),
            value_as_string(obj.get("_section")),
            obj.get("content"),
            obj.get("tool_calls"),
        ),
        _ => ("message".to_string(), None, Some(message), None),
    };

    let content_len = content
        .map(render_message_content)
        .map(|text| sanitize_terminal_text(&text).chars().count())
        .unwrap_or(0);
    let tools_len = tool_calls
        .map(render_tool_calls_inline)
        .map(|text| sanitize_terminal_text(&text).chars().count())
        .unwrap_or(0);
    content_len.saturating_add(tools_len)
}

fn draw_messages_list(
    frame: &mut Frame<'_>,
    area: ratatui::layout::Rect,
    messages: &[Value],
    detail_block: Block<'_>,
    selected_message: usize,
    expanded_set: &HashSet<usize>,
    text: MessagesListText<'_>,
) {
    if messages.is_empty() {
        let detail = Paragraph::new(text.empty_state_text)
            .block(detail_block)
            .wrap(Wrap { trim: false });
        frame.render_widget(detail, area);
        return;
    }

    let items: Vec<ListItem<'_>> = messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let (role, section, content, tool_calls) = match message {
                Value::Object(obj) => (
                    value_as_string(obj.get("role")).unwrap_or_else(|| "message".to_string()),
                    value_as_string(obj.get("_section")),
                    obj.get("content"),
                    obj.get("tool_calls"),
                ),
                _ => ("message".to_string(), None, Some(message), None),
            };

            let expanded_requested = expanded_set.contains(&index);
            let too_large_for_inline = INLINE_EXPAND_ENABLED
                && message_inline_render_char_count(message) > MAX_INLINE_EXPAND_CHARS;
            let expanded = INLINE_EXPAND_ENABLED && expanded_requested && !too_large_for_inline;
            let arrow = if expanded { "▾" } else { "▸" };
            let role_display = role_display_name(&role);

            let mut header_spans = vec![
                Span::styled(
                    format!("{arrow}  #{:<3}", index + 1),
                    Style::default().fg(Color::DarkGray),
                ),
                Span::styled(
                    role_display.to_string(),
                    thread_role_style(&role).add_modifier(Modifier::BOLD),
                ),
            ];
            if let Some(section) = section {
                header_spans.push(Span::styled(
                    format!("  [{section}]"),
                    Style::default().fg(Color::DarkGray),
                ));
            }
            let mut lines: Vec<Line<'_>> = vec![Line::from(header_spans)];
            let mut remaining_lines = if expanded {
                MAX_EXPANDED_RENDER_LINES
            } else {
                MAX_PREVIEW_RENDER_LINES
            };
            remaining_lines = remaining_lines.saturating_sub(1);

            let content_text = content
                .map(render_message_content)
                .map(|text| sanitize_terminal_text(&text))
                .unwrap_or_default();
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

            let (content_lines_added, content_budget_truncated) = push_text_lines_limited(
                &mut lines,
                &shown_content,
                "    ",
                MAX_RENDER_LINE_CHARS,
                remaining_lines,
            );
            remaining_lines = remaining_lines.saturating_sub(content_lines_added);
            if content_truncated || content_budget_truncated {
                lines.push(Line::from(Span::styled(
                    "    ...",
                    Style::default().fg(Color::DarkGray),
                )));
                remaining_lines = remaining_lines.saturating_sub(1);
            }

            if let Some(calls) = tool_calls {
                let tool_calls_text = sanitize_terminal_text(&render_tool_calls_inline(calls));
                if !tool_calls_text.trim().is_empty() && remaining_lines > 0 {
                    lines.push(Line::from(Span::styled(
                        "    Tool calls:",
                        Style::default().fg(Color::DarkGray),
                    )));
                    remaining_lines = remaining_lines.saturating_sub(1);
                    let (shown_tools, tools_truncated) = if expanded {
                        (tool_calls_text, false)
                    } else {
                        truncate_for_preview(&tool_calls_text, 3, 140)
                    };
                    let tools_line_cap = if expanded {
                        MAX_EXPANDED_TOOL_RENDER_LINES
                    } else {
                        10
                    };
                    let allowed_tool_lines = remaining_lines.min(tools_line_cap);
                    let (tool_lines_added, tools_budget_truncated) = push_text_lines_limited(
                        &mut lines,
                        &shown_tools,
                        "      ",
                        MAX_RENDER_LINE_CHARS,
                        allowed_tool_lines,
                    );
                    remaining_lines = remaining_lines.saturating_sub(tool_lines_added);
                    if tools_truncated || tools_budget_truncated {
                        lines.push(Line::from(Span::styled(
                            "      ...",
                            Style::default().fg(Color::DarkGray),
                        )));
                        remaining_lines = remaining_lines.saturating_sub(1);
                    }
                }
            }

            if too_large_for_inline {
                lines.push(Line::from(Span::styled(
                    "    Large message: inline view is truncated for stability. Press v for full view.",
                    Style::default().fg(Color::DarkGray),
                )));
                remaining_lines = remaining_lines.saturating_sub(1);
            }

            if remaining_lines > 0 {
                if INLINE_EXPAND_ENABLED {
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
                } else {
                    lines.push(Line::from(Span::styled(
                        "    Enter or v for full view",
                        Style::default().fg(Color::DarkGray),
                    )));
                }
                remaining_lines = remaining_lines.saturating_sub(1);
            }

            if remaining_lines == 0 {
                lines.push(Line::from(Span::styled(
                    "    [truncated for display]",
                    Style::default().fg(Color::DarkGray),
                )));
            }
            lines.push(Line::from(""));

            ListItem::new(lines)
        })
        .collect();

    let list = List::new(items)
        .block(detail_block.title_bottom(text.controls_text))
        .highlight_style(
            Style::default()
                .bg(Color::DarkGray)
                .add_modifier(Modifier::BOLD),
        )
        .highlight_symbol("▌ ");

    let mut state = ListState::default();
    state.select(Some(selected_message.min(messages.len().saturating_sub(1))));
    frame.render_stateful_widget(list, area, &mut state);
}

fn span_detail_status(
    view: DetailView,
    pane_focus: DetailPaneFocus,
    span_has_message_list: bool,
) -> &'static str {
    match (view, pane_focus) {
        (DetailView::Span, DetailPaneFocus::Tree) => {
            "Span view (tree focus). Right: detail pane  Up/Down: select span  Space: collapse/expand  Tab: raw/messages  t: thread  Ctrl+k: open URL  Backspace/Esc: back"
        }
        (DetailView::Span, DetailPaneFocus::Detail) if span_has_message_list => {
            "Span view (detail focus). Left: tree pane  Up/Down: select message  Enter/v: full view  Tab: raw  t: thread  Ctrl+k: open URL  Backspace/Esc: back"
        }
        (DetailView::Span, DetailPaneFocus::Detail) => {
            "Span view (detail focus). Left: tree pane  Up/Down: scroll  Enter: load span  Tab: messages  t: thread  Ctrl+k: open URL  Backspace/Esc: back"
        }
        (DetailView::Thread, DetailPaneFocus::Tree) => {
            "Thread view (tree focus). Right: detail pane  Up/Down: select span  Enter: toggle/retry  t: span  Ctrl+k: open URL  Backspace/Esc: back"
        }
        (DetailView::Thread, DetailPaneFocus::Detail) => {
            "Thread view (detail focus). Left: tree pane  Up/Down: select message  Enter/v: full view  t: span  Ctrl+k: open URL  Backspace/Esc: back"
        }
    }
}

fn append_message_aware_section(out: &mut String, title: &str, value: Option<&Value>) {
    out.push('\n');
    out.push_str(&title.to_uppercase());
    out.push('\n');
    out.push_str(&"-".repeat(title.len().max(8)));
    out.push('\n');

    let Some(raw_value) = value else {
        out.push_str("-\n");
        return;
    };

    let parsed_value = normalize_value_for_display(raw_value);
    let messages = import_lingua_messages_from_section(raw_value);
    if messages.is_empty() {
        out.push_str(&format_pretty_value(Some(&parsed_value)));
        out.push('\n');
        return;
    }

    out.push_str(&format_lingua_messages_for_span(&messages));
    out.push('\n');
}

fn extract_span_detail_messages(row: &Map<String, Value>) -> Vec<Value> {
    let mut messages = Vec::new();
    messages.extend(import_lingua_messages_for_display(
        "input",
        row.get("input"),
    ));
    messages.extend(import_lingua_messages_for_display(
        "output",
        row.get("output"),
    ));
    messages
}

fn import_lingua_messages_for_display(section: &str, value: Option<&Value>) -> Vec<Value> {
    let Some(value) = value else {
        return Vec::new();
    };

    import_lingua_messages_from_section(value)
        .into_iter()
        .map(|message| {
            if let Value::Object(mut obj) = lingua_message_to_display_json(&message) {
                obj.insert("_section".to_string(), Value::String(section.to_string()));
                Value::Object(obj)
            } else {
                Value::Null
            }
        })
        .filter(|message| !message.is_null())
        .collect()
}

fn normalize_value_for_display(value: &Value) -> Value {
    match value {
        Value::String(s) => serde_json::from_str::<Value>(s).unwrap_or_else(|_| value.clone()),
        _ => value.clone(),
    }
}

fn import_lingua_messages_from_section(value: &Value) -> Vec<LinguaMessage> {
    let mut candidates = vec![value.clone()];
    if let Value::String(s) = value {
        if let Ok(parsed) = serde_json::from_str::<Value>(s) {
            candidates.push(parsed);
        }
    }

    for candidate in candidates {
        let Some(input) = std_json_to_lingua_value(&candidate) else {
            continue;
        };
        let span = LinguaSpan {
            input: Some(input),
            output: None,
            other: lingua::serde_json::Map::new(),
        };
        let messages = import_messages_from_spans(vec![span]);
        if !messages.is_empty() {
            return messages;
        }
    }

    Vec::new()
}

fn std_json_to_lingua_value(value: &Value) -> Option<lingua::serde_json::Value> {
    let encoded = serde_json::to_string(value).ok()?;
    lingua::serde_json::from_str(&encoded).ok()
}

fn lingua_json_to_std_value(value: &lingua::serde_json::Value) -> Value {
    let encoded = lingua::serde_json::to_string(value).unwrap_or_else(|_| "null".to_string());
    serde_json::from_str(&encoded).unwrap_or(Value::Null)
}

fn format_lingua_messages_for_span(messages: &[LinguaMessage]) -> String {
    let mut out = String::new();
    for (idx, message) in messages.iter().enumerate() {
        let display_message = lingua_message_to_display_json(message);
        let Value::Object(obj) = display_message else {
            continue;
        };
        let role = value_as_string(obj.get("role")).unwrap_or_else(|| "message".to_string());
        out.push_str(&format!("[{}] role: {}\n", idx + 1, role));

        let content_text = obj
            .get("content")
            .map(render_message_content)
            .unwrap_or_default()
            .trim()
            .to_string();
        if !content_text.is_empty() {
            out.push_str(&content_text);
            out.push('\n');
        } else if obj.get("tool_calls").is_none() {
            out.push_str("(empty)\n");
        }

        if let Some(tool_calls) = obj.get("tool_calls") {
            let tool_calls_text = render_tool_calls_inline(tool_calls);
            if !tool_calls_text.trim().is_empty() {
                out.push_str("[tool_calls]\n");
                out.push_str(tool_calls_text.trim_end());
                out.push('\n');
            }
        }

        if idx + 1 < messages.len() {
            out.push('\n');
        }
    }

    if out.trim().is_empty() {
        "-".to_string()
    } else {
        out
    }
}

fn lingua_message_to_display_json(message: &LinguaMessage) -> Value {
    let mut out = Map::new();

    match message {
        LinguaMessage::System { content } => {
            out.insert("role".to_string(), Value::String("system".to_string()));
            out.insert("content".to_string(), user_content_to_display_json(content));
        }
        LinguaMessage::User { content } => {
            out.insert("role".to_string(), Value::String("user".to_string()));
            out.insert("content".to_string(), user_content_to_display_json(content));
        }
        LinguaMessage::Assistant { content, .. } => {
            out.insert("role".to_string(), Value::String("assistant".to_string()));
            let (content_value, tool_calls) = assistant_content_to_display_json(content);
            out.insert("content".to_string(), content_value);
            if let Some(tool_calls) = tool_calls {
                out.insert("tool_calls".to_string(), tool_calls);
            }
        }
        LinguaMessage::Tool { content } => {
            out.insert("role".to_string(), Value::String("tool".to_string()));
            out.insert("content".to_string(), tool_content_to_display_json(content));
        }
    }

    Value::Object(out)
}

fn user_content_to_display_json(content: &UserContent) -> Value {
    match content {
        UserContent::String(s) => Value::String(s.clone()),
        UserContent::Array(parts) => Value::Array(
            parts
                .iter()
                .map(user_content_part_to_display_json)
                .collect::<Vec<_>>(),
        ),
    }
}

fn user_content_part_to_display_json(part: &UserContentPart) -> Value {
    match part {
        UserContentPart::Text(text) => {
            json!({"type": "text", "text": text.text.clone()})
        }
        UserContentPart::Image {
            image, media_type, ..
        } => {
            json!({
                "type": "image",
                "media_type": media_type,
                "image": lingua_json_to_std_value(image),
            })
        }
        UserContentPart::File {
            data,
            filename,
            media_type,
            ..
        } => {
            json!({
                "type": "file",
                "filename": filename,
                "media_type": media_type,
                "data": lingua_json_to_std_value(data),
            })
        }
    }
}

fn assistant_content_to_display_json(content: &AssistantContent) -> (Value, Option<Value>) {
    match content {
        AssistantContent::String(s) => (Value::String(s.clone()), None),
        AssistantContent::Array(parts) => {
            let mut content_parts = Vec::new();
            let mut tool_calls = Vec::new();

            for part in parts {
                match part {
                    AssistantContentPart::Text(text) => {
                        content_parts.push(json!({"type": "text", "text": text.text.clone()}));
                    }
                    AssistantContentPart::Reasoning { text, .. } => {
                        content_parts.push(json!({"type": "reasoning", "text": text.clone()}));
                    }
                    AssistantContentPart::ToolCall {
                        tool_call_id,
                        tool_name,
                        arguments,
                        ..
                    } => {
                        tool_calls.push(json!({
                            "tool_call_id": tool_call_id,
                            "name": tool_name,
                            "tool_name": tool_name,
                            "arguments": tool_call_arguments_to_std_value(arguments),
                        }));
                    }
                    AssistantContentPart::ToolResult {
                        tool_call_id,
                        tool_name,
                        output,
                        ..
                    } => {
                        content_parts.push(json!({
                            "type": "tool_result",
                            "tool_call_id": tool_call_id,
                            "tool_name": tool_name,
                            "output": lingua_json_to_std_value(output),
                        }));
                    }
                    AssistantContentPart::File {
                        data,
                        filename,
                        media_type,
                        ..
                    } => {
                        content_parts.push(json!({
                            "type": "file",
                            "filename": filename,
                            "media_type": media_type,
                            "data": lingua_json_to_std_value(data),
                        }));
                    }
                }
            }

            let content_value = if content_parts.is_empty() {
                Value::String(String::new())
            } else {
                Value::Array(content_parts)
            };
            let tool_calls_value = if tool_calls.is_empty() {
                None
            } else {
                Some(Value::Array(tool_calls))
            };
            (content_value, tool_calls_value)
        }
    }
}

fn tool_call_arguments_to_std_value(arguments: &ToolCallArguments) -> Value {
    match arguments {
        ToolCallArguments::Valid(map) => {
            lingua_json_to_std_value(&lingua::serde_json::Value::Object(map.clone()))
        }
        ToolCallArguments::Invalid(s) => Value::String(s.clone()),
    }
}

fn tool_content_to_display_json(content: &[ToolContentPart]) -> Value {
    let mut parts = Vec::new();
    for part in content {
        match part {
            ToolContentPart::ToolResult(result) => {
                parts.push(json!({
                    "type": "tool_result",
                    "tool_call_id": result.tool_call_id.clone(),
                    "tool_name": result.tool_name.clone(),
                    "output": lingua_json_to_std_value(&result.output),
                }));
            }
        }
    }
    Value::Array(parts)
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
    extract_duration_seconds(metrics)
        .map(|seconds| format!("{seconds:.3}s"))
        .unwrap_or_else(|| "-".to_string())
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

fn logs_hints(
    has_more: bool,
    object_ref: &str,
    next_cursor: Option<&str>,
    profile: Option<&str>,
) -> Vec<String> {
    let mut hints = vec![
        "Rows are truncated by preview_length; fetch a single span for full content.".to_string(),
        "Use --json to get machine-readable envelopes for agent workflows.".to_string(),
        "Write large responses to a file to preserve full output context.".to_string(),
    ];
    if has_more {
        if let Some(cursor) = next_cursor {
            hints.push(format!(
                "Next page: bt view logs{} --object-ref {object_ref} --cursor {cursor}",
                profile_flag_suffix(profile)
            ));
        } else {
            hints.push(format!(
                "Use --cursor <next_cursor> with --object-ref {object_ref} to fetch the next page."
            ));
        }
    }
    hints
}

fn trace_hints(
    has_more: bool,
    object_ref: &str,
    trace_id: &str,
    next_cursor: Option<&str>,
    profile: Option<&str>,
) -> Vec<String> {
    let mut hints = vec![
        format!(
            "Trace fetch returns truncated span rows; use `bt view span{} --object-ref {object_ref} --id <row-id>` for full single-span payloads.",
            profile_flag_suffix(profile)
        ),
        "Write output to a file for long traces.".to_string(),
    ];
    if has_more {
        if let Some(cursor) = next_cursor {
            hints.push(format!(
                "Next page: bt view trace{} --object-ref {object_ref} --trace-id {trace_id} --cursor {cursor}",
                profile_flag_suffix(profile)
            ));
        } else {
            hints.push(format!(
                "Use --cursor <next_cursor> with --object-ref {object_ref} and --trace-id {trace_id}."
            ));
        }
    }
    hints
}

fn list_mode_label(list_mode: ListMode) -> &'static str {
    match list_mode {
        ListMode::Summary => "summary",
        ListMode::Spans => "spans",
    }
}

fn print_logs_text(
    rows: &[TraceSummaryRow],
    list_mode: ListMode,
    object_ref: &str,
    profile: Option<&str>,
    limit: usize,
    preview_length: usize,
    next_cursor: Option<&str>,
) {
    println!(
        "bt view logs [{}]: returned {} row(s) (object_ref={}, limit={}, preview_length={})",
        list_mode_label(list_mode),
        rows.len(),
        object_ref,
        limit,
        preview_length
    );
    for (idx, row) in rows.iter().enumerate() {
        let created = preview_string(row.row.get("created"), 24);
        let span_id = value_as_string(row.row.get("span_id")).unwrap_or_else(|| "-".to_string());
        let input = preview_string(row.row.get("input"), 70);
        println!(
            "{:>3}. created={} root_span_id={} id={} span_id={} input={}",
            idx + 1,
            created,
            row.root_span_id,
            value_as_string(row.row.get("id")).unwrap_or_else(|| "-".to_string()),
            span_id,
            input
        );
    }
    println!("\nTruncated fields use preview_length={preview_length}.");
    if let Some(cursor) = next_cursor {
        println!("next_cursor: {cursor}");
        println!(
            "next: bt view logs{} --object-ref {object_ref} --cursor {cursor} --non-interactive",
            profile_flag_suffix(profile)
        );
    } else {
        println!("No additional rows.");
    }
}

fn print_trace_text(
    trace_id: &str,
    rows: &[Map<String, Value>],
    object_ref: &str,
    profile: Option<&str>,
    limit: usize,
    preview_length: usize,
    next_cursor: Option<&str>,
) {
    println!(
        "bt view trace: object_ref={} trace_id={} rows={} (limit={}, preview_length={})",
        object_ref,
        trace_id,
        rows.len(),
        limit,
        preview_length
    );
    for (idx, row) in rows.iter().enumerate() {
        let span_id = value_as_string(row.get("span_id")).unwrap_or_else(|| "-".to_string());
        let created = preview_string(row.get("created"), 24);
        let input = preview_string(row.get("input"), 70);
        println!(
            "{:>3}. created={} id={} span_id={} input={}",
            idx + 1,
            created,
            value_as_string(row.get("id")).unwrap_or_else(|| "-".to_string()),
            span_id,
            input
        );
    }
    println!(
        "\nTrace output is truncated. Use `bt view span{} --object-ref {} --id <row-id>` for full span data.",
        profile_flag_suffix(profile),
        object_ref
    );
    if let Some(cursor) = next_cursor {
        println!("next_cursor: {cursor}");
        println!(
            "next: bt view trace{} --object-ref {} --trace-id {} --cursor {} --non-interactive",
            profile_flag_suffix(profile),
            object_ref,
            trace_id,
            cursor
        );
    } else {
        println!("No additional rows.");
    }
}

fn print_span_text(item: Option<&Map<String, Value>>) {
    match item {
        Some(row) => {
            println!(
                "{}",
                serde_json::to_string_pretty(row)
                    .unwrap_or_else(|_| Value::Object(row.clone()).to_string())
            );
        }
        None => println!("No span found."),
    }
}

fn parse_duration_to_seconds(input: &str) -> Result<u64> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("duration cannot be empty");
    }
    if let Ok(seconds) = trimmed.parse::<u64>() {
        return Ok(seconds);
    }

    let (num_str, unit) = trimmed.split_at(trimmed.len().saturating_sub(1));
    let value: u64 = num_str
        .trim()
        .parse()
        .with_context(|| format!("invalid duration '{input}'"))?;
    let multiplier = match unit.to_ascii_lowercase().as_str() {
        "s" => 1,
        "m" => 60,
        "h" => 60 * 60,
        "d" => 60 * 60 * 24,
        _ => bail!("invalid duration '{input}'. expected suffix s/m/h/d"),
    };
    Ok(value.saturating_mul(multiplier))
}

fn build_base_filter_clause(
    since: Option<&str>,
    window: &str,
    extra_filter: Option<&str>,
) -> Result<String> {
    let time_clause = if let Some(ts) = since.map(str::trim).filter(|s| !s.is_empty()) {
        format!("created >= {}", btql_quote(ts))
    } else {
        let seconds = parse_duration_to_seconds(window)?;
        format!("created >= NOW() - INTERVAL {seconds} SECOND")
    };

    if let Some(extra) = extra_filter.map(str::trim).filter(|s| !s.is_empty()) {
        Ok(format!("({time_clause}) AND ({extra})"))
    } else {
        Ok(time_clause)
    }
}

fn parse_object_ref(value: &str) -> Result<ObjectRef> {
    let parts: Vec<&str> = value.splitn(2, ':').collect();
    if parts.len() != 2 {
        bail!(
            "invalid object ref '{value}'. expected format object_type:object_selector (for example: project_logs:<project_id>)"
        );
    }
    let object_type = parts[0].trim();
    let object_name = parts[1].trim();
    if !matches!(object_type, "project_logs" | "experiment" | "dataset") {
        bail!(
            "unsupported object type '{object_type}'. supported types: project_logs, experiment, dataset"
        );
    }
    if object_name.is_empty() {
        bail!("object selector cannot be empty in '{value}'");
    }
    Ok(ObjectRef {
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
    })
}

fn btql_source_expr(object_ref: &ObjectRef) -> Result<String> {
    let source = match object_ref.object_type.as_str() {
        "project_logs" => "project_logs",
        "experiment" => "experiment",
        "dataset" => "dataset",
        other => bail!(
            "unsupported object type '{other}'. supported types: project_logs, experiment, dataset"
        ),
    };
    Ok(format!("{source}({})", sql_quote(&object_ref.object_name)))
}

fn format_object_ref_arg(object_ref: &ObjectRef) -> String {
    format!("{}:{}", object_ref.object_type, object_ref.object_name)
}

fn profile_flag_suffix(profile: Option<&str>) -> String {
    match profile.filter(|p| !p.trim().is_empty()) {
        Some(profile) => format!(" --profile {}", btql_quote(profile)),
        None => String::new(),
    }
}

fn select_startup_url(
    long_url: Option<&str>,
    positional_url: Option<&str>,
) -> Result<Option<String>> {
    match (long_url, positional_url) {
        (Some(a), Some(b)) if a.trim() != b.trim() => {
            bail!("received both --url and positional URL with different values; pass only one")
        }
        (Some(a), _) => Ok(Some(a.trim().to_string())),
        (_, Some(b)) => Ok(Some(b.trim().to_string())),
        _ => Ok(None),
    }
}

fn parse_trace_url(input: &str) -> Result<ParsedTraceUrl> {
    let input = input.trim();
    if input.is_empty() {
        bail!("trace URL is empty");
    }

    let parsed_url = if let Ok(url) = Url::parse(input) {
        url
    } else if input.contains("://") {
        Url::parse(input).context("invalid trace URL")?
    } else {
        let with_scheme = if input.starts_with('/') {
            format!("https://www.braintrust.dev{input}")
        } else {
            format!("https://{input}")
        };
        Url::parse(&with_scheme).context("invalid trace URL")?
    };

    let mut parsed = ParsedTraceUrl {
        org: None,
        project: None,
        page: None,
        row_ref: None,
        span_id: None,
        trace_view_type: None,
    };

    if let Some(segments) = parsed_url.path_segments() {
        let parts: Vec<&str> = segments.filter(|part| !part.is_empty()).collect();
        if parts.len() >= 2 && parts[0] == "app" {
            parsed.org = Some(parts[1].to_string());
            if parts.len() >= 4 && parts[2] == "p" {
                parsed.project = Some(parts[3].to_string());
                if parts.len() >= 5 {
                    parsed.page = Some(parts[4].to_string());
                }
            }
        }
    }

    for (key, value) in parsed_url.query_pairs() {
        match key.as_ref() {
            "r" => {
                if !value.is_empty() {
                    parsed.row_ref = Some(value.to_string());
                }
            }
            "s" => {
                if !value.is_empty() {
                    parsed.span_id = Some(value.to_string());
                }
            }
            "tvt" => {
                if !value.is_empty() {
                    parsed.trace_view_type = Some(value.to_string());
                }
            }
            _ => {}
        }
    }

    if parsed.row_ref.is_none() && parsed.span_id.is_none() {
        bail!("trace URL must include query parameter `r` or `s`");
    }

    Ok(parsed)
}

fn detail_view_from_tvt(trace_view_type: Option<&str>) -> DetailView {
    if trace_view_type
        .map(|v| v.eq_ignore_ascii_case("thread"))
        .unwrap_or(false)
    {
        DetailView::Thread
    } else {
        DetailView::Span
    }
}

fn is_uuid_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }

    for (idx, b) in bytes.iter().enumerate() {
        let is_hyphen = matches!(idx, 8 | 13 | 18 | 23);
        if is_hyphen {
            if *b != b'-' {
                return false;
            }
        } else if !(*b as char).is_ascii_hexdigit() {
            return false;
        }
    }

    true
}

async fn resolve_trace_target_for_url(
    client: &ApiClient,
    current_project: &ProjectSelection,
    parsed: &ParsedTraceUrl,
    print_queries: bool,
) -> Result<ResolvedTraceTarget> {
    let project =
        resolve_project_target_for_url(client, current_project, parsed.project.as_deref()).await?;
    let project_id = project.id.as_str();

    let mut root_span_id: Option<String> = None;
    if let Some(row_ref) = parsed.row_ref.as_deref() {
        root_span_id = lookup_root_span_id_for_query(
            client,
            &build_url_lookup_by_root_span_id_query(project_id, row_ref),
            "url-open-root-span-id",
            print_queries,
        )
        .await?;

        if root_span_id.is_none() {
            root_span_id = lookup_root_span_id_for_query(
                client,
                &build_url_lookup_by_row_id_query(project_id, row_ref),
                "url-open-row-id",
                print_queries,
            )
            .await?;
        }
    }

    if root_span_id.is_none() {
        if let Some(span_id) = parsed.span_id.as_deref() {
            root_span_id = lookup_root_span_id_for_query(
                client,
                &build_url_lookup_by_span_id_query(project_id, span_id),
                "url-open-span-id",
                print_queries,
            )
            .await?;
        }
    }

    let root_span_id = root_span_id.with_context(|| {
        if let Some(row_ref) = parsed.row_ref.as_deref() {
            format!(
                "could not resolve trace from URL parameter r='{row_ref}' (tried root_span_id then row id lookup)"
            )
        } else if let Some(span_id) = parsed.span_id.as_deref() {
            format!("could not resolve trace from URL parameter s='{span_id}'")
        } else {
            "trace URL must include query parameter `r` or `s`".to_string()
        }
    })?;

    Ok(ResolvedTraceTarget {
        project,
        root_span_id,
        span_id: parsed.span_id.clone(),
        detail_view: detail_view_from_tvt(parsed.trace_view_type.as_deref()),
    })
}

async fn resolve_project_target_for_url(
    client: &ApiClient,
    current_project: &ProjectSelection,
    url_project: Option<&str>,
) -> Result<ProjectSelection> {
    let Some(url_project) = url_project else {
        return Ok(current_project.clone());
    };

    let matches_current = url_project == current_project.id
        || current_project
            .name
            .as_deref()
            .map(|name| name == url_project)
            .unwrap_or(false);
    if matches_current {
        return Ok(current_project.clone());
    }

    if is_uuid_like(url_project) {
        return Ok(ProjectSelection {
            id: url_project.to_string(),
            name: None,
        });
    }

    let project = get_project_by_name(client, url_project)
        .await?
        .with_context(|| format!("project '{url_project}' not found"))?;
    Ok(ProjectSelection {
        id: project.id,
        name: Some(project.name),
    })
}

async fn lookup_root_span_id_for_query(
    client: &ApiClient,
    query: &str,
    label: &str,
    print_queries: bool,
) -> Result<Option<String>> {
    maybe_print_query(print_queries, label, query);
    let response = execute_query(client, query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))?;

    Ok(response
        .data
        .into_iter()
        .next()
        .and_then(|row| {
            value_as_string(row.get("root_span_id")).or_else(|| value_as_string(row.get("span_id")))
        })
        .filter(|v| !v.is_empty()))
}

fn build_url_lookup_by_root_span_id_query(project_id: &str, root_span_id: &str) -> String {
    format!(
        "select: root_span_id, span_id | from: project_logs({}) spans | filter: root_span_id = {} | limit: 1",
        sql_quote(project_id),
        sql_quote(root_span_id),
    )
}

fn build_url_lookup_by_row_id_query(project_id: &str, row_id: &str) -> String {
    format!(
        "select: root_span_id, span_id, id | from: project_logs({}) spans | filter: id = {} | limit: 1",
        sql_quote(project_id),
        sql_quote(row_id),
    )
}

fn build_url_lookup_by_span_id_query(project_id: &str, span_id: &str) -> String {
    format!(
        "select: root_span_id, span_id, id | from: project_logs({}) spans | filter: span_id = {} | limit: 1",
        sql_quote(project_id),
        sql_quote(span_id),
    )
}

fn build_summary_query(
    source_expr: &str,
    list_mode: ListMode,
    preview_length: usize,
    limit: usize,
    cursor: Option<&str>,
    search_query: Option<&str>,
    base_filter: &str,
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    let filter_clause = build_summary_filter(base_filter, search_query);
    let mode = match list_mode {
        ListMode::Summary => "summary",
        ListMode::Spans => "spans",
    };
    format!(
        "select: * | from: {source_expr} {mode} | filter: {filter_clause} | preview_length: {preview_length} | sort: _pagination_key DESC | limit: {limit}{cursor_clause}",
    )
}

fn build_summary_filter(base_filter: &str, search_query: Option<&str>) -> String {
    let Some(search) = search_query.map(str::trim).filter(|s| !s.is_empty()) else {
        return base_filter.to_string();
    };

    let match_term = sql_quote(search);
    let match_clause = TRACE_TEXT_SEARCH_FIELDS
        .iter()
        .map(|field| format!("{field} MATCH {match_term}"))
        .collect::<Vec<_>>()
        .join(" OR ");

    format!("({base_filter}) AND ({match_clause})")
}

fn build_spans_query(
    project_id: &str,
    root_span_id: &str,
    preview_length: usize,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    format!(
        "select: id, span_id, root_span_id, _pagination_key, _xact_id, created, span_parents, span_attributes, metadata.model, error, scores, metrics | from: project_logs({}) spans | filter: root_span_id = {} | preview_length: {} | sort: _pagination_key ASC | limit: {}{}",
        sql_quote(project_id),
        sql_quote(root_span_id),
        preview_length,
        limit,
        cursor_clause,
    )
}

fn build_trace_spans_query(
    source_expr: &str,
    root_span_id: &str,
    preview_length: usize,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let cursor_clause = cursor
        .map(|c| format!(" | cursor: {}", btql_quote(c)))
        .unwrap_or_default();
    format!(
        "select: * | from: {source_expr} spans | filter: root_span_id = {} | preview_length: {} | sort: _pagination_key ASC | limit: {}{}",
        sql_quote(root_span_id),
        preview_length,
        limit,
        cursor_clause,
    )
}

fn build_full_span_query(project_id: &str, span_row_id: &str) -> String {
    let source_expr = format!("project_logs({})", sql_quote(project_id));
    build_full_span_query_by_id(&source_expr, span_row_id)
}

fn build_full_span_query_by_id(source_expr: &str, span_row_id: &str) -> String {
    format!(
        "select: * | from: {source_expr} spans | filter: id = {} | preview_length: -1 | limit: 1",
        sql_quote(span_row_id),
    )
}

fn build_full_span_query_by_span_id(source_expr: &str, span_id: &str) -> String {
    format!(
        "select: * | from: {source_expr} spans | filter: span_id = {} | preview_length: -1 | limit: 1",
        sql_quote(span_id),
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
        eprintln!("bt view [{label}] BTQL:\n{query}\n");
    }
}

fn maybe_print_invoke(enabled: bool, label: &str, request: &Value) {
    if enabled {
        let formatted =
            serde_json::to_string_pretty(request).unwrap_or_else(|_| compact_json(request));
        eprintln!("bt view [{label}] INVOKE /function/invoke:\n{formatted}\n");
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
    query_ctx: &LogsQueryContext,
    total_limit: usize,
    search_query: Option<&str>,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut cursor: Option<String> = None;

    while rows.len() < total_limit {
        let page_limit = (total_limit - rows.len()).min(MAX_BTQL_PAGE_LIMIT);
        let query = build_summary_query(
            &query_ctx.source_expr,
            query_ctx.list_mode,
            query_ctx.preview_length,
            page_limit,
            cursor.as_deref(),
            search_query,
            &query_ctx.base_filter,
        );
        maybe_print_query(query_ctx.print_queries, "summary", &query);

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

async fn fetch_logs_page(
    client: &ApiClient,
    query_ctx: &LogsQueryContext,
    limit: usize,
    cursor: Option<&str>,
    search_query: Option<&str>,
) -> Result<BtqlResponse> {
    let query = build_summary_query(
        &query_ctx.source_expr,
        query_ctx.list_mode,
        query_ctx.preview_length,
        limit,
        cursor,
        search_query,
        &query_ctx.base_filter,
    );
    maybe_print_query(query_ctx.print_queries, "logs", &query);
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
}

async fn fetch_trace_spans_page(
    client: &ApiClient,
    source_expr: &str,
    root_span_id: &str,
    preview_length: usize,
    limit: usize,
    cursor: Option<&str>,
    print_queries: bool,
) -> Result<BtqlResponse> {
    let query = build_trace_spans_query(source_expr, root_span_id, preview_length, limit, cursor);
    maybe_print_query(print_queries, "trace", &query);
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
}

async fn fetch_trace_span_rows_for_tree(
    client: &ApiClient,
    project_id: &str,
    root_span_id: &str,
    preview_length: usize,
    total_limit: usize,
    print_queries: bool,
) -> Result<Vec<Map<String, Value>>> {
    let mut rows: Vec<Map<String, Value>> = Vec::new();
    let mut cursor: Option<String> = None;

    while rows.len() < total_limit {
        let page_limit = (total_limit - rows.len()).min(MAX_BTQL_PAGE_LIMIT);
        let query = build_spans_query(
            project_id,
            root_span_id,
            preview_length,
            page_limit,
            cursor.as_deref(),
        );
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
    app.close_message_viewer();

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
                    if current_selected_row_id == row_id {
                        app.refresh_span_detail_messages(false);
                        if app.detail_view == DetailView::Span {
                            app.set_status(app.span_detail_status_text());
                        }
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
    app.close_message_viewer();

    let Some(root_span_id) = app.loaded_root_span_id.clone() else {
        app.set_error("No trace loaded");
        return Ok(());
    };

    if !force_refresh && (app.thread_messages.is_some() || app.thread_loading) {
        app.set_status(app.span_detail_status_text());
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
                app.set_status(app.span_detail_status_text());
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

#[cfg(test)]
mod tests {
    use super::*;
    use ratatui::backend::TestBackend;
    use ratatui::widgets::{Block, Borders};
    use ratatui::Terminal;
    use serde_json::json;

    fn draw_messages_once(messages: &[Value], expanded: bool) -> bool {
        let backend = TestBackend::new(140, 40);
        let mut terminal = Terminal::new(backend).expect("create terminal");
        let mut expanded_set = HashSet::new();
        if expanded {
            expanded_set.insert(0usize);
        }

        terminal
            .draw(|frame| {
                let area = frame.area();
                draw_messages_list(
                    frame,
                    area,
                    messages,
                    Block::default().title("Messages").borders(Borders::ALL),
                    0,
                    &expanded_set,
                    MessagesListText {
                        empty_state_text: "empty",
                        controls_text: "controls",
                    },
                );
            })
            .expect("draw list");

        terminal
            .backend()
            .buffer()
            .content()
            .iter()
            .any(|cell| !cell.symbol().trim().is_empty())
    }

    #[test]
    fn draw_messages_list_survives_huge_message_expansion_loop() {
        for size in [2_048usize, 16_384, 131_072] {
            let long_text = format!(
                "{}\n{}\n{}\u{1b}[2J{}",
                "A".repeat(size),
                "B".repeat(size),
                "C".repeat(size / 2),
                "D".repeat(size)
            );
            let large_tool_json = json!({
                "payload": format!("{}{}", "T".repeat(size), "U".repeat(size / 2))
            });
            let messages = vec![json!({
                "role": "system",
                "content": long_text,
                "tool_calls": [{
                    "name": "large_tool",
                    "arguments": large_tool_json
                }]
            })];

            for i in 0..50 {
                let expanded = i % 2 == 0;
                let rendered = draw_messages_once(&messages, expanded);
                assert!(
                    rendered,
                    "render produced empty buffer at size={size}, iter={i}"
                );
            }
        }
    }

    #[test]
    fn parse_duration_to_seconds_supports_units() {
        assert_eq!(parse_duration_to_seconds("90").expect("seconds"), 90);
        assert_eq!(parse_duration_to_seconds("15m").expect("minutes"), 900);
        assert_eq!(parse_duration_to_seconds("2h").expect("hours"), 7_200);
        assert_eq!(parse_duration_to_seconds("1d").expect("days"), 86_400);
    }

    #[test]
    fn build_base_filter_clause_uses_window_or_since() {
        let from_window = build_base_filter_clause(None, "1h", Some("metadata.model IS NOT NULL"))
            .expect("window filter");
        assert!(from_window.contains("INTERVAL 3600 SECOND"));
        assert!(from_window.contains("metadata.model IS NOT NULL"));

        let from_since = build_base_filter_clause(Some("2025-01-01T00:00:00Z"), "1h", None)
            .expect("since filter");
        assert!(from_since.contains("created >= \"2025-01-01T00:00:00Z\""));
    }

    #[test]
    fn build_summary_query_switches_summary_vs_spans_mode() {
        let summary_query = build_summary_query(
            "project_logs('p1')",
            ListMode::Summary,
            125,
            10,
            None,
            Some("hello"),
            "created >= NOW() - INTERVAL 3600 SECOND",
        );
        assert!(summary_query.contains("from: project_logs('p1') summary"));
        assert!(summary_query.contains("preview_length: 125"));

        let spans_query = build_summary_query(
            "project_logs('p1')",
            ListMode::Spans,
            50,
            5,
            Some("abc"),
            None,
            "created >= NOW() - INTERVAL 3600 SECOND",
        );
        assert!(spans_query.contains("from: project_logs('p1') spans"));
        assert!(spans_query.contains("cursor: \"abc\""));
        assert!(spans_query.contains("preview_length: 50"));
    }

    #[test]
    fn parse_object_ref_validates_type_and_selector() {
        let parsed = parse_object_ref("project_logs:123").expect("valid object ref");
        assert_eq!(parsed.object_type, "project_logs");
        assert_eq!(parsed.object_name, "123");

        let err = parse_object_ref("badformat").expect_err("should fail");
        assert!(err.to_string().contains("invalid object ref"));

        let err = parse_object_ref("foo:123").expect_err("should fail");
        assert!(err.to_string().contains("unsupported object type"));
    }

    #[test]
    fn trace_and_span_queries_follow_truncation_policy() {
        let trace_query = build_trace_spans_query("project_logs('p1')", "root-1", 125, 10, None);
        assert!(trace_query.contains("preview_length: 125"));
        assert!(trace_query.contains("from: project_logs('p1') spans"));
        assert!(trace_query.contains("filter: root_span_id = 'root-1'"));

        let full_by_row = build_full_span_query_by_id("project_logs('p1')", "row-1");
        assert!(full_by_row.contains("preview_length: -1"));
        assert!(full_by_row.contains("filter: id = 'row-1'"));

        let full_by_span_id = build_full_span_query_by_span_id("project_logs('p1')", "span-1");
        assert!(full_by_span_id.contains("preview_length: -1"));
        assert!(full_by_span_id.contains("filter: span_id = 'span-1'"));
    }
}
