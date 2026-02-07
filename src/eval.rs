use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Args;
use crossterm::queue;
use crossterm::style::{
    Attribute, Color as CtColor, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    Stylize,
};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::Deserialize;
use strip_ansi_escapes::strip;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use unicode_width::UnicodeWidthStr;

use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, Table};
use ratatui::Terminal;

use crate::args::BaseArgs;

const MAX_NAME_LENGTH: usize = 40;

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum EvalLanguage {
    JavaScript,
    Python,
}

#[derive(Debug, Clone, Args)]
pub struct EvalArgs {
    /// One or more eval files to execute (e.g. foo.eval.ts)
    #[arg(required = true, value_name = "FILE")]
    pub files: Vec<String>,

    /// Eval runner binary (e.g. tsx, bun, ts-node, python). Defaults to tsx for JS files.
    #[arg(long, short = 'r', env = "BT_EVAL_JS_RUNNER", value_name = "RUNNER")]
    pub runner: Option<String>,

    /// Run evals locally (do not send logs to Braintrust).
    #[arg(
        long,
        alias = "no-send-logs",
        env = "BT_EVAL_LOCAL",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub no_send_logs: bool,
}

pub async fn run(base: BaseArgs, args: EvalArgs) -> Result<()> {
    run_eval_files(
        &base,
        args.runner.clone(),
        args.files.clone(),
        args.no_send_logs,
    )
    .await
}

async fn run_eval_files(
    base: &BaseArgs,
    runner_override: Option<String>,
    files: Vec<String>,
    no_send_logs: bool,
) -> Result<()> {
    let language = detect_eval_language(&files)?;
    let js_runner = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("eval-runner.ts");
    let py_runner = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("eval-runner.py");

    let socket_path = build_sse_socket_path()?;
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).context("failed to bind SSE unix socket")?;
    let (tx, mut rx) = mpsc::unbounded_channel();

    let tx_sse = tx.clone();
    let sse_task = tokio::spawn(async move {
        match listener.accept().await {
            Ok((stream, _)) => {
                if let Err(err) = read_sse_stream(stream, tx_sse.clone()).await {
                    let _ = tx_sse.send(EvalEvent::Error {
                        message: format!("SSE stream error: {err}"),
                        stack: None,
                    });
                }
            }
            Err(err) => {
                let _ = tx_sse.send(EvalEvent::Error {
                    message: format!("Failed to accept SSE connection: {err}"),
                    stack: None,
                });
            }
        };
    });

    let mut cmd = match language {
        EvalLanguage::Python => build_python_command(runner_override, &py_runner, &files)?,
        EvalLanguage::JavaScript => build_js_command(runner_override, &js_runner, &files),
    };

    cmd.envs(build_env(base));
    if no_send_logs {
        cmd.env("BT_EVAL_NO_SEND_LOGS", "1");
        cmd.env("BT_EVAL_LOCAL", "1");
    }
    cmd.env(
        "BT_EVAL_SSE_SOCK",
        socket_path.to_string_lossy().to_string(),
    );
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to start eval runner")?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    if let Some(stdout) = stdout {
        let tx_stdout = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = forward_stream(stdout, "stdout", tx_stdout).await {
                eprintln!("Failed to read eval stdout: {err}");
            }
        });
    }

    if let Some(stderr) = stderr {
        let tx_stderr = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = forward_stream(stderr, "stderr", tx_stderr).await {
                eprintln!("Failed to read eval stderr: {err}");
            }
        });
    }

    let mut ui = EvalUi::new();
    let mut status = None;

    drop(tx);

    loop {
        tokio::select! {
            event = rx.recv() => {
                match event {
                    Some(event) => ui.handle(event),
                    None => {
                        if status.is_none() {
                            status = Some(child.wait().await.context("eval runner process failed")?);
                            sse_task.abort();
                        }
                        break;
                    }
                }
            }
            exit_status = child.wait(), if status.is_none() => {
                status = Some(exit_status.context("eval runner process failed")?);
                sse_task.abort();
            }
        }

        if status.is_some() && rx.is_closed() {
            break;
        }
    }

    let _ = sse_task.await;

    ui.finish();

    if let Some(status) = status {
        if !status.success() {
            anyhow::bail!("eval runner exited with status {status}");
        }
    }
    let _ = std::fs::remove_file(&socket_path);

    Ok(())
}

fn build_env(base: &BaseArgs) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    if let Some(api_key) = base.api_key.as_ref() {
        envs.push(("BRAINTRUST_API_KEY".to_string(), api_key.clone()));
    }
    if let Some(api_url) = base.api_url.as_ref() {
        envs.push(("BRAINTRUST_API_URL".to_string(), api_url.clone()));
    }
    if let Some(project) = base.project.as_ref() {
        envs.push(("BRAINTRUST_DEFAULT_PROJECT".to_string(), project.clone()));
    }
    envs
}

fn detect_eval_language(files: &[String]) -> Result<EvalLanguage> {
    let mut detected: Option<EvalLanguage> = None;
    for file in files {
        let ext = PathBuf::from(file)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let current = match ext.as_str() {
            "py" => EvalLanguage::Python,
            "ts" | "tsx" | "js" | "mjs" | "cjs" => EvalLanguage::JavaScript,
            _ => {
                anyhow::bail!("Unsupported eval file extension: {ext}");
            }
        };
        if let Some(existing) = detected {
            if existing != current {
                anyhow::bail!(
                    "Mixed eval file types are not supported yet (found {:?} and {:?}).",
                    existing,
                    current
                );
            }
        } else {
            detected = Some(current);
        }
    }

    detected.ok_or_else(|| anyhow::anyhow!("No eval files provided"))
}

fn build_js_command(
    runner_override: Option<String>,
    runner: &PathBuf,
    files: &[String],
) -> Command {
    let runner_override = runner_override
        .or_else(|| std::env::var("BT_EVAL_JS_RUNNER").ok())
        .or_else(|| std::env::var("BT_EVAL_TSX").ok());

    let command = if let Some(explicit) = runner_override.as_deref() {
        let mut command = Command::new(explicit);
        command.arg(runner).args(files);
        command
    } else if let Some(tsx_path) = find_tsx_binary() {
        let mut command = Command::new(tsx_path);
        command.arg(runner).args(files);
        command
    } else {
        let mut command = Command::new("npx");
        command.arg("--yes").arg("tsx").arg(runner).args(files);
        command
    };

    command
}

fn build_python_command(
    runner_override: Option<String>,
    runner: &PathBuf,
    files: &[String],
) -> Result<Command> {
    let runner_override = runner_override
        .or_else(|| std::env::var("BT_EVAL_PYTHON_RUNNER").ok())
        .or_else(|| std::env::var("BT_EVAL_PYTHON").ok());

    let command = if let Some(explicit) = runner_override {
        let mut command = Command::new(explicit);
        command.arg(runner).args(files);
        command
    } else if let Some(python) = find_python_binary() {
        let mut command = Command::new(python);
        command.arg(runner).args(files);
        command
    } else {
        anyhow::bail!(
            "No Python interpreter found in PATH. Please install python or pass --runner."
        );
    };

    Ok(command)
}

fn find_tsx_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("BT_EVAL_TSX") {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join("tsx");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    None
}

fn find_python_binary() -> Option<PathBuf> {
    let candidates = ["python3", "python"];
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for candidate in candidates {
            let path = dir.join(candidate);
            if path.is_file() {
                return Some(path);
            }
        }
    }
    None
}

fn build_sse_socket_path() -> Result<PathBuf> {
    let pid = std::process::id();
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system time")?
        .as_millis();
    Ok(std::env::temp_dir().join(format!("bt-eval-{pid}-{now}.sock")))
}

#[derive(Debug)]
enum EvalEvent {
    Start(ExperimentSummary),
    Summary(ExperimentSummary),
    Progress(SseProgressEventData),
    Done,
    Error {
        message: String,
        stack: Option<String>,
    },
    Console {
        _stream: String,
        message: String,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ExperimentSummary {
    project_name: String,
    experiment_name: String,
    project_id: Option<String>,
    experiment_id: Option<String>,
    project_url: Option<String>,
    experiment_url: Option<String>,
    comparison_experiment_name: Option<String>,
    scores: HashMap<String, ScoreSummary>,
    metrics: Option<HashMap<String, MetricSummary>>,
}

#[derive(Debug, Deserialize)]
struct ScoreSummary {
    name: String,
    score: f64,
    diff: Option<f64>,
    improvements: i64,
    regressions: i64,
}

#[derive(Debug, Deserialize)]
struct EvalErrorPayload {
    message: String,
    stack: Option<String>,
}

#[derive(Debug, Deserialize)]
struct MetricSummary {
    name: String,
    metric: f64,
    unit: String,
    diff: Option<f64>,
    improvements: i64,
    regressions: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct SseProgressEventData {
    id: String,
    object_type: String,
    origin: Option<serde_json::Value>,
    format: String,
    output_type: String,
    name: String,
    event: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct EvalProgressData {
    #[serde(rename = "type")]
    kind_type: String,
    kind: String,
    total: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SseConsoleEventData {
    stream: String,
    message: String,
}

async fn forward_stream<T>(
    stream: T,
    name: &'static str,
    tx: mpsc::UnboundedSender<EvalEvent>,
) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    while let Some(line) = lines.next_line().await? {
        let _ = tx.send(EvalEvent::Console {
            _stream: name.to_string(),
            message: line,
        });
    }
    Ok(())
}

async fn read_sse_stream<T>(stream: T, tx: mpsc::UnboundedSender<EvalEvent>) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    let mut event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            if event.is_some() || !data_lines.is_empty() {
                let data = data_lines.join("\n");
                handle_sse_event(event.take(), data, &tx);
                data_lines.clear();
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        }
    }

    if event.is_some() || !data_lines.is_empty() {
        let data = data_lines.join("\n");
        handle_sse_event(event.take(), data, &tx);
    }

    Ok(())
}

fn handle_sse_event(event: Option<String>, data: String, tx: &mpsc::UnboundedSender<EvalEvent>) {
    let event_name = event.unwrap_or_default();
    match event_name.as_str() {
        "start" => {
            if let Ok(summary) = serde_json::from_str::<ExperimentSummary>(&data) {
                let _ = tx.send(EvalEvent::Start(summary));
            }
        }
        "summary" => {
            if let Ok(summary) = serde_json::from_str::<ExperimentSummary>(&data) {
                let _ = tx.send(EvalEvent::Summary(summary));
            }
        }
        "progress" => {
            if let Ok(progress) = serde_json::from_str::<SseProgressEventData>(&data) {
                let _ = tx.send(EvalEvent::Progress(progress));
            }
        }
        "console" => {
            if let Ok(console) = serde_json::from_str::<SseConsoleEventData>(&data) {
                let _ = tx.send(EvalEvent::Console {
                    _stream: console.stream,
                    message: console.message,
                });
            }
        }
        "error" => {
            if let Ok(payload) = serde_json::from_str::<EvalErrorPayload>(&data) {
                let _ = tx.send(EvalEvent::Error {
                    message: payload.message,
                    stack: payload.stack,
                });
            } else {
                let _ = tx.send(EvalEvent::Error {
                    message: data,
                    stack: None,
                });
            }
        }
        "done" => {
            let _ = tx.send(EvalEvent::Done);
        }
        _ => {}
    }
}

struct EvalUi {
    progress: MultiProgress,
    bars: HashMap<String, ProgressBar>,
    bar_style: ProgressStyle,
    spinner_style: ProgressStyle,
}

impl EvalUi {
    fn new() -> Self {
        let progress = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(10));
        let bar_style =
            ProgressStyle::with_template("{bar:10.blue} {msg} {percent}% {pos}/{len} {eta}")
                .unwrap();
        let spinner_style = ProgressStyle::with_template("{spinner} {msg}").unwrap();
        Self {
            progress,
            bars: HashMap::new(),
            bar_style,
            spinner_style,
        }
    }

    fn finish(&mut self) {
        for (_, bar) in self.bars.drain() {
            bar.finish_and_clear();
        }
    }

    fn handle(&mut self, event: EvalEvent) {
        match event {
            EvalEvent::Start(summary) => {
                let line = format_start_line(&summary);
                let _ = self.progress.println(line);
            }
            EvalEvent::Summary(summary) => {
                let rendered = format_experiment_summary(&summary);
                for line in rendered.lines() {
                    let _ = self.progress.println(line);
                }
            }
            EvalEvent::Progress(progress) => {
                self.handle_progress(progress);
            }
            EvalEvent::Console { message, .. } => {
                let _ = self.progress.println(message);
            }
            EvalEvent::Error { message, stack } => {
                let show_hint = message.contains("Please specify an api key");
                let line = message.as_str().red().to_string();
                let _ = self.progress.println(line);
                if let Some(stack) = stack {
                    for line in stack.lines() {
                        let _ = self.progress.println(line.dark_grey().to_string());
                    }
                }
                if show_hint {
                    let hint = "Hint: pass --api-key or set BRAINTRUST_API_KEY, or use --no-send-logs for local evals.";
                    let _ = self.progress.println(hint.dark_grey().to_string());
                }
            }
            EvalEvent::Done => {
                self.finish();
            }
        }
    }

    fn handle_progress(&mut self, progress: SseProgressEventData) {
        let payload = match serde_json::from_str::<EvalProgressData>(&progress.data) {
            Ok(payload) if payload.kind_type == "eval_progress" => payload,
            _ => return,
        };

        match payload.kind.as_str() {
            "start" => {
                let bar = if let Some(total) = payload.total {
                    if total > 0 {
                        let bar = self.progress.add(ProgressBar::new(total));
                        bar.set_style(self.bar_style.clone());
                        bar
                    } else {
                        let bar = self.progress.add(ProgressBar::new_spinner());
                        bar.set_style(self.spinner_style.clone());
                        bar
                    }
                } else {
                    let bar = self.progress.add(ProgressBar::new_spinner());
                    bar.set_style(self.spinner_style.clone());
                    bar
                };
                bar.set_message(fit_name_to_spaces(&progress.name, MAX_NAME_LENGTH));
                self.bars.insert(progress.name.clone(), bar);
            }
            "increment" => {
                if let Some(bar) = self.bars.get(&progress.name) {
                    bar.inc(1);
                    bar.set_message(fit_name_to_spaces(&progress.name, MAX_NAME_LENGTH));
                }
            }
            "set_total" => {
                if let Some(bar) = self.bars.get(&progress.name) {
                    if let Some(total) = payload.total {
                        bar.set_length(total);
                        bar.set_style(self.bar_style.clone());
                    }
                }
            }
            "stop" => {
                if let Some(bar) = self.bars.remove(&progress.name) {
                    bar.finish_and_clear();
                }
            }
            _ => {}
        }
    }
}

fn fit_name_to_spaces(name: &str, length: usize) -> String {
    let mut padded = name.to_string();
    if padded.len() < length {
        padded.push_str(&" ".repeat(length - padded.len()));
        return padded;
    }
    if padded.len() <= length {
        return padded;
    }
    if length <= 3 {
        return padded.chars().take(length).collect();
    }
    let truncated: String = padded.chars().take(length - 3).collect();
    format!("{truncated}...")
}

fn format_start_line(summary: &ExperimentSummary) -> String {
    let arrow = "▶".cyan();
    let name = summary.experiment_name.as_str().bold();
    let link = summary.experiment_url.as_deref().unwrap_or("locally");
    format!("{arrow} Experiment {name} is running at {link}")
}

fn format_experiment_summary(summary: &ExperimentSummary) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(comparison) = summary.comparison_experiment_name.as_deref() {
        let line = format!(
            "{baseline} {baseline_tag} ← {comparison_name} {comparison_tag}",
            baseline = comparison,
            baseline_tag = "(baseline)".dark_grey(),
            comparison_name = summary.experiment_name,
            comparison_tag = "(comparison)".dark_grey(),
        );
        parts.push(line);
    }

    let has_scores = !summary.scores.is_empty();
    let has_metrics = summary
        .metrics
        .as_ref()
        .map(|metrics| !metrics.is_empty())
        .unwrap_or(false);

    if has_scores || has_metrics {
        let has_comparison = summary.comparison_experiment_name.is_some();
        let mut rows: Vec<Vec<Line>> = Vec::new();

        let header = if has_comparison {
            Some(vec![
                header_line("Name"),
                header_line("Value"),
                header_line("Change"),
                header_line("Improvements"),
                header_line("Regressions"),
            ])
        } else {
            None
        };

        let mut score_values: Vec<_> = summary.scores.values().collect();
        score_values.sort_by(|a, b| a.name.cmp(&b.name));
        for score in score_values {
            let score_percent =
                Line::from(format!("{:.2}%", score.score * 100.0)).alignment(Alignment::Right);
            let diff = format_diff_line(score.diff);
            let improvements = format_improvements_line(score.improvements);
            let regressions = format_regressions_line(score.regressions);
            let name = truncate_plain(&score.name, MAX_NAME_LENGTH);
            let name = Line::from(vec![
                Span::styled("◯", Style::default().fg(Color::Blue)),
                Span::raw(" "),
                Span::raw(name),
            ]);
            if has_comparison {
                rows.push(vec![name, score_percent, diff, improvements, regressions]);
            } else {
                rows.push(vec![name, score_percent]);
            }
        }

        if let Some(metrics) = &summary.metrics {
            let mut metric_values: Vec<_> = metrics.values().collect();
            metric_values.sort_by(|a, b| a.name.cmp(&b.name));
            for metric in metric_values {
                let formatted_value = Line::from(format_metric_value(metric.metric, &metric.unit))
                    .alignment(Alignment::Right);
                let diff = format_diff_line(metric.diff);
                let improvements = format_improvements_line(metric.improvements);
                let regressions = format_regressions_line(metric.regressions);
                let name = truncate_plain(&metric.name, MAX_NAME_LENGTH);
                let name = Line::from(vec![
                    Span::styled("◯", Style::default().fg(Color::Magenta)),
                    Span::raw(" "),
                    Span::raw(name),
                ]);
                if has_comparison {
                    rows.push(vec![name, formatted_value, diff, improvements, regressions]);
                } else {
                    rows.push(vec![name, formatted_value]);
                }
            }
        }

        parts.push(render_table_ratatui(header, rows, has_comparison));
    }

    if let Some(url) = &summary.experiment_url {
        parts.push(format!("See results at {url}"));
    }

    let content = parts.join("\n\n");
    box_with_title("Experiment summary", &content)
}

fn format_diff_line(diff: Option<f64>) -> Line<'static> {
    match diff {
        Some(value) => {
            let sign = if value > 0.0 { "+" } else { "" };
            let percent = format!("{sign}{:.2}%", value * 100.0);
            let style = if value > 0.0 {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };
            Line::from(Span::styled(percent, style)).alignment(Alignment::Right)
        }
        None => Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right),
    }
}

fn format_improvements_line(value: i64) -> Line<'static> {
    if value > 0 {
        Line::from(Span::styled(
            value.to_string(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::DIM),
        ))
        .alignment(Alignment::Right)
    } else {
        Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right)
    }
}

fn format_regressions_line(value: i64) -> Line<'static> {
    if value > 0 {
        Line::from(Span::styled(
            value.to_string(),
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM),
        ))
        .alignment(Alignment::Right)
    } else {
        Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right)
    }
}

fn format_metric_value(metric: f64, unit: &str) -> String {
    let formatted = if metric.fract() == 0.0 {
        format!("{:.0}", metric)
    } else {
        format!("{:.2}", metric)
    };
    if unit == "$" {
        format!("{unit}{formatted}")
    } else {
        format!("{formatted}{unit}")
    }
}

fn render_table_ratatui(
    header: Option<Vec<Line<'static>>>,
    rows: Vec<Vec<Line<'static>>>,
    has_comparison: bool,
) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let columns = if has_comparison { 5 } else { 2 };
    let mut widths = vec![0usize; columns];

    if let Some(header_row) = &header {
        for (idx, line) in header_row.iter().enumerate().take(columns) {
            widths[idx] = widths[idx].max(line.width());
        }
    }

    for row in &rows {
        for (idx, line) in row.iter().enumerate().take(columns) {
            widths[idx] = widths[idx].max(line.width());
        }
    }

    let column_spacing = 2;
    let total_width = widths.iter().sum::<usize>() + column_spacing * (columns - 1);
    let mut height = rows.len();
    if header.is_some() {
        height += 1;
    }
    let backend = TestBackend::new(total_width as u16, height as u16);
    let mut terminal = Terminal::new(backend).expect("failed to create table backend");

    let table_rows = rows.into_iter().map(|row| {
        let cells = row.into_iter().map(Cell::new).collect::<Vec<_>>();
        Row::new(cells)
    });

    let mut table = Table::new(
        table_rows,
        widths.iter().map(|w| Constraint::Length(*w as u16)),
    )
    .column_spacing(column_spacing as u16);

    if let Some(header_row) = header {
        let header_cells = header_row.into_iter().map(Cell::new).collect::<Vec<_>>();
        table = table.header(Row::new(header_cells));
    }

    terminal
        .draw(|frame| {
            let area = frame.area();
            frame.render_widget(table, area);
        })
        .expect("failed to render table");

    let buffer = terminal.backend().buffer();
    buffer_to_ansi_lines(buffer).join("\n")
}

fn header_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(
        text.to_string(),
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    ))
}

fn truncate_plain(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        return text.to_string();
    }
    if max_len <= 3 {
        return text.chars().take(max_len).collect();
    }
    let truncated: String = text.chars().take(max_len - 3).collect();
    format!("{truncated}...")
}

fn box_with_title(title: &str, content: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let content_width = lines
        .iter()
        .map(|line| visible_width(line))
        .max()
        .unwrap_or(0);
    let padding = 1;
    let inner_width = content_width + padding * 2;

    let title_plain = format!(" {title} ");
    let title_width = visible_width(&title_plain);
    let mut top = String::from("╭");
    top.push_str(&title_plain.dark_grey().to_string());
    if inner_width > title_width {
        top.push_str(&"─".repeat(inner_width - title_width));
    }
    top.push('╮');

    let mut boxed = vec![top];
    for line in lines {
        let line_width = visible_width(line);
        let right_padding = inner_width - line_width - padding;
        let mut row = String::from("│");
        row.push_str(&" ".repeat(padding));
        row.push_str(line);
        row.push_str(&" ".repeat(right_padding));
        row.push('│');
        boxed.push(row);
    }

    let bottom = format!("╰{}╯", "─".repeat(inner_width));
    boxed.push(bottom);

    format!("\n{}", boxed.join("\n"))
}

fn visible_width(text: &str) -> usize {
    let stripped = strip(text.as_bytes());
    let stripped = String::from_utf8_lossy(&stripped);
    UnicodeWidthStr::width(stripped.as_ref())
}

fn buffer_to_ansi_lines(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
    let width = buffer.area.width as usize;
    let height = buffer.area.height as usize;
    let mut lines = Vec::with_capacity(height);
    let mut current_style = Style::reset();

    for y in 0..height {
        let mut line = String::new();
        let mut skip = 0usize;
        for x in 0..width {
            let cell = &buffer[(x as u16, y as u16)];
            let symbol = cell.symbol();
            let symbol_width = UnicodeWidthStr::width(symbol);
            if skip > 0 {
                skip -= 1;
                continue;
            }

            let style = Style {
                fg: Some(cell.fg),
                bg: Some(cell.bg),
                add_modifier: cell.modifier,
                ..Style::default()
            };

            if style != current_style {
                line.push_str(&style_to_ansi(style));
                current_style = style;
            }

            line.push_str(symbol);
            skip = symbol_width.saturating_sub(1);
        }
        line.push_str(&style_to_ansi(Style::reset()));
        lines.push(line.trim_end().to_string());
    }

    lines
}

fn style_to_ansi(style: Style) -> String {
    let mut buf = Vec::new();
    let _ = queue!(buf, SetAttribute(Attribute::Reset), ResetColor);

    if let Some(fg) = style.fg {
        let _ = queue!(buf, SetForegroundColor(convert_color(fg)));
    }
    if let Some(bg) = style.bg {
        let _ = queue!(buf, SetBackgroundColor(convert_color(bg)));
    }

    let mods = style.add_modifier;
    if mods.contains(Modifier::BOLD) {
        let _ = queue!(buf, SetAttribute(Attribute::Bold));
    }
    if mods.contains(Modifier::DIM) {
        let _ = queue!(buf, SetAttribute(Attribute::Dim));
    }
    if mods.contains(Modifier::ITALIC) {
        let _ = queue!(buf, SetAttribute(Attribute::Italic));
    }
    if mods.contains(Modifier::UNDERLINED) {
        let _ = queue!(buf, SetAttribute(Attribute::Underlined));
    }
    if mods.contains(Modifier::REVERSED) {
        let _ = queue!(buf, SetAttribute(Attribute::Reverse));
    }
    if mods.contains(Modifier::CROSSED_OUT) {
        let _ = queue!(buf, SetAttribute(Attribute::CrossedOut));
    }
    if mods.contains(Modifier::SLOW_BLINK) {
        let _ = queue!(buf, SetAttribute(Attribute::SlowBlink));
    }
    if mods.contains(Modifier::RAPID_BLINK) {
        let _ = queue!(buf, SetAttribute(Attribute::RapidBlink));
    }

    String::from_utf8_lossy(&buf).to_string()
}

fn convert_color(color: Color) -> CtColor {
    match color {
        Color::Reset => CtColor::Reset,
        Color::Black => CtColor::Black,
        Color::Red => CtColor::Red,
        Color::Green => CtColor::Green,
        Color::Yellow => CtColor::Yellow,
        Color::Blue => CtColor::Blue,
        Color::Magenta => CtColor::Magenta,
        Color::Cyan => CtColor::Cyan,
        Color::Gray => CtColor::Grey,
        Color::DarkGray => CtColor::DarkGrey,
        Color::LightRed => CtColor::DarkRed,
        Color::LightGreen => CtColor::DarkGreen,
        Color::LightYellow => CtColor::DarkYellow,
        Color::LightBlue => CtColor::DarkBlue,
        Color::LightMagenta => CtColor::DarkMagenta,
        Color::LightCyan => CtColor::DarkCyan,
        Color::White => CtColor::White,
        Color::Indexed(value) => CtColor::AnsiValue(value),
        Color::Rgb(r, g, b) => CtColor::Rgb { r, g, b },
    }
}
