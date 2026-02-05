use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::Args;
use crossterm::style::{Attribute, Stylize};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use serde::Deserialize;
use strip_ansi_escapes::strip;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use unicode_width::UnicodeWidthStr;

use crate::args::BaseArgs;

const MAX_NAME_LENGTH: usize = 40;

#[derive(Debug, Clone, Args)]
pub struct EvalArgs {
    /// One or more eval files to execute (e.g. foo.eval.ts)
    #[arg(required = true)]
    pub files: Vec<String>,
}

pub async fn run(base: BaseArgs, args: EvalArgs) -> Result<()> {
    let runner = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("eval-runner.ts");

    let socket_path = build_sse_socket_path()?;
    let _ = std::fs::remove_file(&socket_path);
    let listener = UnixListener::bind(&socket_path).context("failed to bind SSE unix socket")?;
    let (tx, mut rx) = mpsc::unbounded_channel();

    let tx_sse = tx.clone();
    let sse_task = tokio::spawn(async move {
        match listener.accept().await {
            Ok((stream, _)) => {
                if let Err(err) = read_sse_stream(stream, tx_sse.clone()).await {
                    let _ = tx_sse.send(EvalEvent::Error(format!("SSE stream error: {err}")));
                }
            }
            Err(err) => {
                let _ = tx_sse.send(EvalEvent::Error(format!(
                    "Failed to accept SSE connection: {err}"
                )));
            }
        };
    });

    let mut cmd = if let Some(tsx_path) = find_tsx_binary() {
        let mut command = Command::new(tsx_path);
        command.arg(runner).args(&args.files);
        command
    } else {
        let mut command = Command::new("npx");
        command
            .arg("--yes")
            .arg("tsx")
            .arg(runner)
            .args(&args.files);
        command
    };

    cmd.envs(build_env(base));
    cmd.env(
        "BT_EVAL_SSE_SOCK",
        socket_path.to_string_lossy().to_string(),
    );
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .context("failed to start eval runner (npx tsx)")?;

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
            Some(event) = rx.recv() => {
                ui.handle(event);
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

fn build_env(base: BaseArgs) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    if let Some(api_key) = base.api_key {
        envs.push(("BRAINTRUST_API_KEY".to_string(), api_key));
    }
    if let Some(api_url) = base.api_url {
        envs.push(("BRAINTRUST_API_URL".to_string(), api_url));
    }
    if let Some(project) = base.project {
        envs.push(("BRAINTRUST_DEFAULT_PROJECT".to_string(), project));
    }
    envs
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
    Error(String),
    Console { _stream: String, message: String },
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
            let _ = tx.send(EvalEvent::Error(data));
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
            EvalEvent::Error(message) => {
                let line = message.red().to_string();
                let _ = self.progress.println(line);
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
        let mut rows: Vec<Vec<String>> = Vec::new();

        if has_comparison {
            rows.push(vec![
                "Name".dark_grey().to_string(),
                "Value".dark_grey().to_string(),
                "Change".dark_grey().to_string(),
                "Improvements".dark_grey().to_string(),
                "Regressions".dark_grey().to_string(),
            ]);
        }

        let mut score_values: Vec<_> = summary.scores.values().collect();
        score_values.sort_by(|a, b| a.name.cmp(&b.name));
        for score in score_values {
            let score_percent = format!("{:.2}%", score.score * 100.0);
            let diff = format_diff(score.diff);
            let improvements = format_improvements(score.improvements);
            let regressions = format_regressions(score.regressions);
            let name = truncate_plain(&score.name, MAX_NAME_LENGTH);
            let name = format!("{} {}", "◯".blue(), name);
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
                let formatted_value = format_metric_value(metric.metric, &metric.unit);
                let diff = format_diff(metric.diff);
                let improvements = format_improvements(metric.improvements);
                let regressions = format_regressions(metric.regressions);
                let name = truncate_plain(&metric.name, MAX_NAME_LENGTH);
                let name = format!("{} {}", "◯".magenta(), name);
                if has_comparison {
                    rows.push(vec![name, formatted_value, diff, improvements, regressions]);
                } else {
                    rows.push(vec![name, formatted_value]);
                }
            }
        }

        parts.push(render_table(rows, has_comparison));
    }

    if let Some(url) = &summary.experiment_url {
        parts.push(format!("See results at {url}"));
    }

    let content = parts.join("\n\n");
    box_with_title("Experiment summary", &content)
}

fn format_diff(diff: Option<f64>) -> String {
    match diff {
        Some(value) => {
            let sign = if value > 0.0 { "+" } else { "" };
            let percent = format!("{sign}{:.2}%", value * 100.0);
            if value > 0.0 {
                percent.green().to_string()
            } else {
                percent.red().to_string()
            }
        }
        None => "-".dark_grey().to_string(),
    }
}

fn format_improvements(value: i64) -> String {
    if value > 0 {
        value
            .to_string()
            .green()
            .attribute(Attribute::Dim)
            .to_string()
    } else {
        "-".dark_grey().to_string()
    }
}

fn format_regressions(value: i64) -> String {
    if value > 0 {
        value
            .to_string()
            .red()
            .attribute(Attribute::Dim)
            .to_string()
    } else {
        "-".dark_grey().to_string()
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

fn render_table(rows: Vec<Vec<String>>, has_comparison: bool) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let columns = if has_comparison { 5 } else { 2 };
    let mut widths = vec![0usize; columns];
    for row in &rows {
        for (idx, cell) in row.iter().enumerate().take(columns) {
            let w = visible_width(cell);
            if w > widths[idx] {
                widths[idx] = w;
            }
        }
    }

    let mut lines = Vec::new();
    for (row_idx, row) in rows.iter().enumerate() {
        let mut line = String::new();
        for (idx, cell) in row.iter().enumerate().take(columns) {
            let width = widths[idx];
            let is_header = row_idx == 0 && has_comparison;
            let aligned = if idx == 0 || is_header {
                pad_right_visible(cell, width)
            } else {
                pad_left_visible(cell, width)
            };
            line.push_str(&aligned);
            if idx + 1 < columns {
                line.push(' ');
            }
        }
        lines.push(line);
    }

    lines.join("\n")
}

fn pad_right_visible(text: &str, width: usize) -> String {
    let w = visible_width(text);
    if w >= width {
        return text.to_string();
    }
    format!("{text}{}", " ".repeat(width - w))
}

fn pad_left_visible(text: &str, width: usize) -> String {
    let w = visible_width(text);
    if w >= width {
        return text.to_string();
    }
    format!("{}{}", " ".repeat(width - w), text)
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
