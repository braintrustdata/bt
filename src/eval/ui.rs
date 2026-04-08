use std::collections::HashMap;
use std::io::IsTerminal;

use crossterm::queue;
use crossterm::style::{
    Attribute, Color as CtColor, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    Stylize,
};
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, Table};
use ratatui::Terminal;
use strip_ansi_escapes::strip;
use unicode_width::UnicodeWidthStr;

use crate::ui::{animations_enabled, is_quiet};

use super::events::{
    EvalEvent, EvalProgressData, ExperimentStart, ExperimentSummary, SseProgressEventData,
};
use super::{MAX_DEFERRED_EVAL_ERRORS, MAX_NAME_LENGTH};

pub(super) struct EvalUi {
    progress: MultiProgress,
    bars: HashMap<String, ProgressBar>,
    bar_style: ProgressStyle,
    spinner_style: ProgressStyle,
    jsonl: bool,
    list: bool,
    verbose: bool,
    deferred_errors: Vec<String>,
    suppressed_stderr_lines: usize,
    finished: bool,
}

impl EvalUi {
    pub(super) fn new(jsonl: bool, list: bool, verbose: bool) -> Self {
        let draw_target = if std::io::stderr().is_terminal() && animations_enabled() && !is_quiet()
        {
            ProgressDrawTarget::stderr_with_hz(10)
        } else {
            ProgressDrawTarget::stderr()
        };
        let progress = MultiProgress::with_draw_target(draw_target);
        let bar_style =
            ProgressStyle::with_template("{bar:10.blue} {msg} {percent}% {pos}/{len} {eta}")
                .unwrap();
        let spinner_style = ProgressStyle::with_template("{spinner} {msg}").unwrap();
        Self {
            progress,
            bars: HashMap::new(),
            bar_style,
            spinner_style,
            jsonl,
            list,
            verbose,
            deferred_errors: Vec::new(),
            suppressed_stderr_lines: 0,
            finished: false,
        }
    }

    pub(super) fn finish(&mut self) {
        if self.finished {
            return;
        }
        for (_, bar) in self.bars.drain() {
            bar.finish_and_clear();
        }
        let _ = self.progress.clear();
        self.progress.set_draw_target(ProgressDrawTarget::hidden());
        self.print_deferred_error_footnote();
        self.finished = true;
    }

    pub(super) fn handle(&mut self, event: EvalEvent) {
        match event {
            EvalEvent::Processing(payload) => {
                self.print_persistent_line(format_processing_line(payload.evaluators));
            }
            EvalEvent::Start(start) => {
                if let Some(line) = format_start_line(&start) {
                    self.print_persistent_line(line);
                }
            }
            EvalEvent::Summary(summary) => {
                if self.jsonl {
                    if let Ok(line) = serde_json::to_string(&summary) {
                        println!("{line}");
                    }
                } else {
                    let rendered = format_experiment_summary(&summary);
                    self.print_persistent_multiline(rendered);
                }
            }
            EvalEvent::Progress(progress) => {
                self.handle_progress(progress);
            }
            EvalEvent::Dependencies { .. } => {}
            EvalEvent::Console { stream, message } => {
                if stream == "stdout" && (self.list || self.jsonl) {
                    println!("{message}");
                } else if stream == "stderr" && !self.verbose {
                    self.suppressed_stderr_lines += 1;
                } else {
                    let _ = self.progress.println(message);
                }
            }
            EvalEvent::Error { message, stack, .. } => {
                let show_hint = message.contains("Please specify an api key");
                if self.verbose {
                    let line = message.as_str().red().to_string();
                    let _ = self.progress.println(line);
                    if let Some(stack) = stack {
                        for line in stack.lines() {
                            let _ = self.progress.println(line.dark_grey().to_string());
                        }
                    }
                } else {
                    self.record_deferred_error(message);
                }
                if show_hint {
                    let hint = "Hint: pass --api-key, set BRAINTRUST_API_KEY, run `bt auth login`/`bt auth login --oauth`, or use --no-send-logs for local evals.";
                    if self.verbose {
                        let _ = self.progress.println(hint.dark_grey().to_string());
                    } else {
                        self.record_deferred_error(hint.to_string());
                    }
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

    fn print_persistent_line(&self, line: String) {
        self.progress.suspend(|| {
            eprintln!("{line}");
        });
    }

    fn print_persistent_multiline(&self, text: String) {
        self.progress.suspend(|| {
            for line in text.lines() {
                eprintln!("{line}");
            }
        });
    }

    fn record_deferred_error(&mut self, message: String) {
        let trimmed = message.trim();
        if trimmed.is_empty() {
            return;
        }
        if self
            .deferred_errors
            .iter()
            .any(|existing| existing == trimmed)
        {
            return;
        }
        if self.deferred_errors.len() < MAX_DEFERRED_EVAL_ERRORS {
            self.deferred_errors.push(trimmed.to_string());
        }
    }

    fn print_deferred_error_footnote(&self) {
        if self.verbose {
            return;
        }
        if self.deferred_errors.is_empty() && self.suppressed_stderr_lines == 0 {
            return;
        }

        eprintln!();
        if !self.deferred_errors.is_empty() {
            let noun = if self.deferred_errors.len() == 1 {
                "error"
            } else {
                "errors"
            };
            eprintln!(
                "Encountered {} evaluator {noun}:",
                self.deferred_errors.len()
            );
            for message in &self.deferred_errors {
                eprintln!("  - {message}");
            }
        }
        if self.suppressed_stderr_lines > 0 {
            eprintln!(
                "Suppressed {} stderr line(s). Re-run with `bt eval --verbose ...` to inspect details.",
                self.suppressed_stderr_lines
            );
        }
    }
}

impl Drop for EvalUi {
    fn drop(&mut self) {
        self.finish();
    }
}

fn fit_name_to_spaces(name: &str, length: usize) -> String {
    let char_count = name.chars().count();
    if char_count < length {
        let mut padded = name.to_string();
        padded.push_str(&" ".repeat(length - char_count));
        return padded;
    }
    if char_count == length {
        return name.to_string();
    }
    if length <= 3 {
        return name.chars().take(length).collect();
    }
    if length <= 5 {
        let truncated: String = name.chars().take(length - 3).collect();
        return format!("{truncated}...");
    }

    let keep_total = length - 3;
    let head_len = keep_total / 2;
    let tail_len = keep_total - head_len;
    let head: String = name.chars().take(head_len).collect();
    let tail: String = name
        .chars()
        .rev()
        .take(tail_len)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{head}...{tail}")
}

fn format_processing_line(evaluators: usize) -> String {
    let noun = if evaluators == 1 {
        "evaluator"
    } else {
        "evaluators"
    };
    format!("Processing {evaluators} {noun}...")
}

fn format_start_line(start: &ExperimentStart) -> Option<String> {
    let experiment_name = start
        .experiment_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let experiment_url = start
        .experiment_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let arrow = "▶".cyan();

    match (experiment_name, experiment_url) {
        (Some(name), Some(url)) => Some(format!(
            "{arrow} Experiment {} is running at {url}",
            name.bold()
        )),
        (Some(name), None) => Some(format!(
            "{arrow} Experiment {} is running at locally",
            name.bold()
        )),
        (None, Some(url)) => Some(format!("{arrow} Experiment is running at {url}")),
        (None, None) => None,
    }
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
        format!("{metric:.0}")
    } else {
        format!("{metric:.2}")
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
        let right_padding = inner_width.saturating_sub(line_width + padding);
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
        Color::LightRed => CtColor::Red,
        Color::LightGreen => CtColor::Green,
        Color::LightYellow => CtColor::Yellow,
        Color::LightBlue => CtColor::Blue,
        Color::LightMagenta => CtColor::Magenta,
        Color::LightCyan => CtColor::Cyan,
        Color::White => CtColor::White,
        Color::Indexed(value) => CtColor::AnsiValue(value),
        Color::Rgb(r, g, b) => CtColor::Rgb { r, g, b },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn box_with_title_handles_ansi_content_without_panicking() {
        let content = "plain line\n\x1b[38;5;196mred text\x1b[0m";
        let boxed = box_with_title("Summary", content);
        assert!(boxed.contains("Summary"));
        assert!(boxed.contains("plain line"));
        assert!(boxed.contains("red text"));
    }

    #[test]
    fn format_processing_line_handles_pluralization() {
        assert_eq!(format_processing_line(1), "Processing 1 evaluator...");
        assert_eq!(format_processing_line(2), "Processing 2 evaluators...");
    }

    #[test]
    fn format_start_line_handles_partial_payload() {
        let start = ExperimentStart {
            experiment_name: Some("my-exp".to_string()),
            experiment_url: Some("https://example.dev/exp".to_string()),
            ..Default::default()
        };
        let line = format_start_line(&start).expect("line should be rendered");
        assert!(line.contains("my-exp"));
        assert!(line.contains("https://example.dev/exp"));

        assert!(format_start_line(&ExperimentStart::default()).is_none());
    }

    #[test]
    fn fit_name_to_spaces_preserves_suffix_when_truncating() {
        let rendered =
            fit_name_to_spaces("Topics [experimentName=facets-real-world-30b-f5a78312]", 40);
        assert_eq!(rendered.chars().count(), 40);
        assert!(rendered.contains("..."));
        assert!(rendered.contains("f5a78312]"));
    }

    #[test]
    fn fit_name_to_spaces_pads_short_names() {
        let rendered = fit_name_to_spaces("short", 10);
        assert_eq!(rendered, "short     ");
    }
}
