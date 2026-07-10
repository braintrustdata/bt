use crossterm::queue;
use crossterm::style::{
    Attribute, Color as CtColor, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    Stylize,
};
use ratatui::backend::TestBackend;
use ratatui::layout::Constraint;
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, Table};
use ratatui::Terminal;
use strip_ansi_escapes::strip;
use unicode_width::UnicodeWidthStr;

const MAX_SUMMARY_METRIC_NAME_LENGTH: usize = 40;
const MAX_SUMMARY_EXPERIMENT_NAME_LENGTH: usize = 28;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SummaryMetricKind {
    Score,
    Metric,
}

#[derive(Debug, Clone)]
pub struct SummaryExperimentColumn {
    pub name: String,
    pub role: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct SummaryMetricCell {
    pub value: Option<f64>,
    pub delta: Option<f64>,
    pub improvements: i64,
    pub regressions: i64,
}

#[derive(Debug, Clone)]
pub struct SummaryMetricRow {
    pub name: String,
    pub kind: SummaryMetricKind,
    pub unit: Option<String>,
    pub cells: Vec<SummaryMetricCell>,
}

#[derive(Debug, Clone, Default)]
pub struct SummaryTableOptions {
    pub show_all_rows: bool,
    pub hidden_rows_message: Option<String>,
}

pub fn summary_metric_unit(metric_name: &str, unit: Option<&str>) -> Option<String> {
    let normalized_name = metric_name.to_ascii_lowercase().replace([' ', '-'], "_");
    if matches!(normalized_name.as_str(), "time_to_first_token" | "ttft") {
        return Some("s".to_string());
    }

    unit.filter(|unit| !unit.is_empty())
        .map(ToString::to_string)
}

pub fn render_ratatui_table(
    header: Option<Vec<Line<'static>>>,
    rows: Vec<Vec<Line<'static>>>,
    columns: usize,
) -> String {
    if rows.is_empty() || columns == 0 {
        return String::new();
    }

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

pub fn render_experiment_summary_table(
    columns: &[SummaryExperimentColumn],
    rows: &[SummaryMetricRow],
    options: &SummaryTableOptions,
) -> String {
    if columns.is_empty() || rows.is_empty() {
        return String::new();
    }

    let rendered_columns = rendered_summary_columns(columns);
    let (visible_rows, hidden_count) = visible_summary_rows(rows, columns.len(), options);

    let mut header = Vec::with_capacity(columns.len() + 1);
    header.push(header_line("Scores and metrics"));
    for column in &rendered_columns {
        header.push(header_line(&column.header));
    }

    let rendered_rows = visible_rows
        .iter()
        .map(|row| {
            let mut cells = Vec::with_capacity(columns.len() + 1);
            cells.push(summary_metric_name_line(row));
            for idx in 0..columns.len() {
                let cell = row.cells.get(idx).cloned().unwrap_or_default();
                cells.push(summary_metric_cell_line(row, &cell));
            }
            cells
        })
        .collect::<Vec<_>>();

    let mut parts = Vec::new();
    if let Some(legend) = summary_column_legend(&rendered_columns) {
        parts.push(legend);
    }
    if summary_rows_have_counts(&visible_rows) {
        parts.push("counts: ↑n/↓m = improvements/regressions".to_string());
    }
    if hidden_count > 0 {
        if let Some(message) = options.hidden_rows_message.as_deref() {
            parts.push(format!("hidden rows: {hidden_count} {message}"));
        }
    }
    if !rendered_rows.is_empty() {
        parts.push(render_ratatui_table(
            Some(header),
            rendered_rows,
            columns.len() + 1,
        ));
    }

    parts.join("\n\n")
}

pub fn header_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(
        text.to_string(),
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    ))
}

#[derive(Debug, Clone)]
struct RenderedSummaryColumn {
    header: String,
    alias: String,
    full_name: String,
    role: Option<String>,
}

fn rendered_summary_columns(columns: &[SummaryExperimentColumn]) -> Vec<RenderedSummaryColumn> {
    let aliases = abbreviated_column_names(columns);
    columns
        .iter()
        .zip(aliases)
        .map(|(column, alias)| RenderedSummaryColumn {
            header: summary_column_header(&alias, column.role.as_deref()),
            alias,
            full_name: column.name.clone(),
            role: column.role.clone(),
        })
        .collect()
}

fn summary_column_header(alias: &str, role: Option<&str>) -> String {
    let name = super::table::truncate(alias, MAX_SUMMARY_EXPERIMENT_NAME_LENGTH);
    match role.filter(|role| !role.is_empty()) {
        Some(role) => format!("{name} ({role})"),
        None => name,
    }
}

fn summary_column_legend(columns: &[RenderedSummaryColumn]) -> Option<String> {
    let needs_legend = columns.iter().any(|column| {
        column.alias != column.full_name
            || column.role.as_deref().is_some_and(|role| !role.is_empty())
    });
    if !needs_legend {
        return None;
    }

    let mut lines = Vec::with_capacity(columns.len() + 1);
    lines.push("experiments:".to_string());
    for column in columns {
        let header = summary_column_header(&column.alias, column.role.as_deref());
        lines.push(format!("  {header} = {}", column.full_name));
    }
    Some(lines.join("\n"))
}

fn abbreviated_column_names(columns: &[SummaryExperimentColumn]) -> Vec<String> {
    let tokenized = columns
        .iter()
        .map(|column| tokenize_experiment_name(&column.name))
        .collect::<Vec<_>>();
    let prefix_len = common_prefix_len(&tokenized);
    let suffix_len = common_suffix_len(&tokenized, prefix_len);

    let mut aliases = tokenized
        .iter()
        .zip(columns)
        .map(|(tokens, column)| {
            let end = tokens.len().saturating_sub(suffix_len);
            let alias_tokens = if prefix_len < end {
                &tokens[prefix_len..end]
            } else {
                &tokens[..]
            };
            let alias = alias_tokens.join("-");
            if alias.is_empty() {
                column.name.clone()
            } else {
                alias
            }
        })
        .collect::<Vec<_>>();
    make_aliases_unique(&mut aliases);
    aliases
}

fn tokenize_experiment_name(name: &str) -> Vec<String> {
    name.split(['-', '_', ' ', '.'])
        .filter(|part| !part.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn common_prefix_len(tokenized: &[Vec<String>]) -> usize {
    let Some(first) = tokenized.first() else {
        return 0;
    };
    let mut len = 0;
    'outer: while let Some(candidate) = first.get(len) {
        for tokens in tokenized.iter().skip(1) {
            if tokens.get(len) != Some(candidate) {
                break 'outer;
            }
        }
        len += 1;
    }
    len
}

fn common_suffix_len(tokenized: &[Vec<String>], prefix_len: usize) -> usize {
    let Some(first) = tokenized.first() else {
        return 0;
    };
    let mut len = 0;
    'outer: loop {
        if first.len().saturating_sub(len) <= prefix_len {
            break;
        }
        let candidate = &first[first.len() - 1 - len];
        for tokens in tokenized.iter().skip(1) {
            if tokens.len().saturating_sub(len) <= prefix_len
                || tokens.get(tokens.len() - 1 - len) != Some(candidate)
            {
                break 'outer;
            }
        }
        len += 1;
    }
    len
}

fn make_aliases_unique(aliases: &mut [String]) {
    for idx in 0..aliases.len() {
        if aliases[..idx].iter().any(|alias| alias == &aliases[idx]) {
            aliases[idx] = format!("{} #{}", aliases[idx], idx + 1);
        }
    }
}

fn visible_summary_rows<'a>(
    rows: &'a [SummaryMetricRow],
    columns: usize,
    options: &SummaryTableOptions,
) -> (Vec<&'a SummaryMetricRow>, usize) {
    if options.show_all_rows {
        return (rows.iter().collect(), 0);
    }

    let visible = rows
        .iter()
        .filter(|row| summary_row_has_signal(row, columns))
        .collect::<Vec<_>>();
    let hidden_count = rows.len().saturating_sub(visible.len());
    (visible, hidden_count)
}

fn summary_row_has_signal(row: &SummaryMetricRow, columns: usize) -> bool {
    let start_idx = if columns > 1 { 1 } else { 0 };
    (start_idx..columns).any(|idx| {
        let cell = row.cells.get(idx).cloned().unwrap_or_default();
        cell.value.is_some_and(|value| value.abs() > f64::EPSILON)
            || cell.delta.is_some_and(|delta| delta.abs() > f64::EPSILON)
            || cell.improvements > 0
            || cell.regressions > 0
    })
}

fn summary_rows_have_counts(rows: &[&SummaryMetricRow]) -> bool {
    rows.iter().any(|row| {
        row.cells
            .iter()
            .any(|cell| cell.improvements > 0 || cell.regressions > 0)
    })
}

fn summary_metric_name_line(row: &SummaryMetricRow) -> Line<'static> {
    let color = match row.kind {
        SummaryMetricKind::Score => Color::Blue,
        SummaryMetricKind::Metric => Color::Magenta,
    };
    Line::from(vec![
        Span::styled("◯", Style::default().fg(color)),
        Span::raw(" "),
        Span::raw(super::table::truncate(
            &row.name,
            MAX_SUMMARY_METRIC_NAME_LENGTH,
        )),
    ])
}

fn summary_metric_cell_line(row: &SummaryMetricRow, cell: &SummaryMetricCell) -> Line<'static> {
    let mut spans = vec![Span::raw(summary_value(row, cell.value))];
    let mut suffixes = Vec::new();
    let has_counts = cell.improvements > 0 || cell.regressions > 0;

    if let Some(delta) = cell
        .delta
        .filter(|delta| delta.abs() > f64::EPSILON || has_counts)
    {
        suffixes.push(summary_delta(row, delta));
    }
    if has_counts {
        suffixes.push(summary_counts(cell.improvements, cell.regressions));
    }

    if !suffixes.is_empty() {
        spans.push(Span::raw(format!(" ({})", suffixes.join("; "))));
    }

    Line::from(spans)
}

fn summary_counts(improvements: i64, regressions: i64) -> String {
    match (improvements, regressions) {
        (improvements, 0) if improvements > 0 => format!("↑{improvements}"),
        (0, regressions) if regressions > 0 => format!("↓{regressions}"),
        _ => format!("↑{improvements}/↓{regressions}"),
    }
}

fn summary_value(row: &SummaryMetricRow, value: Option<f64>) -> String {
    match (row.kind, value) {
        (_, None) => "-".to_string(),
        (SummaryMetricKind::Score, Some(value)) => format!("{:.2}%", value * 100.0),
        (SummaryMetricKind::Metric, Some(value)) => {
            format_metric_value(value, row.unit.as_deref().unwrap_or_default())
        }
    }
}

fn summary_delta(row: &SummaryMetricRow, delta: f64) -> String {
    match row.kind {
        SummaryMetricKind::Score => {
            let sign = if delta > 0.0 { "+" } else { "" };
            format!("{sign}{:.2}%", delta * 100.0)
        }
        SummaryMetricKind::Metric => {
            let sign = if delta > 0.0 {
                "+"
            } else if delta < 0.0 {
                "-"
            } else {
                ""
            };
            let value = format_metric_value(delta.abs(), row.unit.as_deref().unwrap_or_default());
            format!("{sign}{value}")
        }
    }
}

fn format_metric_value(metric: f64, unit: &str) -> String {
    let formatted = if metric.fract() == 0.0 {
        format!("{metric:.0}")
    } else {
        format!("{metric:.2}")
    };
    match unit {
        "$" => format!("{unit}{formatted}"),
        "" | "-" => formatted,
        _ => format!("{formatted}{unit}"),
    }
}

pub fn box_with_title(title: &str, content: &str) -> String {
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
    fn experiment_summary_table_uses_compact_improvement_regression_counts() {
        let rendered = render_experiment_summary_table(
            &[
                SummaryExperimentColumn {
                    name: "baseline".to_string(),
                    role: Some("baseline".to_string()),
                },
                SummaryExperimentColumn {
                    name: "comparison".to_string(),
                    role: Some("comparison".to_string()),
                },
            ],
            &[SummaryMetricRow {
                name: "quality".to_string(),
                kind: SummaryMetricKind::Score,
                unit: Some("%".to_string()),
                cells: vec![
                    SummaryMetricCell {
                        value: Some(0.4),
                        delta: None,
                        improvements: 0,
                        regressions: 0,
                    },
                    SummaryMetricCell {
                        value: Some(0.5),
                        delta: Some(0.1),
                        improvements: 2,
                        regressions: 1,
                    },
                ],
            }],
            &SummaryTableOptions::default(),
        );
        let stripped = strip_ansi_escapes::strip(rendered.as_bytes());
        let stripped = String::from_utf8_lossy(&stripped);

        assert!(stripped.contains("+10.00%"));
        assert!(stripped.contains("↑2/↓1"));
        assert!(stripped.contains("counts: ↑n/↓m = improvements/regressions"));
        assert!(!stripped.contains("I=2"));
        assert!(!stripped.contains("R=1"));
    }

    #[test]
    fn experiment_summary_table_omits_zero_side_of_counts() {
        let rendered = render_experiment_summary_table(
            &[
                SummaryExperimentColumn {
                    name: "baseline".to_string(),
                    role: Some("baseline".to_string()),
                },
                SummaryExperimentColumn {
                    name: "comparison".to_string(),
                    role: Some("comparison".to_string()),
                },
            ],
            &[
                SummaryMetricRow {
                    name: "better".to_string(),
                    kind: SummaryMetricKind::Score,
                    unit: Some("%".to_string()),
                    cells: vec![
                        SummaryMetricCell {
                            value: Some(0.4),
                            delta: None,
                            improvements: 0,
                            regressions: 0,
                        },
                        SummaryMetricCell {
                            value: Some(0.5),
                            delta: Some(0.1),
                            improvements: 2,
                            regressions: 0,
                        },
                    ],
                },
                SummaryMetricRow {
                    name: "worse".to_string(),
                    kind: SummaryMetricKind::Score,
                    unit: Some("%".to_string()),
                    cells: vec![
                        SummaryMetricCell {
                            value: Some(0.5),
                            delta: None,
                            improvements: 0,
                            regressions: 0,
                        },
                        SummaryMetricCell {
                            value: Some(0.4),
                            delta: Some(-0.1),
                            improvements: 0,
                            regressions: 3,
                        },
                    ],
                },
            ],
            &SummaryTableOptions::default(),
        );
        let stripped = strip_ansi_escapes::strip(rendered.as_bytes());
        let stripped = String::from_utf8_lossy(&stripped);

        assert!(stripped.contains("↑2"));
        assert!(stripped.contains("↓3"));
        assert!(!stripped.contains("↑2/↓0"));
        assert!(!stripped.contains("↑0/↓3"));
    }

    #[test]
    fn experiment_summary_value_cells_are_left_aligned() {
        let row = SummaryMetricRow {
            name: "quality".to_string(),
            kind: SummaryMetricKind::Score,
            unit: Some("%".to_string()),
            cells: vec![],
        };
        let line = summary_metric_cell_line(
            &row,
            &SummaryMetricCell {
                value: Some(0.5),
                delta: Some(0.1),
                improvements: 2,
                regressions: 1,
            },
        );

        assert!(line.alignment.is_none());
    }

    #[test]
    fn summary_metric_unit_treats_time_to_first_token_as_seconds() {
        assert_eq!(
            summary_metric_unit("time_to_first_token", Some("tok")).as_deref(),
            Some("s")
        );
        assert_eq!(
            summary_metric_unit("Time to first token", Some("tok")).as_deref(),
            Some("s")
        );
    }

    #[test]
    fn experiment_summary_table_abbreviates_common_name_prefixes_with_legend() {
        let rendered = render_experiment_summary_table(
            &[
                SummaryExperimentColumn {
                    name: "facets-self-contained-sentiment-v3".to_string(),
                    role: Some("baseline".to_string()),
                },
                SummaryExperimentColumn {
                    name: "facets-self-contained-bdea83fd".to_string(),
                    role: Some("comparison".to_string()),
                },
            ],
            &[SummaryMetricRow {
                name: "quality".to_string(),
                kind: SummaryMetricKind::Score,
                unit: Some("%".to_string()),
                cells: vec![
                    SummaryMetricCell {
                        value: Some(0.4),
                        delta: None,
                        improvements: 0,
                        regressions: 0,
                    },
                    SummaryMetricCell {
                        value: Some(0.5),
                        delta: Some(0.1),
                        improvements: 0,
                        regressions: 0,
                    },
                ],
            }],
            &SummaryTableOptions::default(),
        );
        let stripped = strip_ansi_escapes::strip(rendered.as_bytes());
        let stripped = String::from_utf8_lossy(&stripped);

        assert!(stripped.contains("sentiment-v3 (baseline)"));
        assert!(stripped.contains("bdea83fd (comparison)"));
        assert!(stripped.contains("sentiment-v3 (baseline) = facets-self-contained-sentiment-v3"));
    }

    #[test]
    fn experiment_summary_table_hides_zero_no_change_rows_by_default() {
        let rendered = render_experiment_summary_table(
            &[SummaryExperimentColumn {
                name: "experiment".to_string(),
                role: None,
            }],
            &[
                SummaryMetricRow {
                    name: "zero_metric".to_string(),
                    kind: SummaryMetricKind::Metric,
                    unit: None,
                    cells: vec![SummaryMetricCell {
                        value: Some(0.0),
                        delta: None,
                        improvements: 0,
                        regressions: 0,
                    }],
                },
                SummaryMetricRow {
                    name: "duration".to_string(),
                    kind: SummaryMetricKind::Metric,
                    unit: Some("s".to_string()),
                    cells: vec![SummaryMetricCell {
                        value: Some(1.0),
                        delta: None,
                        improvements: 0,
                        regressions: 0,
                    }],
                },
            ],
            &SummaryTableOptions {
                show_all_rows: false,
                hidden_rows_message: Some("zero/no-change rows omitted".to_string()),
            },
        );
        let stripped = strip_ansi_escapes::strip(rendered.as_bytes());
        let stripped = String::from_utf8_lossy(&stripped);

        assert!(!stripped.contains("zero_metric"));
        assert!(stripped.contains("duration"));
        assert!(stripped.contains("hidden rows: 1 zero/no-change rows omitted"));
    }

    #[test]
    fn experiment_summary_table_hides_baseline_only_rows_when_comparing() {
        let rendered = render_experiment_summary_table(
            &[
                SummaryExperimentColumn {
                    name: "baseline".to_string(),
                    role: Some("baseline".to_string()),
                },
                SummaryExperimentColumn {
                    name: "comparison".to_string(),
                    role: Some("comparison".to_string()),
                },
            ],
            &[
                SummaryMetricRow {
                    name: "baseline_only".to_string(),
                    kind: SummaryMetricKind::Metric,
                    unit: Some("s".to_string()),
                    cells: vec![
                        SummaryMetricCell {
                            value: Some(1.0),
                            delta: None,
                            improvements: 0,
                            regressions: 0,
                        },
                        SummaryMetricCell {
                            value: None,
                            delta: None,
                            improvements: 0,
                            regressions: 0,
                        },
                    ],
                },
                SummaryMetricRow {
                    name: "changed".to_string(),
                    kind: SummaryMetricKind::Metric,
                    unit: Some("s".to_string()),
                    cells: vec![
                        SummaryMetricCell {
                            value: Some(1.0),
                            delta: None,
                            improvements: 0,
                            regressions: 0,
                        },
                        SummaryMetricCell {
                            value: Some(2.0),
                            delta: Some(1.0),
                            improvements: 0,
                            regressions: 1,
                        },
                    ],
                },
            ],
            &SummaryTableOptions {
                show_all_rows: false,
                hidden_rows_message: Some("zero/no-change rows omitted".to_string()),
            },
        );
        let stripped = strip_ansi_escapes::strip(rendered.as_bytes());
        let stripped = String::from_utf8_lossy(&stripped);

        assert!(!stripped.contains("baseline_only"));
        assert!(stripped.contains("changed"));
        assert!(stripped.contains("hidden rows: 1 zero/no-change rows omitted"));
    }
}
