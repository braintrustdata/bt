//! Human and JSON rendering for `bt cost`.

use std::fmt::Write as _;
use std::io::{self, Write as _};
use std::path::Path;

use anyhow::Result;
use chrono::FixedOffset;
use dialoguer::console;
use serde_json::{json, Map, Value};

use crate::{
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate},
    utils::{format_cost, pluralize},
};

use super::query::Dimension;
use super::rows::{CostRow, Totals};
use super::ResolvedContext;

/// Everything needed to render a cost report.
pub(super) struct Report<'a> {
    pub ctx: &'a ResolvedContext,
    /// Window bounds, offset-adjusted, no time-zone suffix.
    pub since: String,
    pub until: String,
    /// Display time-zone: applied to day/hour dimension values, and reported in
    /// JSON as `timezone`.
    pub offset: FixedOffset,
    pub timezone: &'a str,
    pub display_dims: &'a [Dimension],
    pub sources: &'a [String],
    pub rows: &'a [CostRow],
    pub totals: &'a Totals,
    pub pricing_file: Option<&'a Path>,
    pub truncated: bool,
    pub verbose: bool,
    /// Pre-rendered ASCII chart, shown in place of the table when present.
    pub chart: Option<&'a str>,
    /// Inline-image escape sequence, shown in place of the table when present.
    pub image: Option<&'a str>,
}

fn key_display(value: Option<&String>) -> String {
    match value {
        Some(value) if !value.is_empty() => value.clone(),
        _ => "—".to_string(),
    }
}

/// A dimension value for display: day/hour buckets are converted into the
/// report's time zone; other dimensions pass through unchanged.
fn dimension_value(dim: Dimension, value: Option<&String>, offset: FixedOffset) -> Option<String> {
    let value = value?;
    if matches!(dim, Dimension::Day | Dimension::Hour) {
        Some(super::pricing::reoffset_local(value, offset).unwrap_or_else(|| value.clone()))
    } else {
        Some(value.clone())
    }
}

fn cost_source(row: &CostRow) -> &'static str {
    match (row.logged_priced_spans > 0, row.file_priced_spans > 0) {
        (true, true) => "mixed",
        (true, false) => "logged",
        (false, true) => "computed",
        (false, false) => "none",
    }
}

/// Quote a field per RFC 4180 when it contains a delimiter, quote, or newline.
fn csv_field(value: &str) -> String {
    if value.contains(['"', ',', '\n', '\r']) {
        format!("\"{}\"", value.replace('"', "\"\""))
    } else {
        value.to_string()
    }
}

/// Emit the breakdown rows as CSV (header + one row per group). CSV is flat, so
/// the total and window metadata are omitted — the rows are the data, and
/// consumers aggregate. Day/hour values are in the report's time zone.
pub(super) fn print_csv(report: &Report) -> Result<()> {
    let mut headers: Vec<String> = report
        .display_dims
        .iter()
        .map(|dim| dim.alias().to_string())
        .collect();
    headers.push("cost".to_string());
    headers.push("priced_spans".to_string());
    headers.push("unpriced_spans".to_string());
    if report.pricing_file.is_some() {
        headers.push("pricing_file_cost".to_string());
    }

    let mut out = String::new();
    out.push_str(
        &headers
            .iter()
            .map(|field| csv_field(field))
            .collect::<Vec<_>>()
            .join(","),
    );
    out.push('\n');

    for row in report.rows {
        let mut fields: Vec<String> = report
            .display_dims
            .iter()
            .zip(row.keys.iter())
            .map(|(dim, value)| {
                dimension_value(*dim, value.as_ref(), report.offset).unwrap_or_default()
            })
            .collect();
        fields.push(
            row.cost()
                .map(|cost| format!("{cost:.6}"))
                .unwrap_or_default(),
        );
        fields.push(row.priced_spans().to_string());
        fields.push(row.unpriced_spans.to_string());
        if report.pricing_file.is_some() {
            fields.push(format!("{:.6}", row.file_cost));
        }
        out.push_str(
            &fields
                .iter()
                .map(|field| csv_field(field))
                .collect::<Vec<_>>()
                .join(","),
        );
        out.push('\n');
    }

    print!("{out}");
    Ok(())
}

pub(super) fn print_json(report: &Report) -> Result<()> {
    let rows: Vec<Value> = report
        .rows
        .iter()
        .map(|row| {
            let mut object = Map::new();
            for (dim, value) in report.display_dims.iter().zip(row.keys.iter()) {
                let value = dimension_value(*dim, value.as_ref(), report.offset);
                object.insert(
                    dim.alias().to_string(),
                    value.map(Value::String).unwrap_or(Value::Null),
                );
            }
            object.insert("cost".to_string(), cost_to_json(row.cost()));
            object.insert("cost_source".to_string(), json!(cost_source(row)));
            object.insert("priced_spans".to_string(), json!(row.priced_spans()));
            object.insert("unpriced_spans".to_string(), json!(row.unpriced_spans));
            // The pricing-file contribution is only meaningful (and non-zero)
            // when a pricing file is in play.
            if report.pricing_file.is_some() {
                object.insert(
                    "pricing_file_cost".to_string(),
                    json!(round6(row.file_cost)),
                );
            }
            if report.verbose {
                object.insert("no_usage_spans".to_string(), json!(row.no_usage_spans));
            }
            Value::Object(object)
        })
        .collect();

    let total = json!({
        "cost": cost_to_json(report.totals.cost()),
        "priced_spans": report.totals.priced_spans(),
        "unpriced_spans": report.totals.unpriced_spans,
        "no_usage_spans": report.totals.no_usage_spans,
        "candidate_spans": report.totals.candidate_spans,
        "truncated": report.truncated,
    });

    let output = json!({
        "org": report.ctx.client.org_name(),
        "project": report.ctx.project.name,
        "currency": "USD",
        "since": report.since,
        "until": report.until,
        "timezone": report.timezone,
        "group_by": report.display_dims.iter().map(|dim| dim.alias()).collect::<Vec<_>>(),
        "sources": report.sources,
        "pricing_file": report.pricing_file.map(|path| path.display().to_string()),
        "rows": rows,
        "total": total,
    });
    println!("{}", serde_json::to_string(&output)?);
    Ok(())
}

fn cost_to_json(cost: Option<f64>) -> Value {
    match cost {
        Some(cost) => json!(round6(cost)),
        None => Value::Null,
    }
}

fn round6(value: f64) -> f64 {
    (value * 1_000_000.0).round() / 1_000_000.0
}

pub(super) fn print_table(report: &Report) -> Result<()> {
    let mut output = String::new();
    let count = format!(
        "{} {}",
        report.rows.len(),
        pluralize(report.rows.len(), "cost group", None)
    );
    writeln!(
        output,
        "{} in {} {} {}",
        console::style(count),
        console::style(report.ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(&report.ctx.project.name).bold()
    )?;
    writeln!(
        output,
        "{} {} {}",
        console::style(&report.since).dim(),
        console::style("to").dim(),
        console::style(&report.until).dim()
    )?;
    writeln!(
        output,
        "{}: {}",
        console::style("Sources").dim(),
        console::style(report.sources.join(", ")).dim()
    )?;
    if let Some(path) = report.pricing_file {
        writeln!(
            output,
            "{}: {}",
            console::style("Pricing").dim(),
            console::style(path.display()).dim()
        )?;
    }
    output.push('\n');

    // An inline image can't go through the pager (less would show raw escape
    // bytes), so write the header, the image, and the total straight to stdout.
    if let Some(image) = report.image {
        let mut tail = String::new();
        write_total(&mut tail, report)?;
        tail.push('\n');
        let mut stdout = io::stdout().lock();
        write!(stdout, "{output}")?;
        write!(stdout, "{image}")?;
        writeln!(stdout, "{tail}")?;
        return Ok(());
    }

    // A chart replaces the per-row table: it carries the breakdown, so the list
    // would be redundant. The header block and Total still frame it.
    if let Some(chart) = report.chart {
        output.push_str(chart);
        write_total(&mut output, report)?;
        output.push('\n');
        print_with_pager(&output)?;
        return Ok(());
    }

    // The pricing-file column only appears when a pricing file is set, so the
    // common case never shows a column of zeros.
    let show_file_column = report.pricing_file.is_some();
    let mut headers: Vec<_> = report
        .display_dims
        .iter()
        .map(|dim| header(dim.header()))
        .collect();
    headers.push(header("Cost"));
    if show_file_column {
        headers.push(header("Pricing file"));
    }
    if report.verbose {
        headers.push(header("Priced"));
        headers.push(header("Unpriced"));
    }
    let mut table = styled_table();
    table.set_header(headers);
    apply_column_padding(&mut table, (0, 4));

    for row in report.rows {
        let mut cells: Vec<String> = report
            .display_dims
            .iter()
            .zip(row.keys.iter())
            .map(|(dim, value)| {
                let value = dimension_value(*dim, value.as_ref(), report.offset);
                truncate(&key_display(value.as_ref()), 50)
            })
            .collect();
        cells.push(cost_cell(row));
        if show_file_column {
            cells.push(format_cost(row.file_cost));
        }
        if report.verbose {
            cells.push(row.priced_spans().to_string());
            cells.push(row.unpriced_spans.to_string());
        }
        table.add_row(cells);
    }
    write!(output, "{table}")?;

    write_total(&mut output, report)?;
    output.push('\n');
    print_with_pager(&output)?;
    Ok(())
}

fn cost_cell(row: &CostRow) -> String {
    match row.cost() {
        // A cost with unpriced spans in the same group is approximate.
        Some(cost) if row.unpriced_spans > 0 => format!("~{}", format_cost(cost)),
        Some(cost) => format_cost(cost),
        None if row.unpriced_spans > 0 => "unknown".to_string(),
        None => "n/a".to_string(),
    }
}

fn write_total(output: &mut String, report: &Report) -> Result<()> {
    let totals = report.totals;
    let total_cost = totals
        .cost()
        .map(format_cost)
        .unwrap_or_else(|| "n/a".to_string());
    let prefix = if totals.unpriced_spans > 0 { "~" } else { "" };
    write!(
        output,
        "\n\nTotal {}",
        console::style(format!("{prefix}{total_cost}")).bold()
    )?;
    write!(
        output,
        "   {} priced",
        console::style(totals.priced_spans()).dim()
    )?;
    if totals.unpriced_spans > 0 {
        let total_spans = totals.priced_spans() + totals.unpriced_spans;
        write!(
            output,
            "   {}",
            console::style(format!(
                "{} of {} spans without usage (not priced)",
                totals.unpriced_spans, total_spans
            ))
            .yellow()
        )?;
    }
    if totals.no_usage_spans > 0 {
        write!(
            output,
            "   {}",
            console::style(format!("{} without cost or usage", totals.no_usage_spans)).dim()
        )?;
    }
    if report.truncated {
        write!(
            output,
            "\n{}",
            console::style(
                "rows truncated at the backend limit; total is a lower bound — pass --no-limit for the exact figure"
            )
            .yellow()
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::csv_field;

    #[test]
    fn csv_field_quotes_only_when_needed() {
        assert_eq!(csv_field("gpt-5.6-sol"), "gpt-5.6-sol");
        assert_eq!(csv_field("a,b"), "\"a,b\"");
        assert_eq!(csv_field("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(csv_field("line\nbreak"), "\"line\nbreak\"");
    }
}
