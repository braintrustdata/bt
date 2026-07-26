use std::cmp::Ordering;

use serde::Serialize;
use serde_json::{Map, Value};

use super::{
    build_span_entries, extract_duration_seconds, extract_model_name, extract_parent_span_id,
    extract_span_end_seconds, extract_span_name_and_type, extract_start_time,
    format_compact_duration, format_object_ref_arg, format_u64_with_commas, parse_f64ish,
    parse_u64ish, profile_flag_suffix, project_label, span_has_error, value_as_object_owned,
    ResolvedTraceCommandTarget,
};

#[derive(Debug, Clone, Serialize)]
pub(super) struct WaterfallView {
    summary: WaterfallSummary,
    spans: Vec<WaterfallSpan>,
}

#[derive(Debug, Clone, Serialize)]
struct WaterfallSummary {
    span_count: usize,
    trace_start_seconds: Option<f64>,
    trace_end_seconds: Option<f64>,
    trace_duration_seconds: Option<f64>,
    metrics: WaterfallMetrics,
}

#[derive(Debug, Clone, Serialize)]
struct WaterfallSpan {
    row_id: String,
    span_id: String,
    root_span_id: String,
    parent_span_id: Option<String>,
    depth: usize,
    name: String,
    span_type: String,
    model: Option<String>,
    has_error: bool,
    offset_seconds: Option<f64>,
    start_seconds: Option<f64>,
    end_seconds: Option<f64>,
    duration_seconds: Option<f64>,
    metrics: WaterfallMetrics,
}

#[derive(Debug, Clone, Default, Serialize)]
struct WaterfallMetrics {
    total_tokens: Option<u64>,
    prompt_tokens: Option<u64>,
    completion_tokens: Option<u64>,
    cached_prompt_tokens: Option<u64>,
    uncached_prompt_tokens: Option<u64>,
    cache_write_tokens: Option<u64>,
    cache_hit_percent: Option<f64>,
    estimated_cost: Option<f64>,
    time_to_first_token_seconds: Option<f64>,
}

pub(super) fn build_waterfall_view(
    rows: Vec<Map<String, Value>>,
    root_span_id: &str,
) -> WaterfallView {
    let entries = build_span_entries(rows);
    let trace_start_seconds = entries
        .iter()
        .filter_map(|span| extract_start_time(&span.row))
        .fold(None, |best: Option<f64>, value| match best {
            Some(best) if best <= value => Some(best),
            _ => Some(value),
        });
    let trace_end_seconds = entries
        .iter()
        .filter_map(|span| extract_span_end_seconds(&span.row))
        .fold(None, |best: Option<f64>, value| match best {
            Some(best) if best >= value => Some(best),
            _ => Some(value),
        });
    let trace_duration_seconds = match (trace_start_seconds, trace_end_seconds) {
        (Some(start), Some(end)) if end >= start => Some(end - start),
        _ => entries
            .iter()
            .find(|span| span.span_id == root_span_id)
            .and_then(|span| extract_duration_seconds(span.row.get("metrics"))),
    };

    let spans = entries
        .into_iter()
        .map(|entry| {
            let start_seconds = extract_start_time(&entry.row);
            let duration_seconds = extract_duration_seconds(entry.row.get("metrics"));
            let end_seconds = extract_span_end_seconds(&entry.row);
            let offset_seconds = match (start_seconds, trace_start_seconds) {
                (Some(start), Some(trace_start)) if start >= trace_start => {
                    Some(start - trace_start)
                }
                _ => None,
            };
            let (name, span_type) = extract_span_name_and_type(&entry.row, &entry.span_id);

            WaterfallSpan {
                row_id: entry.id,
                span_id: entry.span_id,
                root_span_id: entry.root_span_id,
                parent_span_id: extract_parent_span_id(&entry.row),
                depth: entry.depth,
                name,
                span_type,
                model: extract_model_name(&entry.row),
                has_error: span_has_error(&entry.row),
                offset_seconds,
                start_seconds,
                end_seconds,
                duration_seconds,
                metrics: extract_waterfall_metrics(&entry.row),
            }
        })
        .collect::<Vec<_>>();

    let root_metrics = spans
        .iter()
        .find(|span| span.span_id == root_span_id)
        .map(|span| span.metrics.clone())
        .unwrap_or_default();
    let aggregate_metrics = aggregate_waterfall_metrics(&spans);
    let metrics = prefer_waterfall_metrics(root_metrics, aggregate_metrics);

    WaterfallView {
        summary: WaterfallSummary {
            span_count: spans.len(),
            trace_start_seconds,
            trace_end_seconds,
            trace_duration_seconds,
            metrics,
        },
        spans,
    }
}

pub(super) fn print_waterfall_text(
    target: &ResolvedTraceCommandTarget,
    waterfall: &WaterfallView,
    profile: Option<&str>,
    limit: usize,
    has_more: bool,
) {
    println!(
        "bt view waterfall: project={} root_span_id={} spans={}",
        project_label(&target.project),
        target.root_span_id,
        waterfall.summary.span_count,
    );
    if let Some(span_id) = target.span_id.as_deref() {
        if let Some((idx, span)) = waterfall
            .spans
            .iter()
            .enumerate()
            .find(|(_, span)| span.span_id == span_id || span.row_id == span_id)
        {
            println!("selected: row {} id={}", idx + 1, span.row_id);
        }
    }

    println!(
        "summary: duration={} {}",
        format_duration_value(waterfall.summary.trace_duration_seconds),
        format_waterfall_metrics(&waterfall.summary.metrics)
    );
    print_waterfall_outliers(waterfall);

    println!("\nspans:");
    let selected_span = target.span_id.as_deref();
    for (idx, span) in waterfall.spans.iter().enumerate() {
        let selected_marker = if selected_span
            .is_some_and(|selected| selected == span.span_id || selected == span.row_id)
        {
            "*"
        } else {
            ""
        };
        println!(
            "  row={}{} id={} offset={} duration={} span={} {}",
            idx + 1,
            selected_marker,
            span.row_id,
            format_offset(span.offset_seconds),
            format_duration_value(span.duration_seconds),
            json_string(&format_waterfall_span_label(span)),
            format_waterfall_metrics(&span.metrics),
        );
    }

    if has_more {
        println!(
            "\nTrace has more spans than --limit {limit}; increase --limit for a complete waterfall."
        );
    }
    println!(
        "\nspan detail: bt view span{} --object-ref {} --id <id>",
        profile_flag_suffix(profile),
        format_object_ref_arg(&target.object_ref)
    );
    println!("Use the inline `id=` value from a span row.");
    println!(
        "json: bt view waterfall --json{} --trace-id {}",
        profile_flag_suffix(profile),
        target.root_span_id
    );
}

fn print_waterfall_outliers(waterfall: &WaterfallView) {
    let candidates = waterfall_outlier_candidates(waterfall);
    println!("outliers:");

    if let Some((idx, span)) = max_waterfall_span_by(&candidates, |span| span.duration_seconds) {
        println!("  slowest: {}", format_waterfall_outlier(idx, span));
    } else {
        println!("  slowest: none");
    }

    if let Some((idx, span)) =
        max_waterfall_span_by(&candidates, |span| span.metrics.estimated_cost)
    {
        println!("  highest_cost: {}", format_waterfall_outlier(idx, span));
    } else {
        println!("  highest_cost: none");
    }

    if let Some((idx, span)) = max_waterfall_span_by(&candidates, |span| {
        span.metrics.total_tokens.map(|tokens| tokens as f64)
    }) {
        println!("  highest_tokens: {}", format_waterfall_outlier(idx, span));
    } else {
        println!("  highest_tokens: none");
    }

    let llm_candidates = candidates
        .iter()
        .copied()
        .filter(|(_, span)| {
            span.span_type.eq_ignore_ascii_case("llm")
                && span.metrics.prompt_tokens.unwrap_or(0) > 0
                && span.metrics.cache_hit_percent.is_some()
        })
        .collect::<Vec<_>>();
    if let Some((idx, span)) =
        min_waterfall_span_by(&llm_candidates, |span| span.metrics.cache_hit_percent)
    {
        println!("  lowest_cache: {}", format_waterfall_outlier(idx, span));
    } else {
        println!("  lowest_cache: none");
    }

    let errors = waterfall
        .spans
        .iter()
        .enumerate()
        .filter(|(_, span)| span.has_error)
        .take(3)
        .collect::<Vec<_>>();
    if errors.is_empty() {
        println!("  errors: none");
    } else {
        for (idx, span) in errors {
            println!("  error: {}", format_waterfall_outlier(idx, span));
        }
    }
}

fn waterfall_outlier_candidates(waterfall: &WaterfallView) -> Vec<(usize, &WaterfallSpan)> {
    waterfall
        .spans
        .iter()
        .enumerate()
        .filter(|(_, span)| {
            !is_waterfall_wrapper_span(span, waterfall.summary.trace_duration_seconds)
        })
        .collect()
}

fn is_waterfall_wrapper_span(span: &WaterfallSpan, trace_duration_seconds: Option<f64>) -> bool {
    if span.span_id == span.root_span_id {
        return true;
    }

    matches!(
        (span.duration_seconds, trace_duration_seconds),
        (Some(duration), Some(trace_duration))
            if span.depth <= 1 && trace_duration > 0.0 && duration >= trace_duration * 0.95
    )
}

fn max_waterfall_span_by<'a, F>(
    candidates: &[(usize, &'a WaterfallSpan)],
    value_fn: F,
) -> Option<(usize, &'a WaterfallSpan)>
where
    F: Fn(&WaterfallSpan) -> Option<f64>,
{
    candidates
        .iter()
        .filter_map(|(idx, span)| {
            value_fn(span)
                .filter(|value| value.is_finite())
                .map(|value| (*idx, *span, value))
        })
        .max_by(|left, right| left.2.partial_cmp(&right.2).unwrap_or(Ordering::Equal))
        .map(|(idx, span, _)| (idx, span))
}

fn min_waterfall_span_by<'a, F>(
    candidates: &[(usize, &'a WaterfallSpan)],
    value_fn: F,
) -> Option<(usize, &'a WaterfallSpan)>
where
    F: Fn(&WaterfallSpan) -> Option<f64>,
{
    candidates
        .iter()
        .filter_map(|(idx, span)| {
            value_fn(span)
                .filter(|value| value.is_finite())
                .map(|value| (*idx, *span, value))
        })
        .min_by(|left, right| left.2.partial_cmp(&right.2).unwrap_or(Ordering::Equal))
        .map(|(idx, span, _)| (idx, span))
}

fn format_waterfall_outlier(idx: usize, span: &WaterfallSpan) -> String {
    format!(
        "row={} id={} offset={} duration={} span={} {}",
        idx + 1,
        span.row_id,
        format_offset(span.offset_seconds),
        format_duration_value(span.duration_seconds),
        json_string(&format_waterfall_span_label(span)),
        format_waterfall_metrics(&span.metrics),
    )
}

fn extract_waterfall_metrics(row: &Map<String, Value>) -> WaterfallMetrics {
    let metrics = value_as_object_owned(row.get("metrics")).unwrap_or_default();
    let prompt_tokens = parse_first_u64ish(&metrics, &["prompt_tokens", "input_tokens"]);
    let completion_tokens = parse_first_u64ish(&metrics, &["completion_tokens", "output_tokens"]);
    let total_tokens = parse_first_u64ish(&metrics, &["total_tokens", "tokens"])
        .or_else(|| Some(prompt_tokens?.saturating_add(completion_tokens?)));
    let cached_prompt_tokens = parse_first_u64ish(
        &metrics,
        &[
            "prompt_cached_tokens",
            "cached_prompt_tokens",
            "cache_read_input_tokens",
            "prompt_cache_read_tokens",
            "input_cached_tokens",
        ],
    );
    let uncached_prompt_tokens = match (prompt_tokens, cached_prompt_tokens) {
        (Some(prompt), Some(cached)) => Some(prompt.saturating_sub(cached)),
        _ => None,
    };
    let cache_write_tokens = parse_first_u64ish(
        &metrics,
        &[
            "cache_creation_input_tokens",
            "prompt_cache_creation_tokens",
            "cache_write_input_tokens",
        ],
    );
    let cache_hit_percent = parse_first_f64ish(
        &metrics,
        &[
            "prompt_cache_hit_percent",
            "prompt_cache_hit_percentage",
            "cache_hit_percent",
            "cache_hit_percentage",
            "prompt_cache_hit_rate",
            "cache_hit_rate",
        ],
    )
    .and_then(normalize_cache_hit_percent)
    .or_else(|| derive_cache_hit_percent(cached_prompt_tokens, prompt_tokens));
    let estimated_cost = parse_first_f64ish(&metrics, &["estimated_cost", "cost"])
        .filter(|value| value.is_finite() && *value >= 0.0);
    let time_to_first_token_seconds = extract_ttft_seconds(&metrics);

    WaterfallMetrics {
        total_tokens,
        prompt_tokens,
        completion_tokens,
        cached_prompt_tokens,
        uncached_prompt_tokens,
        cache_write_tokens,
        cache_hit_percent,
        estimated_cost,
        time_to_first_token_seconds,
    }
}

fn aggregate_waterfall_metrics(spans: &[WaterfallSpan]) -> WaterfallMetrics {
    let total_tokens = sum_optional_u64(spans.iter().map(|span| span.metrics.total_tokens));
    let prompt_tokens = sum_optional_u64(spans.iter().map(|span| span.metrics.prompt_tokens));
    let completion_tokens =
        sum_optional_u64(spans.iter().map(|span| span.metrics.completion_tokens));
    let cached_prompt_tokens =
        sum_optional_u64(spans.iter().map(|span| span.metrics.cached_prompt_tokens));
    let uncached_prompt_tokens =
        sum_optional_u64(spans.iter().map(|span| span.metrics.uncached_prompt_tokens));
    let cache_write_tokens =
        sum_optional_u64(spans.iter().map(|span| span.metrics.cache_write_tokens));
    let estimated_cost = sum_optional_f64(spans.iter().map(|span| span.metrics.estimated_cost));
    let cache_hit_percent = derive_cache_hit_percent(cached_prompt_tokens, prompt_tokens);

    WaterfallMetrics {
        total_tokens,
        prompt_tokens,
        completion_tokens,
        cached_prompt_tokens,
        uncached_prompt_tokens,
        cache_write_tokens,
        cache_hit_percent,
        estimated_cost,
        time_to_first_token_seconds: None,
    }
}

fn prefer_waterfall_metrics(
    preferred: WaterfallMetrics,
    fallback: WaterfallMetrics,
) -> WaterfallMetrics {
    let cache_hit_percent = preferred
        .cache_hit_percent
        .or_else(|| {
            derive_cache_hit_percent(preferred.cached_prompt_tokens, preferred.prompt_tokens)
        })
        .or(fallback.cache_hit_percent);

    WaterfallMetrics {
        total_tokens: preferred.total_tokens.or(fallback.total_tokens),
        prompt_tokens: preferred.prompt_tokens.or(fallback.prompt_tokens),
        completion_tokens: preferred.completion_tokens.or(fallback.completion_tokens),
        cached_prompt_tokens: preferred
            .cached_prompt_tokens
            .or(fallback.cached_prompt_tokens),
        uncached_prompt_tokens: preferred
            .uncached_prompt_tokens
            .or(fallback.uncached_prompt_tokens),
        cache_write_tokens: preferred.cache_write_tokens.or(fallback.cache_write_tokens),
        cache_hit_percent,
        estimated_cost: preferred.estimated_cost.or(fallback.estimated_cost),
        time_to_first_token_seconds: preferred
            .time_to_first_token_seconds
            .or(fallback.time_to_first_token_seconds),
    }
}

fn extract_ttft_seconds(metrics: &Map<String, Value>) -> Option<f64> {
    let keys = [
        "time_to_first_token",
        "ttft",
        "time_to_first_token_seconds",
        "first_token_latency",
    ];
    keys.iter().find_map(|key| {
        parse_f64ish(metrics.get(*key)).filter(|&value| value.is_finite() && value >= 0.0)
    })
}

fn parse_first_u64ish(metrics: &Map<String, Value>, keys: &[&str]) -> Option<u64> {
    keys.iter().find_map(|key| parse_u64ish(metrics.get(*key)))
}

fn parse_first_f64ish(metrics: &Map<String, Value>, keys: &[&str]) -> Option<f64> {
    keys.iter().find_map(|key| parse_f64ish(metrics.get(*key)))
}

fn normalize_cache_hit_percent(value: f64) -> Option<f64> {
    if !value.is_finite() || value < 0.0 {
        return None;
    }
    let percent = if value <= 1.0 { value * 100.0 } else { value };
    Some(percent.min(100.0))
}

fn derive_cache_hit_percent(
    cached_prompt_tokens: Option<u64>,
    prompt_tokens: Option<u64>,
) -> Option<f64> {
    let cached = cached_prompt_tokens?;
    let prompt = prompt_tokens?;
    if prompt == 0 {
        return None;
    }
    Some(((cached as f64) / (prompt as f64) * 100.0).min(100.0))
}

fn sum_optional_u64(values: impl Iterator<Item = Option<u64>>) -> Option<u64> {
    let mut total = 0_u64;
    let mut found = false;
    for value in values.flatten() {
        total = total.saturating_add(value);
        found = true;
    }
    found.then_some(total)
}

fn sum_optional_f64(values: impl Iterator<Item = Option<f64>>) -> Option<f64> {
    let mut total = 0.0_f64;
    let mut found = false;
    for value in values.flatten().filter(|value| value.is_finite()) {
        total += value;
        found = true;
    }
    found.then_some(total)
}

fn format_offset(seconds: Option<f64>) -> String {
    seconds
        .map(|seconds| format!("+{}", format_compact_duration(seconds)))
        .unwrap_or_else(|| "-".to_string())
}

fn format_duration_value(seconds: Option<f64>) -> String {
    seconds
        .map(format_compact_duration)
        .unwrap_or_else(|| "-".to_string())
}

fn json_string(value: &str) -> String {
    serde_json::to_string(value).unwrap_or_else(|_| format!("{value:?}"))
}

fn format_waterfall_span_label(span: &WaterfallSpan) -> String {
    let label = if span.span_type.eq_ignore_ascii_case("llm") {
        match span
            .model
            .as_deref()
            .filter(|model| !model.trim().is_empty())
        {
            Some(model) => format!("llm {model}"),
            None if span.name.eq_ignore_ascii_case("llm") => "llm".to_string(),
            None => format!("llm {}", span.name),
        }
    } else if span.span_type == "span" {
        span.name.clone()
    } else {
        format!("{} [{}]", span.name, span.span_type)
    };

    if span.has_error {
        format!("{label} !")
    } else {
        label
    }
}

fn format_waterfall_metrics(metrics: &WaterfallMetrics) -> String {
    let mut parts = Vec::new();
    if let Some(total) = metrics.total_tokens {
        parts.push(format!("tokens={}", format_u64_with_commas(total)));
    }
    if let Some(prompt) = metrics.prompt_tokens {
        parts.push(format!("input={}", format_u64_with_commas(prompt)));
    }
    if let Some(completion) = metrics.completion_tokens {
        parts.push(format!("output={}", format_u64_with_commas(completion)));
    }

    match (metrics.cached_prompt_tokens, metrics.cache_hit_percent) {
        (Some(cached), Some(hit_percent)) => parts.push(format!(
            "cache={} cached={}",
            format_percent(hit_percent),
            format_u64_with_commas(cached)
        )),
        (Some(cached), None) => parts.push(format!("cached={}", format_u64_with_commas(cached))),
        (None, Some(hit_percent)) => parts.push(format!("cache={}", format_percent(hit_percent))),
        (None, None) => {}
    }
    if let Some(uncached) = metrics.uncached_prompt_tokens {
        if metrics.cached_prompt_tokens.is_none() && uncached > 0 {
            parts.push(format!("uncached={}", format_u64_with_commas(uncached)));
        }
    }
    if let Some(write_tokens) = metrics.cache_write_tokens {
        parts.push(format!(
            "cache_write={}",
            format_u64_with_commas(write_tokens)
        ));
    }
    if let Some(cost) = metrics.estimated_cost {
        parts.push(format!("cost={}", format_cost(cost)));
    }
    if let Some(ttft) = metrics.time_to_first_token_seconds {
        parts.push(format!("ttft={}", format_compact_duration(ttft)));
    }

    if parts.is_empty() {
        "metrics=none".to_string()
    } else {
        parts.join(" ")
    }
}

fn format_percent(value: f64) -> String {
    format!("{value:.1}%")
}

fn format_cost(cost: f64) -> String {
    if cost > 0.0 && cost < 0.001 {
        "<$0.001".to_string()
    } else if cost < 1.0 {
        format!("${cost:.3}")
    } else {
        format!("${cost:.2}")
    }
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Map};

    use super::{build_waterfall_view, extract_waterfall_metrics, is_waterfall_wrapper_span};

    #[test]
    fn waterfall_metrics_derive_cache_hit_and_track_cache_writes() {
        let mut row = Map::new();
        row.insert(
            "metrics".to_string(),
            json!({
                "prompt_tokens": 1000,
                "completion_tokens": 25,
                "prompt_cached_tokens": 940,
                "cache_creation_input_tokens": 30,
                "estimated_cost": 0.024,
                "time_to_first_token": 0.69
            }),
        );

        let metrics = extract_waterfall_metrics(&row);

        assert_eq!(metrics.total_tokens, Some(1025));
        assert_eq!(metrics.cached_prompt_tokens, Some(940));
        assert_eq!(metrics.uncached_prompt_tokens, Some(60));
        assert_eq!(metrics.cache_write_tokens, Some(30));
        assert_eq!(metrics.cache_hit_percent, Some(94.0));
        assert_eq!(metrics.estimated_cost, Some(0.024));
        assert_eq!(metrics.time_to_first_token_seconds, Some(0.69));
    }

    #[test]
    fn waterfall_metrics_prefer_explicit_cache_hit_rate() {
        let mut row = Map::new();
        row.insert(
            "metrics".to_string(),
            json!({
                "prompt_tokens": 1000,
                "prompt_cached_tokens": 100,
                "cache_hit_rate": 0.99
            }),
        );

        let metrics = extract_waterfall_metrics(&row);

        assert_eq!(metrics.cache_hit_percent, Some(99.0));
    }

    #[test]
    fn waterfall_view_builds_offsets() {
        let root = json!({
            "id": "row-root",
            "span_id": "root-span",
            "root_span_id": "root-span",
            "span_attributes": {"name": "session", "type": "task"},
            "metrics": {"start": 10.0, "duration": 10.0, "prompt_tokens": 100, "prompt_cached_tokens": 50}
        });
        let child = json!({
            "id": "row-child",
            "span_id": "child-span",
            "root_span_id": "root-span",
            "span_parents": ["root-span"],
            "span_attributes": {"name": "llm", "type": "llm"},
            "metadata": {"model": "test-model"},
            "metrics": {"start": 12.0, "duration": 4.0, "total_tokens": 42}
        });
        let rows = vec![
            root.as_object().expect("object").clone(),
            child.as_object().expect("object").clone(),
        ];

        let waterfall = build_waterfall_view(rows, "root-span");

        assert_eq!(waterfall.summary.span_count, 2);
        assert_eq!(waterfall.summary.trace_duration_seconds, Some(10.0));
        assert_eq!(waterfall.summary.metrics.cache_hit_percent, Some(50.0));
        assert_eq!(waterfall.spans[1].offset_seconds, Some(2.0));
        assert_eq!(waterfall.spans[1].duration_seconds, Some(4.0));
        assert!(is_waterfall_wrapper_span(
            &waterfall.spans[0],
            waterfall.summary.trace_duration_seconds
        ));
        assert!(!is_waterfall_wrapper_span(
            &waterfall.spans[1],
            waterfall.summary.trace_duration_seconds
        ));
    }
}
