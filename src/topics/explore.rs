use std::fmt::Write as _;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};

use crate::{
    traces::{run_interactive_project_log_trace_list, ProjectLogTraceSeed},
    ui::{
        apply_column_padding, fuzzy_select_opt, header, is_interactive, print_with_pager,
        styled_table, truncate, with_spinner,
    },
};

use super::{
    api::{self, TopicClassificationRow, TopicExploreFacet, TopicTraceRow},
    formatting::{format_count, format_project_header, format_timestamp_with_relative},
    ClassificationsArgs, ExploreArgs, ExploreTimeArgs, FacetsArgs, ResolvedContext,
    TopicTracesArgs,
};

pub async fn run_facets(ctx: &ResolvedContext, args: &FacetsArgs, json: bool) -> Result<()> {
    let filter_clause = resolve_filter_clause(ctx, &args.time, args.output.print_queries).await?;
    let report = with_spinner(
        "Loading facets and topic maps...",
        api::fetch_topics_explore_facets(
            ctx,
            args.automation_id.as_deref(),
            &filter_clause,
            args.output.print_queries,
        ),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_facets_report(&report))?;
    Ok(())
}

pub async fn run_classifications(
    ctx: &ResolvedContext,
    args: &ClassificationsArgs,
    json: bool,
) -> Result<()> {
    let filter_clause = resolve_filter_clause(ctx, &args.time, args.output.print_queries).await?;
    let report = with_spinner(
        "Loading topic labels...",
        api::fetch_topic_classifications(
            ctx,
            args.selection.automation_id.as_deref(),
            args.selection.facet.as_deref(),
            args.selection.topic_map.as_deref(),
            args.sort_limit.sort,
            args.sort_limit.limit,
            &filter_clause,
            args.output.print_queries,
        ),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_classifications_report(&report))?;
    Ok(())
}

pub async fn run_traces(ctx: &ResolvedContext, args: &TopicTracesArgs, json: bool) -> Result<()> {
    let filter_clause = resolve_filter_clause(ctx, &args.time, args.output.print_queries).await?;
    let report = with_spinner(
        "Loading traces for topic label...",
        api::fetch_topic_traces(
            ctx,
            args.selection.automation_id.as_deref(),
            args.selection.facet.as_deref(),
            args.selection.topic_map.as_deref(),
            args.topic.topic.as_deref(),
            args.topic.topic_id.as_deref(),
            args.sort_limit.sort.into(),
            args.sort_limit.limit,
            args.cursor.as_deref(),
            &filter_clause,
            args.output.print_queries,
        ),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_traces_report(&report))?;
    Ok(())
}

pub async fn run_explore(ctx: &ResolvedContext, args: &ExploreArgs, json: bool) -> Result<()> {
    if json {
        bail!("`bt topics explore` is interactive and does not support --json; use `bt topics facets`, `bt topics classifications`, or `bt topics traces` with --json");
    }
    if !is_interactive() {
        bail!("`bt topics explore` requires a TTY; use `bt topics facets`, `bt topics classifications`, and `bt topics traces` for non-interactive exploration");
    }
    if args.trace_page_size == 0 {
        bail!("--trace-page-size must be greater than 0");
    }

    let filter_clause = resolve_filter_clause(ctx, &args.time, args.output.print_queries).await?;
    let facets_report = with_spinner(
        "Loading facets and topic maps...",
        api::fetch_topics_explore_facets(
            ctx,
            args.selection.automation_id.as_deref(),
            &filter_clause,
            args.output.print_queries,
        ),
    )
    .await?;
    if facets_report.facets.is_empty() {
        bail!("no topic maps found; run `bt topics config` to inspect Topics setup");
    }

    let candidates = matching_explore_topic_maps(&facets_report.facets, args)?;
    let mut default_facet_index = 0usize;
    let mut force_facet_prompt = false;
    let mut users_cache = api::OrgUsersCache::default();

    while let Some((selected_map, selected_map_index)) =
        select_explore_topic_map(&candidates, default_facet_index, force_facet_prompt)?
    {
        default_facet_index = selected_map_index;
        let automation_id = Some(selected_map.automation_id.as_str());
        let facet = selected_map.facet.as_deref();
        let topic_map = Some(selected_map.topic_map_id.as_str());

        let classifications_report = with_spinner(
            "Loading topic labels...",
            api::fetch_topic_classifications(
                ctx,
                automation_id,
                facet,
                topic_map,
                args.sort_limit.sort,
                args.sort_limit.limit,
                &filter_clause,
                args.output.print_queries,
            ),
        )
        .await?;
        if classifications_report.classifications.is_empty() {
            bail!("no topic labels found for the selected topic map in this time window");
        }

        let classification_labels = classifications_report
            .classifications
            .iter()
            .map(format_classification_choice)
            .collect::<Vec<_>>();
        let mut default_classification_index = 0usize;

        loop {
            let Some(classification_index) = fuzzy_select_opt(
                "Select topic label: label / traces / cost / tokens / avg cost / id (Esc to facets)",
                &classification_labels,
                default_classification_index.min(classification_labels.len().saturating_sub(1)),
            )?
            else {
                force_facet_prompt = true;
                break;
            };
            default_classification_index = classification_index;
            let selected_classification = classifications_report
                .classifications
                .get(classification_index)
                .expect("selected classification");

            let topic_id = (!selected_classification.topic_id.is_empty())
                .then_some(selected_classification.topic_id.as_str());
            let topic = topic_id
                .is_none()
                .then_some(selected_classification.topic.as_str());

            let mut traces_report = with_spinner(
                "Loading matching traces...",
                api::fetch_topic_traces_with_user_cache(
                    ctx,
                    automation_id,
                    facet,
                    topic_map,
                    topic,
                    topic_id,
                    args.sort_limit.sort,
                    args.trace_page_size,
                    None,
                    &filter_clause,
                    args.output.print_queries,
                    &mut users_cache,
                ),
            )
            .await?;
            if traces_report.traces.is_empty() {
                eprintln!(
                    "No traces found for the selected topic label in this time window. Select another topic label."
                );
                continue;
            }

            let topic_selection = ExploreTraceSelection {
                automation_id,
                facet,
                topic_map,
                topic,
                topic_id,
                filter_clause: &filter_clause,
            };
            run_trace_picker(
                ctx,
                args,
                topic_selection,
                &mut traces_report,
                &mut users_cache,
            )
            .await?;
        }
    }

    Ok(())
}

async fn resolve_filter_clause(
    ctx: &ResolvedContext,
    time: &ExploreTimeArgs,
    print_queries: bool,
) -> Result<String> {
    let filter = api::topic_explore_filter_clause(
        ctx,
        time.since.as_deref(),
        &time.window,
        time.filter.as_deref(),
        time.repo.as_deref(),
        print_queries,
    );
    if time
        .repo
        .as_deref()
        .map(str::trim)
        .is_some_and(|repo| !repo.is_empty())
    {
        with_spinner("Resolving repo filter...", filter).await
    } else {
        filter.await
    }
}

async fn run_trace_picker(
    ctx: &ResolvedContext,
    args: &ExploreArgs,
    selection: ExploreTraceSelection<'_>,
    traces_report: &mut api::TopicTracesReport,
    users_cache: &mut api::OrgUsersCache,
) -> Result<()> {
    let mut default_trace_index = 0usize;

    loop {
        let trace_labels = trace_picker_labels(traces_report);
        let Some(trace_index) = fuzzy_select_opt(
            "Select trace: created / user / repo / cost / tokens / duration / root / input (Esc to topic labels)",
            &trace_labels,
            default_trace_index.min(trace_labels.len().saturating_sub(1)),
        )? else {
            break;
        };

        if trace_index == traces_report.traces.len() && traces_report.next_cursor.is_some() {
            let previous_len = traces_report.traces.len();
            load_more_traces(ctx, args, selection, traces_report, users_cache).await?;
            default_trace_index = previous_len.min(traces_report.traces.len().saturating_sub(1));
            continue;
        }

        default_trace_index = trace_index;
        let trace = traces_report
            .traces
            .get(trace_index)
            .expect("selected trace");
        if trace.root_span_id.is_empty() {
            println!("{}", render_selected_trace(trace));
            continue;
        }

        let trace_seeds = traces_report
            .traces
            .iter()
            .map(trace_viewer_seed)
            .collect::<Vec<_>>();
        run_interactive_project_log_trace_list(
            ctx.client.clone(),
            &ctx.project.id,
            Some(&ctx.project.name),
            trace_seeds.clone(),
            &trace.root_span_id,
            args.output.print_queries,
        )
        .await?;
    }

    Ok(())
}

#[derive(Clone, Copy)]
struct ExploreTraceSelection<'a> {
    automation_id: Option<&'a str>,
    facet: Option<&'a str>,
    topic_map: Option<&'a str>,
    topic: Option<&'a str>,
    topic_id: Option<&'a str>,
    filter_clause: &'a str,
}

async fn load_more_traces(
    ctx: &ResolvedContext,
    args: &ExploreArgs,
    selection: ExploreTraceSelection<'_>,
    traces_report: &mut api::TopicTracesReport,
    users_cache: &mut api::OrgUsersCache,
) -> Result<()> {
    let Some(cursor) = traces_report.next_cursor.clone() else {
        return Ok(());
    };

    let next_report = with_spinner(
        "Loading more traces...",
        api::fetch_topic_traces_with_user_cache(
            ctx,
            selection.automation_id,
            selection.facet,
            selection.topic_map,
            selection.topic,
            selection.topic_id,
            args.sort_limit.sort,
            args.trace_page_size,
            Some(&cursor),
            selection.filter_clause,
            args.output.print_queries,
            users_cache,
        ),
    )
    .await?;

    traces_report.traces.extend(next_report.traces);
    traces_report.next_cursor = next_report.next_cursor;
    Ok(())
}

fn matching_explore_topic_maps(
    rows: &[TopicExploreFacet],
    args: &ExploreArgs,
) -> Result<Vec<TopicExploreFacet>> {
    let candidates = rows
        .iter()
        .filter(|row| {
            optional_selector_matches(
                Some(row.automation_id.as_str()),
                args.selection.automation_id.as_deref(),
            )
        })
        .filter(|row| {
            optional_selector_matches(
                Some(row.facet.as_deref().unwrap_or("Ungrouped")),
                args.selection.facet.as_deref(),
            )
        })
        .filter(|row| match args.selection.topic_map.as_deref() {
            Some(selector) => {
                selector_matches(&row.topic_map, selector)
                    || selector_matches(&row.topic_map_id, selector)
            }
            None => true,
        })
        .cloned()
        .collect::<Vec<_>>();

    if candidates.is_empty() {
        bail!(
            "topic map selection did not match any configured topic map; run `bt topics facets` to list available facets and topic maps"
        );
    }

    Ok(candidates)
}

fn select_explore_topic_map(
    candidates: &[TopicExploreFacet],
    default_index: usize,
    force_prompt: bool,
) -> Result<Option<(TopicExploreFacet, usize)>> {
    match candidates.len() {
        0 => bail!("no topic maps to select from"),
        1 if !force_prompt => Ok(Some((
            candidates
                .first()
                .cloned()
                .expect("single topic map candidate"),
            0,
        ))),
        _ => {
            let labels = candidates
                .iter()
                .map(format_facet_choice)
                .collect::<Vec<_>>();
            let Some(index) = fuzzy_select_opt(
                "Select facet/topic map: facet / topic map / labeled / eligible / errors / id (Esc to exit)",
                &labels,
                default_index.min(labels.len().saturating_sub(1)),
            )?
            else {
                return Ok(None);
            };
            Ok(Some((
                candidates
                    .get(index)
                    .cloned()
                    .expect("selected topic map candidate"),
                index,
            )))
        }
    }
}

fn optional_selector_matches(candidate: Option<&str>, selector: Option<&str>) -> bool {
    match selector {
        Some(selector) => candidate
            .map(|candidate| selector_matches(candidate, selector))
            .unwrap_or(false),
        None => true,
    }
}

fn selector_matches(candidate: &str, selector: &str) -> bool {
    candidate == selector || candidate.eq_ignore_ascii_case(selector)
}

fn render_facets_report(report: &api::TopicsExploreFacetsReport) -> String {
    let mut output = format_project_header(
        &report.project.name,
        &report.project.id,
        &report.project.org_name,
    );
    output.push('\n');

    if report.facets.is_empty() {
        output.push_str("\nNo topic maps found. Run `bt topics config` to inspect Topics setup.\n");
        return output;
    }

    writeln!(
        output,
        "{} facet/topic map rows found.",
        format_count(report.facets.len())
    )
    .expect("write to string");

    let mut table = styled_table();
    table.set_header(vec![
        header("Facet"),
        header("Topic map"),
        header("Topic map ID"),
        header("Version"),
        header("Eligible"),
        header("Labeled"),
        header("Processing"),
        header("Errors"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for row in &report.facets {
        table.add_row(vec![
            row.facet.as_deref().unwrap_or("Ungrouped").to_string(),
            row.topic_map.clone(),
            row.topic_map_id.clone(),
            row.version.as_deref().unwrap_or("-").to_string(),
            format_count(row.eligible),
            format_count(row.labeled),
            format_count(row.processing),
            format_count(row.errors),
        ]);
    }

    writeln!(output, "\n{table}").expect("write to string");
    output
}

fn render_classifications_report(report: &api::TopicClassificationsReport) -> String {
    let mut output = format_project_header(
        &report.project.name,
        &report.project.id,
        &report.project.org_name,
    );
    writeln!(
        output,
        "\nTopic map: {} / {} ({})",
        report.topic_map.facet.as_deref().unwrap_or("Ungrouped"),
        report.topic_map.topic_map,
        report.topic_map.topic_map_id
    )
    .expect("write to string");

    if report.classifications.is_empty() {
        output.push_str("\nNo topic labels found in this time window.\n");
        return output;
    }

    writeln!(
        output,
        "{} topic labels found.",
        format_count(report.classifications.len())
    )
    .expect("write to string");

    let mut table = styled_table();
    table.set_header(vec![
        header("Topic label"),
        header("Topic ID"),
        header("Traces"),
        header("Tokens"),
        header("Cost"),
        header("Avg tokens"),
        header("Avg cost"),
        header("Latest"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for row in &report.classifications {
        table.add_row(vec![
            truncate(&row.topic, 36),
            truncate(&row.topic_id, 28),
            format_count(row.traces),
            format_tokens(row.tokens),
            format_cost(row.cost),
            format_tokens(row.avg_tokens),
            format_cost(row.avg_cost),
            row.latest
                .as_deref()
                .map(format_timestamp_with_relative)
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }

    writeln!(output, "\n{table}").expect("write to string");
    output
}

fn render_traces_report(report: &api::TopicTracesReport) -> String {
    let mut output = format_project_header(
        &report.project.name,
        &report.project.id,
        &report.project.org_name,
    );
    writeln!(
        output,
        "\nTopic map: {} / {} ({})",
        report.topic_map.facet.as_deref().unwrap_or("Ungrouped"),
        report.topic_map.topic_map,
        report.topic_map.topic_map_id
    )
    .expect("write to string");

    if report.traces.is_empty() {
        output.push_str("\nNo traces found in this time window.\n");
        return output;
    }

    writeln!(
        output,
        "Showing {} traces.",
        format_count(report.traces.len())
    )
    .expect("write to string");

    let mut table = styled_table();
    table.set_header(vec![
        header("Created"),
        header("Root span ID"),
        header("User"),
        header("Repo"),
        header("Topic"),
        header("Tokens"),
        header("Cost"),
        header("Duration"),
        header("Input"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for row in &report.traces {
        table.add_row(vec![
            row.created
                .as_deref()
                .map(format_timestamp_with_relative)
                .unwrap_or_else(|| "-".to_string()),
            truncate(&row.root_span_id, 24),
            trace_user_label(row)
                .map(|user| truncate(&user, 28))
                .unwrap_or_else(|| "-".to_string()),
            trace_repo_label(row)
                .map(|repo| truncate(&repo, 28))
                .unwrap_or_else(|| "-".to_string()),
            truncate(row.topic.as_deref().unwrap_or("-"), 28),
            format_tokens(row.tokens),
            format_cost(row.cost),
            format_duration(row.duration_seconds),
            row.input
                .as_deref()
                .map(|input| truncate(input, 70))
                .unwrap_or_else(|| "-".to_string()),
        ]);
    }

    writeln!(output, "\n{table}").expect("write to string");
    if let Some(cursor) = report.next_cursor.as_deref() {
        writeln!(
            output,
            "\nNext cursor: {cursor}\nUse `bt topics traces --cursor <next_cursor>` with the same topic filters to fetch more traces."
        )
        .expect("write to string");
    }
    output
}

fn trace_picker_labels(report: &api::TopicTracesReport) -> Vec<String> {
    let mut labels = report
        .traces
        .iter()
        .map(format_trace_choice)
        .collect::<Vec<_>>();
    if report.next_cursor.is_some() {
        labels.push(format!(
            "{}  {}  {}  {}  {}  {}  {}  {}",
            left_cell("Load more traces", 16),
            left_cell("", 24),
            left_cell("", 24),
            right_cell("", 9),
            right_cell("", 10),
            right_cell("", 8),
            left_cell("", 24),
            left_cell("", 60),
        ));
    }
    labels
}

fn format_facet_choice(row: &TopicExploreFacet) -> String {
    format!(
        "{}  {}  {}  {}  {}  {}",
        left_cell(row.facet.as_deref().unwrap_or("Ungrouped"), 18),
        left_cell(&row.topic_map, 28),
        right_cell(&format_count(row.labeled), 8),
        right_cell(&format_count(row.eligible), 8),
        right_cell(&format_count(row.errors), 6),
        row.topic_map_id
    )
}

fn format_classification_choice(row: &TopicClassificationRow) -> String {
    format!(
        "{}  {}  {}  {}  {}  {}",
        left_cell(&row.topic, 34),
        right_cell(&format_count(row.traces), 8),
        right_cell(&format_cost(row.cost), 9),
        right_cell(&format_tokens(row.tokens), 10),
        right_cell(&format_cost(row.avg_cost), 9),
        row.topic_id
    )
}

fn format_trace_choice(row: &TopicTraceRow) -> String {
    format!(
        "{}  {}  {}  {}  {}  {}  {}  {}",
        left_cell(&format_compact_timestamp(row.created.as_deref()), 16),
        left_cell(
            &trace_user_label(row).unwrap_or_else(|| "-".to_string()),
            24
        ),
        left_cell(
            &trace_repo_label(row).unwrap_or_else(|| "-".to_string()),
            24
        ),
        right_cell(&format_cost(row.cost), 9),
        right_cell(&format_tokens(row.tokens), 10),
        right_cell(&format_duration(row.duration_seconds), 8),
        left_cell(&row.root_span_id, 24),
        row.input
            .as_deref()
            .map(|input| truncate(input, 60))
            .unwrap_or_else(|| "-".to_string())
    )
}

fn left_cell(value: &str, width: usize) -> String {
    let value = truncate(value, width);
    format!("{value:<width$}")
}

fn right_cell(value: &str, width: usize) -> String {
    let value = truncate(value, width);
    format!("{value:>width$}")
}

fn format_compact_timestamp(value: Option<&str>) -> String {
    let Some(value) = value else {
        return "-".to_string();
    };
    let Ok(parsed) = DateTime::parse_from_rfc3339(value) else {
        return truncate(value, 16);
    };
    parsed
        .with_timezone(&Utc)
        .format("%Y-%m-%d %H:%M")
        .to_string()
}

fn trace_viewer_seed(row: &TopicTraceRow) -> ProjectLogTraceSeed {
    ProjectLogTraceSeed {
        created: row.created.clone(),
        root_span_id: row.root_span_id.clone(),
        span_id: row.span_id.clone(),
        row_id: row.row_id.clone(),
        input: row.input.clone(),
        duration_seconds: row.duration_seconds,
        total_tokens: row.tokens,
        estimated_cost: row.cost,
    }
}

fn render_selected_trace(trace: &TopicTraceRow) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "Selected trace: {}",
        if trace.root_span_id.is_empty() {
            "<missing-root-span-id>"
        } else {
            &trace.root_span_id
        }
    )
    .expect("write to string");
    if let Some(topic) = trace.topic.as_deref() {
        writeln!(output, "topic: {topic}").expect("write to string");
    }
    if let Some(created) = trace.created.as_deref() {
        writeln!(
            output,
            "created: {}",
            format_timestamp_with_relative(created)
        )
        .expect("write to string");
    }
    if let Some(user) = trace_user_label_with_id(trace) {
        writeln!(output, "user: {user}").expect("write to string");
    }
    if let Some(repo) = trace_repo_label(trace) {
        writeln!(output, "repo: {repo}").expect("write to string");
    }
    if let Some(origin) = trace.git_origin_url.as_deref() {
        writeln!(output, "origin: {origin}").expect("write to string");
    }
    writeln!(output, "url: {}", trace.app_url).expect("write to string");
    if !trace.root_span_id.is_empty() {
        writeln!(output).expect("write to string");
        writeln!(output, "bt view trace --trace-id {}", trace.root_span_id)
            .expect("write to string");
        writeln!(output, "bt view thread --trace-id {}", trace.root_span_id)
            .expect("write to string");
        writeln!(
            output,
            "bt view waterfall --trace-id {}",
            trace.root_span_id
        )
        .expect("write to string");
    }
    output
}

fn trace_user_label(row: &TopicTraceRow) -> Option<String> {
    row.created_by_user_name
        .as_deref()
        .or(row.created_by_user_email.as_deref())
        .or(row.created_by_user_id.as_deref())
        .map(ToString::to_string)
}

fn trace_user_label_with_id(row: &TopicTraceRow) -> Option<String> {
    let label = trace_user_label(row)?;
    let Some(user_id) = row.created_by_user_id.as_deref() else {
        return Some(label);
    };
    if label == user_id {
        Some(label)
    } else {
        Some(format!("{label} ({user_id})"))
    }
}

fn trace_repo_label(row: &TopicTraceRow) -> Option<String> {
    row.repo
        .as_deref()
        .or(row.git_origin_url.as_deref())
        .map(ToString::to_string)
}

fn format_tokens(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "-".to_string();
    }
    format_count(value.round() as usize)
}

fn format_cost(value: f64) -> String {
    if !value.is_finite() || value <= 0.0 {
        return "-".to_string();
    }
    if value < 0.001 {
        return "<$0.001".to_string();
    }
    if value < 1.0 {
        return format!("${value:.3}");
    }
    format!("${value:.2}")
}

fn format_duration(seconds: Option<f64>) -> String {
    let Some(seconds) = seconds.filter(|seconds| seconds.is_finite() && *seconds >= 0.0) else {
        return "-".to_string();
    };
    if seconds < 1.0 {
        return format!("{:.0}ms", seconds * 1000.0);
    }
    if seconds < 60.0 {
        return format!("{seconds:.1}s");
    }
    let minutes = (seconds / 60.0).floor();
    let remainder = seconds % 60.0;
    format!("{minutes:.0}m {remainder:.0}s")
}
