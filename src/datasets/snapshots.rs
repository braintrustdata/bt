use std::{fmt::Write as _, time::Duration};

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use clap::{builder::BoolishValueParser, Args, Subcommand};
use dialoguer::{console, Confirm};
use serde_json::json;

use crate::{
    args::BaseArgs,
    ui::{
        apply_column_padding, header, is_interactive, print_command_status, print_with_pager,
        styled_table, truncate, with_spinner, with_spinner_visible, CommandStatus,
    },
    utils::{pluralize, profile_author_slug, resolve_profile_info, sanitize_name_segment},
};

use super::{
    api::{self, Dataset, DatasetRestorePreview, DatasetRestoreResult, DatasetSnapshot},
    ResolvedContext,
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt datasets snapshots list my-dataset
  bt datasets snapshots create my-dataset
  bt datasets snapshots create my-dataset baseline
  bt datasets snapshots create my-dataset baseline --xact-id 1000192656880881099
  bt datasets snapshots restore my-dataset
  bt datasets snapshots restore my-dataset --name baseline
  bt datasets snapshots restore my-dataset --snapshot 1000192656880881099 --force
")]
pub(super) struct SnapshotsArgs {
    #[command(subcommand)]
    pub(super) command: SnapshotsCommands,
}

#[derive(Debug, Clone, Subcommand)]
pub(super) enum SnapshotsCommands {
    /// List snapshots for a dataset
    List(SnapshotListArgs),
    /// Create a new snapshot for a dataset
    Create(SnapshotCreateArgs),
    /// Restore a dataset to a saved snapshot
    Restore(SnapshotRestoreArgs),
}

#[derive(Debug, Clone, Args)]
pub(super) struct SnapshotDatasetArgs {
    /// Dataset name (positional)
    #[arg(value_name = "DATASET")]
    pub(super) dataset_positional: Option<String>,

    /// Dataset name (flag)
    #[arg(long = "dataset", short = 'd')]
    pub(super) dataset_flag: Option<String>,
}

impl SnapshotDatasetArgs {
    pub(super) fn dataset_name(&self) -> Option<&str> {
        self.dataset_positional
            .as_deref()
            .or(self.dataset_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub(super) struct SnapshotNameArgs {
    /// Snapshot name (positional)
    #[arg(value_name = "SNAPSHOT")]
    pub(super) name_positional: Option<String>,

    /// Snapshot name (flag)
    #[arg(long = "name", short = 'n')]
    pub(super) name_flag: Option<String>,
}

impl SnapshotNameArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub(super) struct SnapshotListArgs {
    #[command(flatten)]
    pub(super) dataset: SnapshotDatasetArgs,
}

impl SnapshotListArgs {
    pub(super) fn dataset_name(&self) -> Option<&str> {
        self.dataset.dataset_name()
    }
}

#[derive(Debug, Clone, Args)]
pub(super) struct SnapshotCreateArgs {
    #[command(flatten)]
    pub(super) dataset: SnapshotDatasetArgs,

    #[command(flatten)]
    pub(super) snapshot: SnapshotNameArgs,

    /// Transaction id to snapshot. Defaults to the dataset's current head xact.
    #[arg(
        long = "xact-id",
        env = "BT_DATASETS_SNAPSHOT_XACT_ID",
        value_name = "XACT_ID"
    )]
    pub(super) xact_id: Option<String>,

    /// Optional snapshot description
    #[arg(long, env = "BT_DATASETS_SNAPSHOT_DESCRIPTION", value_name = "TEXT")]
    pub(super) description: Option<String>,
}

impl SnapshotCreateArgs {
    pub(super) fn dataset_name(&self) -> Option<&str> {
        self.dataset.dataset_name()
    }

    pub(super) fn snapshot_name(&self) -> Option<&str> {
        self.snapshot.name()
    }
}

#[derive(Debug, Clone, Args)]
pub(super) struct SnapshotRestoreArgs {
    #[command(flatten)]
    pub(super) dataset: SnapshotDatasetArgs,

    /// Saved snapshot name to restore
    #[arg(
        long,
        short = 'n',
        env = "BT_DATASETS_SNAPSHOT_RESTORE_NAME",
        value_name = "NAME",
        conflicts_with = "snapshot"
    )]
    pub(super) name: Option<String>,

    /// Transaction id to restore
    #[arg(
        long = "snapshot",
        visible_alias = "version",
        env = "BT_DATASETS_SNAPSHOT_RESTORE_XACT_ID",
        value_name = "XACT_ID",
        conflicts_with = "name"
    )]
    pub(super) snapshot: Option<String>,

    /// Skip confirmation after preview and apply the restore
    #[arg(
        long,
        short = 'f',
        env = "BT_DATASETS_SNAPSHOT_RESTORE_FORCE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    pub(super) force: bool,
}

impl SnapshotRestoreArgs {
    pub(super) fn dataset_name(&self) -> Option<&str> {
        self.dataset.dataset_name()
    }

    pub(super) fn snapshot_name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    pub(super) fn snapshot_xact_id(&self) -> Option<&str> {
        self.snapshot.as_deref()
    }
}

pub(super) fn command_is_read_only(args: &SnapshotsArgs) -> bool {
    matches!(args.command, SnapshotsCommands::List(_))
}

pub(super) async fn run(ctx: &ResolvedContext, base: &BaseArgs, args: SnapshotsArgs) -> Result<()> {
    match args.command {
        SnapshotsCommands::List(list_args) => run_list(ctx, &list_args, base.json).await,
        SnapshotsCommands::Create(create_args) => {
            run_create(ctx, base, &create_args, base.json).await
        }
        SnapshotsCommands::Restore(restore_args) => {
            run_restore(ctx, &restore_args, base.json).await
        }
    }
}

#[derive(Debug, Clone)]
struct RestoreTarget {
    name: Option<String>,
    xact_id: String,
}

impl RestoreTarget {
    fn display_target(&self) -> String {
        match self.name.as_deref() {
            Some(name) => format!("snapshot '{name}' (xact {})", self.xact_id),
            None => format!("xact {}", self.xact_id),
        }
    }
}

async fn run_list(ctx: &ResolvedContext, args: &SnapshotListArgs, json: bool) -> Result<()> {
    let dataset = resolve_existing_dataset(ctx, args.dataset_name(), "snapshots list").await?;

    let mut snapshots = with_spinner(
        "Loading dataset snapshots...",
        api::list_dataset_snapshots(&ctx.client, &dataset.id),
    )
    .await?;
    sort_snapshots_for_display(&mut snapshots);

    if json {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "snapshots": snapshots,
            }))?
        );
        return Ok(());
    }

    let mut output = String::new();
    let count = format!(
        "{} {}",
        snapshots.len(),
        pluralize(snapshots.len(), "snapshot", None)
    );
    writeln!(
        output,
        "{} found for {} {} {} {} {}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(&ctx.project.name).bold(),
        console::style("/").dim().bold(),
        console::style(&dataset.name).bold()
    )?;

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Description"),
        header("Xact"),
        header("Created"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for snapshot in &snapshots {
        let description = snapshot
            .description
            .as_deref()
            .filter(|description| !description.is_empty())
            .map(|description| truncate(description, 60))
            .unwrap_or_else(|| "-".to_string());
        let xact_id = &snapshot.xact_id;
        let created = snapshot
            .created
            .as_deref()
            .map(|created| truncate(created, 10))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![&snapshot.name, &description, &xact_id, &created]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}

async fn run_create(
    ctx: &ResolvedContext,
    base: &BaseArgs,
    args: &SnapshotCreateArgs,
    json_output: bool,
) -> Result<()> {
    let dataset = resolve_existing_dataset(ctx, args.dataset_name(), "snapshots create").await?;
    let snapshot_name = resolve_snapshot_name(base, ctx, args.snapshot_name());
    let xact_id = resolve_snapshot_xact_id(ctx, &dataset, args.xact_id.as_deref()).await?;
    let description = normalize_optional_text(args.description.as_deref());

    let create_result = match with_spinner_visible(
        "Creating dataset snapshot...",
        api::create_dataset_snapshot(
            &ctx.client,
            &dataset.id,
            &snapshot_name,
            description.as_deref(),
            &xact_id,
        ),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(create_result) => create_result,
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!(
                    "Failed to create snapshot '{}' for dataset '{}'",
                    snapshot_name, dataset.name
                ),
            );
            return Err(error);
        }
    };
    let snapshot = create_result.dataset_snapshot;
    let found_existing = create_result.found_existing;

    if json_output {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "snapshot": snapshot,
                "found_existing": found_existing,
                "mode": "snapshot_create",
            }))?
        );
        return Ok(());
    }

    if found_existing {
        print_command_status(
            CommandStatus::Warning,
            &format!(
                "Snapshot '{}' already exists for '{}' (xact {}).",
                snapshot.name, dataset.name, snapshot.xact_id
            ),
        );
        return Ok(());
    }

    print_command_status(
        CommandStatus::Success,
        &format!(
            "Created snapshot '{}' for '{}' (xact {}).",
            snapshot.name, dataset.name, snapshot.xact_id
        ),
    );
    Ok(())
}

async fn run_restore(
    ctx: &ResolvedContext,
    args: &SnapshotRestoreArgs,
    json_output: bool,
) -> Result<()> {
    let dataset = resolve_existing_dataset(ctx, args.dataset_name(), "snapshots restore").await?;
    let target = resolve_restore_target(ctx, &dataset, args).await?;

    let preview = match with_spinner_visible(
        "Previewing dataset restore...",
        api::preview_dataset_restore(&ctx.client, &dataset.id, &target.xact_id),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(preview) => preview,
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!(
                    "Failed to preview restore for dataset '{}' to {}",
                    dataset.name,
                    target.display_target()
                ),
            );
            return Err(error);
        }
    };

    if json_output {
        return run_restore_json(ctx, dataset, target, preview, args.force).await;
    }

    print_restore_preview(&dataset, &target, &preview)?;

    if !args.force {
        if !is_interactive() {
            print_command_status(
                CommandStatus::Warning,
                &format!(
                    "Restore preview complete for '{}'. Re-run with --force to apply it non-interactively.",
                    dataset.name
                ),
            );
            return Ok(());
        }

        let confirmed = Confirm::new()
            .with_prompt(format!(
                "Restore dataset '{}' to {}?",
                dataset.name,
                target.display_target()
            ))
            .default(false)
            .interact()?;
        if !confirmed {
            print_command_status(
                CommandStatus::Warning,
                &format!(
                    "Cancelled restore for '{}' (no changes applied).",
                    dataset.name
                ),
            );
            return Ok(());
        }
    }

    run_restore_execute(ctx, &dataset, &target).await?;
    Ok(())
}

async fn run_restore_json(
    ctx: &ResolvedContext,
    dataset: Dataset,
    target: RestoreTarget,
    preview: DatasetRestorePreview,
    force: bool,
) -> Result<()> {
    if !force {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "target": {
                    "name": target.name.as_deref(),
                    "xact_id": target.xact_id,
                },
                "preview": preview,
                "restored": false,
                "mode": "snapshot_restore",
            }))?
        );
        return Ok(());
    }

    let result = run_restore_execute(ctx, &dataset, &target).await?;
    println!(
        "{}",
        serde_json::to_string(&json!({
            "dataset": dataset,
            "target": {
                "name": target.name.as_deref(),
                "xact_id": target.xact_id,
            },
            "preview": preview,
            "result": result,
            "restored": true,
            "mode": "snapshot_restore",
        }))?
    );
    Ok(())
}

async fn run_restore_execute(
    ctx: &ResolvedContext,
    dataset: &Dataset,
    target: &RestoreTarget,
) -> Result<DatasetRestoreResult> {
    let result = match with_spinner_visible(
        "Restoring dataset...",
        api::restore_dataset(&ctx.client, &dataset.id, &target.xact_id),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!(
                    "Failed to restore dataset '{}' to {}",
                    dataset.name,
                    target.display_target()
                ),
            );
            return Err(error);
        }
    };

    print_command_status(
        CommandStatus::Success,
        &format!(
            "Restored dataset '{}' to {} (xact {}; {} restored, {} deleted).",
            dataset.name,
            target.display_target(),
            result.xact_id.as_str(),
            result.rows_restored,
            result.rows_deleted
        ),
    );
    Ok(result)
}

async fn resolve_existing_dataset(
    ctx: &ResolvedContext,
    name: Option<&str>,
    command: &str,
) -> Result<Dataset> {
    match name.map(str::trim).filter(|value| !value.is_empty()) {
        Some(name) => with_spinner(
            "Loading dataset...",
            api::get_dataset_by_name(&ctx.client, &ctx.project.id, name),
        )
        .await?
        .ok_or_else(|| anyhow!("dataset '{name}' not found")),
        None => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets {command} <dataset>");
            }
            super::select_dataset_interactive(&ctx.client, &ctx.project.id).await
        }
    }
}

fn resolve_snapshot_name(base: &BaseArgs, ctx: &ResolvedContext, name: Option<&str>) -> String {
    match name.map(str::trim).filter(|value| !value.is_empty()) {
        Some(name) => name.to_string(),
        None => default_snapshot_name(
            resolve_default_snapshot_author(base, ctx)
                .as_deref()
                .unwrap_or("user"),
            Utc::now(),
        ),
    }
}

async fn resolve_snapshot_xact_id(
    ctx: &ResolvedContext,
    dataset: &Dataset,
    explicit_xact_id: Option<&str>,
) -> Result<String> {
    if let Some(xact_id) = explicit_xact_id.and_then(|value| normalize_optional_text(Some(value))) {
        return Ok(xact_id);
    }

    let head_xact_id = with_spinner(
        "Resolving dataset head xact...",
        api::get_dataset_head_xact_id(&ctx.client, &dataset.id),
    )
    .await?;

    head_xact_id.ok_or_else(|| {
        anyhow!(
            "dataset '{}' has no rows, so no head xact could be inferred; pass --xact-id explicitly",
            dataset.name
        )
    })
}

async fn resolve_restore_target(
    ctx: &ResolvedContext,
    dataset: &Dataset,
    args: &SnapshotRestoreArgs,
) -> Result<RestoreTarget> {
    if let Some(xact_id) = args
        .snapshot_xact_id()
        .and_then(|value| normalize_optional_text(Some(value)))
    {
        return Ok(RestoreTarget {
            name: None,
            xact_id,
        });
    }

    let snapshots = with_spinner(
        "Loading dataset snapshots...",
        api::list_dataset_snapshots(&ctx.client, &dataset.id),
    )
    .await?;
    if let Some(snapshot_name) = args
        .snapshot_name()
        .and_then(|value| normalize_optional_text(Some(value)))
    {
        return resolve_restore_target_by_name(&snapshots, &dataset.name, &snapshot_name);
    }

    if is_interactive() {
        return select_restore_target_interactive(&dataset.name, &snapshots);
    }

    bail!(
        "restore target required. Use: bt datasets snapshots restore <dataset> (--name <NAME> | --snapshot <XACT_ID>)"
    );
}

fn sort_snapshots_for_display(snapshots: &mut [DatasetSnapshot]) {
    snapshots.sort_by(|a, b| b.created.cmp(&a.created).then_with(|| a.name.cmp(&b.name)));
}

fn select_restore_target_interactive(
    dataset_name: &str,
    snapshots: &[DatasetSnapshot],
) -> Result<RestoreTarget> {
    let mut restorable_snapshots: Vec<&DatasetSnapshot> = snapshots.iter().collect();

    if restorable_snapshots.is_empty() {
        bail!(
            "no restorable dataset snapshots found for '{}'",
            dataset_name
        );
    }

    restorable_snapshots
        .sort_by(|a, b| b.created.cmp(&a.created).then_with(|| a.name.cmp(&b.name)));

    let labels: Vec<String> = restorable_snapshots
        .iter()
        .map(|snapshot| restore_snapshot_label(snapshot))
        .collect();
    let selection = crate::ui::fuzzy_select("Select dataset snapshot", &labels, 0)?;
    restore_target_from_snapshot(restorable_snapshots[selection])
}

fn restore_snapshot_label(snapshot: &DatasetSnapshot) -> String {
    let created = snapshot
        .created
        .as_deref()
        .map(|created| truncate(created, 10))
        .unwrap_or_else(|| "-".to_string());

    match snapshot
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        Some(description) => format!(
            "{} (xact {}, created {}, {})",
            snapshot.name,
            snapshot.xact_id,
            created,
            truncate(description, 40)
        ),
        None => format!(
            "{} (xact {}, created {})",
            snapshot.name, snapshot.xact_id, created
        ),
    }
}

fn restore_target_from_snapshot(snapshot: &DatasetSnapshot) -> Result<RestoreTarget> {
    Ok(RestoreTarget {
        name: Some(snapshot.name.clone()),
        xact_id: snapshot.xact_id.clone(),
    })
}

fn resolve_restore_target_by_name(
    snapshots: &[DatasetSnapshot],
    dataset_name: &str,
    snapshot_name: &str,
) -> Result<RestoreTarget> {
    let mut matches = snapshots
        .iter()
        .filter(|snapshot| snapshot.name == snapshot_name);
    let Some(snapshot) = matches.next() else {
        bail!(
            "dataset snapshot '{}' was not found for '{}'",
            snapshot_name,
            dataset_name
        );
    };
    if matches.next().is_some() {
        bail!(
            "multiple dataset snapshots named '{}' were found for '{}'; use --snapshot <XACT_ID> instead",
            snapshot_name,
            dataset_name
        );
    }

    restore_target_from_snapshot(snapshot)
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn print_restore_preview(
    dataset: &Dataset,
    target: &RestoreTarget,
    preview: &DatasetRestorePreview,
) -> Result<()> {
    let mut output = String::new();
    writeln!(
        output,
        "Restore preview for {} to {}:\n",
        console::style(&dataset.name).bold(),
        console::style(target.display_target()).bold()
    )?;
    writeln!(
        output,
        "Rows to restore: {}",
        console::style(preview.rows_to_restore).bold()
    )?;
    writeln!(
        output,
        "Rows to delete: {}",
        console::style(preview.rows_to_delete).bold()
    )?;

    print_with_pager(&output)?;
    Ok(())
}

fn resolve_default_snapshot_author(base: &BaseArgs, ctx: &ResolvedContext) -> Option<String> {
    if api_key_override_active(base) {
        return None;
    }

    let profile = resolve_profile_info(base.profile.as_deref(), Some(ctx.client.org_name()))?;
    profile_author_slug(&profile)
}

fn default_snapshot_name(author: &str, now: DateTime<Utc>) -> String {
    let author = sanitize_name_segment(author).unwrap_or_else(|| "user".to_string());
    format!("{author}-{}", now.format("%Y%m%d-%H%M%Sz"))
}

fn api_key_override_active(base: &BaseArgs) -> bool {
    !base.prefer_profile
        && base
            .api_key
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dataset_snapshot(
        name: &str,
        xact_id: &str,
        created: &str,
        description: Option<&str>,
    ) -> DatasetSnapshot {
        DatasetSnapshot {
            id: format!("snapshot_{name}"),
            dataset_id: "dataset_1".to_string(),
            name: name.to_string(),
            description: description.map(ToOwned::to_owned),
            xact_id: xact_id.to_string(),
            created: Some(created.to_string()),
        }
    }

    #[test]
    fn default_snapshot_name_formats_author_and_timestamp() {
        let now = DateTime::parse_from_rfc3339("2026-04-10T12:34:56Z")
            .expect("parse timestamp")
            .with_timezone(&Utc);
        assert_eq!(
            default_snapshot_name("Alice Smith", now),
            "alice-smith-20260410-123456z"
        );
    }

    #[test]
    fn restore_snapshot_label_includes_disambiguating_details() {
        let label = restore_snapshot_label(&dataset_snapshot(
            "baseline",
            "1000192656880881099",
            "2026-04-10T12:34:56Z",
            Some("Initial snapshot for restore flow"),
        ));
        assert!(label.contains("baseline"));
        assert!(label.contains("1000192656880881099"));
        assert!(label.contains(&truncate("2026-04-10T12:34:56Z", 10)));
        assert!(label.contains("Initial snapshot for restore flow"));
    }

    #[test]
    fn resolve_restore_target_by_name_returns_unique_match() {
        let snapshots = vec![dataset_snapshot(
            "baseline",
            "1000192656880881099",
            "2026-04-10T00:00:00Z",
            None,
        )];

        let target =
            resolve_restore_target_by_name(&snapshots, "my-dataset", "baseline").expect("target");
        assert_eq!(target.name.as_deref(), Some("baseline"));
        assert_eq!(target.xact_id, "1000192656880881099");
    }

    #[test]
    fn resolve_restore_target_by_name_rejects_duplicates() {
        let snapshots = vec![
            dataset_snapshot(
                "baseline",
                "1000192656880881099",
                "2026-04-10T00:00:00Z",
                None,
            ),
            dataset_snapshot(
                "baseline",
                "1000192656880881100",
                "2026-04-11T00:00:00Z",
                None,
            ),
        ];

        let error = resolve_restore_target_by_name(&snapshots, "my-dataset", "baseline")
            .expect_err("duplicate snapshot names should fail");
        assert!(error.to_string().contains("use --snapshot <XACT_ID>"));
    }

    #[test]
    fn restore_target_display_uses_name_when_available() {
        let target = RestoreTarget {
            name: Some("baseline".to_string()),
            xact_id: "1000192656880881099".to_string(),
        };
        assert_eq!(
            target.display_target(),
            "snapshot 'baseline' (xact 1000192656880881099)"
        );
    }
}
