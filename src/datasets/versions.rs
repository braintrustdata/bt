use std::{fmt::Write as _, time::Duration};

use anyhow::{anyhow, bail, Result};
use chrono::{DateTime, Utc};
use dialoguer::{console, Confirm};
use serde_json::json;

use crate::{
    args::BaseArgs,
    auth::{self, ProfileInfo},
    ui::{
        apply_column_padding, header, is_interactive, print_command_status, print_with_pager,
        styled_table, truncate, with_spinner, with_spinner_visible, CommandStatus,
    },
    utils::pluralize,
};

use super::{
    api::{self, Dataset, DatasetRestorePreview, DatasetRestoreResult, DatasetVersion},
    ResolvedContext, VersionCreateArgs, VersionListArgs, VersionRestoreArgs, VersionsArgs,
    VersionsCommands,
};

pub(super) async fn run(ctx: &ResolvedContext, base: &BaseArgs, args: VersionsArgs) -> Result<()> {
    let json = base.json;
    match args.command {
        VersionsCommands::List(list_args) => run_list(ctx, &list_args, json).await,
        VersionsCommands::Create(create_args) => run_create(ctx, base, &create_args, json).await,
        VersionsCommands::Restore(restore_args) => run_restore(ctx, &restore_args, json).await,
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
            Some(name) => format!("version '{name}' (xact {})", self.xact_id),
            None => format!("xact {}", self.xact_id),
        }
    }
}

async fn run_list(ctx: &ResolvedContext, args: &VersionListArgs, json: bool) -> Result<()> {
    let dataset = resolve_existing_dataset(
        ctx,
        args.dataset_name(),
        "bt datasets versions list <dataset>",
    )
    .await?;

    let mut versions = with_spinner(
        "Loading dataset versions...",
        api::list_dataset_versions(&ctx.client, &dataset.id),
    )
    .await?;
    sort_versions_for_display(&mut versions);

    if json {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "versions": versions,
            }))?
        );
        return Ok(());
    }

    let mut output = String::new();
    let count = format!(
        "{} {}",
        versions.len(),
        pluralize(versions.len(), "version", None)
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

    for version in &versions {
        let description = version
            .description_text()
            .map(|description| truncate(description, 60))
            .unwrap_or_else(|| "-".to_string());
        let xact_id = version
            .xact_id_text()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| "-".to_string());
        let created = version
            .created_text()
            .map(|created| truncate(created, 10))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![&version.name, &description, &xact_id, &created]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}

async fn run_create(
    ctx: &ResolvedContext,
    base: &BaseArgs,
    args: &VersionCreateArgs,
    json_output: bool,
) -> Result<()> {
    let dataset = resolve_existing_dataset(
        ctx,
        args.dataset_name(),
        "bt datasets versions create <dataset> <name> [--xact-id <XACT_ID>]",
    )
    .await?;
    let version_name = resolve_version_name(base, ctx, args.version_name());
    let xact_id = resolve_version_xact_id(ctx, &dataset, args.xact_id.as_deref()).await?;
    let description = normalize_optional_text(args.description.as_deref());

    let version = match with_spinner_visible(
        "Creating dataset version...",
        api::create_dataset_version(
            &ctx.client,
            &dataset.id,
            &version_name,
            description.as_deref(),
            &xact_id,
        ),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(version) => version,
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!(
                    "Failed to create version '{}' for dataset '{}'",
                    version_name, dataset.name
                ),
            );
            return Err(error);
        }
    };

    if json_output {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "version": version,
                "mode": "version_create",
            }))?
        );
        return Ok(());
    }

    let version_xact_id = version.xact_id_text().unwrap_or(&xact_id);
    print_command_status(
        CommandStatus::Success,
        &format!(
            "Created version '{}' for '{}' (xact {}).",
            version.name, dataset.name, version_xact_id
        ),
    );
    Ok(())
}

async fn run_restore(
    ctx: &ResolvedContext,
    args: &VersionRestoreArgs,
    json_output: bool,
) -> Result<()> {
    let usage =
        "bt datasets versions restore <dataset> (--name <NAME> | --version <XACT_ID>) [--force]";
    let dataset = resolve_existing_dataset(ctx, args.dataset_name(), usage).await?;
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
                "mode": "version_restore",
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
            "mode": "version_restore",
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
    usage: &str,
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
                bail!("dataset name required. Use: {usage}");
            }
            super::select_dataset_interactive(&ctx.client, &ctx.project.id).await
        }
    }
}

fn resolve_version_name(base: &BaseArgs, ctx: &ResolvedContext, name: Option<&str>) -> String {
    match name.map(str::trim).filter(|value| !value.is_empty()) {
        Some(name) => name.to_string(),
        None => default_version_name(
            resolve_default_version_author(base, ctx)
                .as_deref()
                .unwrap_or("user"),
            Utc::now(),
        ),
    }
}

async fn resolve_version_xact_id(
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
    args: &VersionRestoreArgs,
) -> Result<RestoreTarget> {
    if let Some(xact_id) = args
        .version_xact_id()
        .and_then(|value| normalize_optional_text(Some(value)))
    {
        return Ok(RestoreTarget {
            name: None,
            xact_id,
        });
    }

    let versions = with_spinner(
        "Loading dataset versions...",
        api::list_dataset_versions(&ctx.client, &dataset.id),
    )
    .await?;
    if let Some(version_name) = args
        .version_name()
        .and_then(|value| normalize_optional_text(Some(value)))
    {
        return resolve_restore_target_by_name(&versions, &dataset.name, &version_name);
    }

    if is_interactive() {
        return select_restore_target_interactive(&dataset.name, &versions);
    }

    bail!(
        "restore target required. Use: bt datasets versions restore <dataset> (--name <NAME> | --version <XACT_ID>)"
    );
}

fn sort_versions_for_display(versions: &mut [DatasetVersion]) {
    versions.sort_by(|a, b| {
        b.created_text()
            .cmp(&a.created_text())
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn select_restore_target_interactive(
    dataset_name: &str,
    versions: &[DatasetVersion],
) -> Result<RestoreTarget> {
    let mut restorable_versions: Vec<&DatasetVersion> = versions
        .iter()
        .filter(|version| version.xact_id_text().is_some())
        .collect();

    if restorable_versions.is_empty() {
        bail!(
            "no restorable dataset versions found for '{}'",
            dataset_name
        );
    }

    restorable_versions.sort_by(|a, b| {
        b.created_text()
            .cmp(&a.created_text())
            .then_with(|| a.name.cmp(&b.name))
    });

    let labels: Vec<String> = restorable_versions
        .iter()
        .map(|version| restore_version_label(version))
        .collect();
    let selection = crate::ui::fuzzy_select("Select dataset version", &labels, 0)?;
    restore_target_from_version(restorable_versions[selection], dataset_name)
}

fn restore_version_label(version: &DatasetVersion) -> String {
    let xact_id = version.xact_id_text().unwrap_or("-");
    let created = version
        .created_text()
        .map(|created| truncate(created, 10))
        .unwrap_or_else(|| "-".to_string());

    match version.description_text() {
        Some(description) => format!(
            "{} (xact {}, created {}, {})",
            version.name,
            xact_id,
            created,
            truncate(description, 40)
        ),
        None => format!("{} (xact {}, created {})", version.name, xact_id, created),
    }
}

fn restore_target_from_version(
    version: &DatasetVersion,
    dataset_name: &str,
) -> Result<RestoreTarget> {
    let Some(xact_id) = version.xact_id_text().map(ToOwned::to_owned) else {
        bail!(
            "dataset version '{}' for '{}' is missing an xact id; use --version <XACT_ID> instead",
            version.name,
            dataset_name
        );
    };

    Ok(RestoreTarget {
        name: Some(version.name.clone()),
        xact_id,
    })
}

fn resolve_restore_target_by_name(
    versions: &[DatasetVersion],
    dataset_name: &str,
    version_name: &str,
) -> Result<RestoreTarget> {
    let mut matches = versions
        .iter()
        .filter(|version| version.name == version_name);
    let Some(version) = matches.next() else {
        bail!(
            "dataset version '{}' was not found for '{}'",
            version_name,
            dataset_name
        );
    };
    if matches.next().is_some() {
        bail!(
            "multiple dataset versions named '{}' were found for '{}'; use --version <XACT_ID> instead",
            version_name,
            dataset_name
        );
    }

    restore_target_from_version(version, dataset_name)
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

    if !preview.extra.is_empty() {
        writeln!(output)?;
        writeln!(
            output,
            "{}",
            serde_json::to_string_pretty(&serde_json::Value::Object(preview.extra.clone()))
                .unwrap_or_else(|_| serde_json::Value::Object(preview.extra.clone()).to_string())
        )?;
    }
    print_with_pager(&output)?;
    Ok(())
}

fn resolve_default_version_author(base: &BaseArgs, ctx: &ResolvedContext) -> Option<String> {
    if api_key_override_active(base) {
        return None;
    }

    let profile = auth::resolve_profile_info(base.profile.as_deref(), Some(ctx.client.org_name()))?;
    profile_author_slug(&profile)
}

fn profile_author_slug(profile: &ProfileInfo) -> Option<String> {
    [
        profile.user_name.as_deref(),
        profile.email.as_deref().and_then(email_local_part),
        Some(profile.name.as_str()),
    ]
    .into_iter()
    .flatten()
    .find_map(sanitize_version_name_segment)
}

fn email_local_part(email: &str) -> Option<&str> {
    email
        .split_once('@')
        .map(|(local, _)| local)
        .or(Some(email))
}

fn default_version_name(author: &str, now: DateTime<Utc>) -> String {
    let author = sanitize_version_name_segment(author).unwrap_or_else(|| "user".to_string());
    format!("{author}-{}", now.format("%Y%m%d-%H%M%Sz"))
}

fn sanitize_version_name_segment(value: &str) -> Option<String> {
    let mut normalized = String::new();
    let mut last_was_dash = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !normalized.is_empty() && !last_was_dash {
            normalized.push('-');
            last_was_dash = true;
        }
    }

    while normalized.ends_with('-') {
        normalized.pop();
    }

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
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

    fn profile_info(name: &str, user_name: Option<&str>, email: Option<&str>) -> ProfileInfo {
        ProfileInfo {
            name: name.to_string(),
            org_name: None,
            user_name: user_name.map(ToOwned::to_owned),
            email: email.map(ToOwned::to_owned),
            api_key_hint: None,
        }
    }

    fn dataset_version(
        name: &str,
        xact_id: Option<&str>,
        created: Option<&str>,
        description: Option<&str>,
    ) -> DatasetVersion {
        DatasetVersion {
            id: format!("snapshot_{name}"),
            dataset_id: Some("dataset_1".to_string()),
            name: name.to_string(),
            description: description.map(ToOwned::to_owned),
            xact_id: xact_id.map(ToOwned::to_owned),
            created: created.map(ToOwned::to_owned),
            created_at: None,
            metadata: None,
        }
    }

    #[test]
    fn profile_author_slug_prefers_user_name() {
        let profile = profile_info("work", Some("Alice Smith"), Some("alice@example.com"));
        assert_eq!(
            profile_author_slug(&profile).as_deref(),
            Some("alice-smith")
        );
    }

    #[test]
    fn profile_author_slug_falls_back_to_email_local_part() {
        let profile = profile_info("work", None, Some("alice.dev@example.com"));
        assert_eq!(profile_author_slug(&profile).as_deref(), Some("alice-dev"));
    }

    #[test]
    fn profile_author_slug_falls_back_to_profile_name() {
        let profile = profile_info("Work Profile", None, None);
        assert_eq!(
            profile_author_slug(&profile).as_deref(),
            Some("work-profile")
        );
    }

    #[test]
    fn default_version_name_formats_author_and_timestamp() {
        let now = DateTime::parse_from_rfc3339("2026-04-10T12:34:56Z")
            .expect("parse timestamp")
            .with_timezone(&Utc);
        assert_eq!(
            default_version_name("Alice Smith", now),
            "alice-smith-20260410-123456z"
        );
    }

    #[test]
    fn sanitize_version_name_segment_collapses_non_alnum() {
        assert_eq!(
            sanitize_version_name_segment("  A/B C__D  ").as_deref(),
            Some("a-b-c-d")
        );
        assert!(sanitize_version_name_segment("!!!").is_none());
    }

    #[test]
    fn restore_version_label_includes_disambiguating_details() {
        let label = restore_version_label(&dataset_version(
            "baseline",
            Some("1000192656880881099"),
            Some("2026-04-10T12:34:56Z"),
            Some("Initial snapshot for restore flow"),
        ));
        assert!(label.contains("baseline"));
        assert!(label.contains("1000192656880881099"));
        assert!(label.contains(&truncate("2026-04-10T12:34:56Z", 10)));
        assert!(label.contains("Initial snapshot for restore flow"));
    }

    #[test]
    fn restore_target_from_version_requires_xact_id() {
        let version = dataset_version("baseline", None, Some("2026-04-10T00:00:00Z"), None);
        let error = restore_target_from_version(&version, "my-dataset")
            .expect_err("missing xact id should fail");
        assert!(error.to_string().contains("missing an xact id"));
    }

    #[test]
    fn resolve_restore_target_by_name_returns_unique_match() {
        let versions = vec![dataset_version(
            "baseline",
            Some("1000192656880881099"),
            Some("2026-04-10T00:00:00Z"),
            None,
        )];

        let target =
            resolve_restore_target_by_name(&versions, "my-dataset", "baseline").expect("target");
        assert_eq!(target.name.as_deref(), Some("baseline"));
        assert_eq!(target.xact_id, "1000192656880881099");
    }

    #[test]
    fn resolve_restore_target_by_name_rejects_duplicates() {
        let versions = vec![
            dataset_version(
                "baseline",
                Some("1000192656880881099"),
                Some("2026-04-10T00:00:00Z"),
                None,
            ),
            dataset_version(
                "baseline",
                Some("1000192656880881100"),
                Some("2026-04-11T00:00:00Z"),
                None,
            ),
        ];

        let error = resolve_restore_target_by_name(&versions, "my-dataset", "baseline")
            .expect_err("duplicate version names should fail");
        assert!(error.to_string().contains("use --version <XACT_ID>"));
    }

    #[test]
    fn resolve_restore_target_by_name_requires_xact_id() {
        let versions = vec![dataset_version(
            "baseline",
            None,
            Some("2026-04-10T00:00:00Z"),
            None,
        )];

        let error = resolve_restore_target_by_name(&versions, "my-dataset", "baseline")
            .expect_err("missing xact id should fail");
        assert!(error.to_string().contains("missing an xact id"));
    }

    #[test]
    fn restore_target_display_uses_name_when_available() {
        let target = RestoreTarget {
            name: Some("baseline".to_string()),
            xact_id: "1000192656880881099".to_string(),
        };
        assert_eq!(
            target.display_target(),
            "version 'baseline' (xact 1000192656880881099)"
        );
    }
}
