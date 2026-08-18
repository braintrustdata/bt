use std::fmt::Write as _;

use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::args::LoginBaseArgs;
use crate::{auth, config, ui};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt profiles
  bt profiles default
  bt profiles default test-profile --global
  bt profiles list --json
  bt profiles delete test-profile
  bt profiles rename test-profile renamed-profile
")]
pub struct ProfilesArgs {
    #[command(subcommand)]
    command: Option<ProfilesCommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum ProfilesCommand {
    /// List saved profiles
    List,
    /// Show or set the default profile
    #[command(visible_alias = "set-default")]
    Default(DefaultArgs),
    /// Delete a saved profile and its credentials
    Delete(DeleteArgs),
    /// Rename a saved profile and move its credentials
    Rename(RenameArgs),
}

#[derive(Debug, Clone, Args)]
struct DefaultArgs {
    /// Profile name to make the default (omit to show the current default)
    #[arg(value_name = "NAME")]
    name: Option<String>,

    /// Read or write the global default
    #[arg(long, short = 'g', conflicts_with = "local")]
    global: bool,

    /// Read or write the current working tree's local default
    #[arg(long, short = 'l')]
    local: bool,
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Profile name (interactive picker if omitted)
    #[arg(value_name = "NAME")]
    name: Option<String>,

    /// Skip confirmation prompt (requires a profile name)
    #[arg(long, short = 'f')]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct RenameArgs {
    /// Current profile name
    #[arg(value_name = "NAME")]
    name: String,

    /// New profile name
    #[arg(value_name = "NEW_NAME")]
    new_name: String,
}

#[derive(Serialize)]
struct ProfileOutput<'a> {
    name: &'a str,
    auth: &'a str,
    app_url: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_url: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    org: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_email: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key_hint: Option<&'a str>,
}

impl<'a> From<&'a auth::ProfileInfo> for ProfileOutput<'a> {
    fn from(profile: &'a auth::ProfileInfo) -> Self {
        Self {
            name: &profile.name,
            auth: &profile.auth,
            app_url: &profile.app_url,
            api_url: profile.oauth_api_url.as_deref(),
            org: profile.org_name.as_deref(),
            user_name: profile.user_name.as_deref(),
            user_email: profile.email.as_deref(),
            api_key_hint: profile.api_key_hint.as_deref(),
        }
    }
}

pub fn run(base: LoginBaseArgs, args: ProfilesArgs) -> Result<()> {
    match args.command {
        None | Some(ProfilesCommand::List) => list(base.json),
        Some(ProfilesCommand::Default(args)) => default_profile(base.json, args),
        Some(ProfilesCommand::Delete(args)) => delete(&base, args),
        Some(ProfilesCommand::Rename(args)) => rename(base.json, args),
    }
}

fn default_profile(json: bool, args: DefaultArgs) -> Result<()> {
    let Some(name) = args.name else {
        let config = if args.global {
            config::load_global()?
        } else if args.local {
            config::local_path()
                .as_deref()
                .map(config::load_file)
                .unwrap_or_default()
        } else {
            config::load()?
        };
        let name = config::trimmed_option(config.profile.as_deref());
        if json {
            println!("{}", serde_json::json!({ "name": name }));
        } else if let Some(name) = name {
            println!("{name}");
        } else {
            println!("No default profile set.");
        }
        return Ok(());
    };

    let name = name.trim();
    if name.is_empty() {
        bail!("profile name cannot be empty");
    }
    let profiles = auth::list_profiles()?;
    if !profiles.iter().any(|profile| profile.name == name) {
        let available = profiles
            .iter()
            .map(|profile| profile.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        let suffix = if available.is_empty() {
            String::new()
        } else {
            format!(": {available}")
        };
        bail!(
            "profile '{name}' not found; run `bt profiles list` to see available profiles{suffix}"
        );
    }

    let path = config::resolve_write_path(args.global, args.local)?;
    let mut selected_config = config::load_file(&path);
    selected_config.profile = Some(name.to_string());
    config::save_file(&path, &selected_config)?;
    let scope = if path == config::global_path()? {
        "global"
    } else {
        "local"
    };

    if json {
        println!(
            "{}",
            serde_json::json!({
                "name": name,
                "path": path,
                "scope": scope,
                "status": "default",
            })
        );
    } else {
        ui::print_command_status(
            ui::CommandStatus::Success,
            &format!("Set default profile to '{name}' ({scope})"),
        );
    }
    Ok(())
}

fn list(json: bool) -> Result<()> {
    let profiles = auth::list_profiles()?;
    if json {
        let output: Vec<_> = profiles.iter().map(ProfileOutput::from).collect();
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }
    if profiles.is_empty() {
        println!("No saved profiles. Run `bt login` to create one.");
        return Ok(());
    }

    let mut output = String::new();
    writeln!(output, "{} saved profiles\n", profiles.len())?;
    let mut table = ui::styled_table();
    table.set_header(vec![
        ui::header("Name"),
        ui::header("Auth"),
        ui::header("Identity / org"),
        ui::header("App URL"),
    ]);
    ui::apply_column_padding(&mut table, (0, 4));
    for profile in &profiles {
        let identity = profile
            .email
            .as_deref()
            .or(profile.user_name.as_deref())
            .or(profile.org_name.as_deref())
            .or(profile.api_key_hint.as_deref())
            .unwrap_or("-");
        table.add_row(vec![
            profile.name.as_str(),
            profile.auth.as_str(),
            identity,
            profile.app_url.as_str(),
        ]);
    }
    write!(output, "{table}")?;
    ui::print_with_pager(&output)?;
    Ok(())
}

fn delete(base: &LoginBaseArgs, args: DeleteArgs) -> Result<()> {
    if args.force && args.name.is_none() && !base.profile_explicit {
        bail!("profile name required when using --force. Use: bt profiles delete <name> --force");
    }

    let name = if let Some(name) = args.name {
        name
    } else if base.profile_explicit {
        base.profile.clone().unwrap_or_default()
    } else if ui::can_prompt() {
        auth::select_profile_interactive(base.profile.as_deref())?
            .expect("profile selection returns a name")
    } else {
        bail!("profile name required. Use: bt profiles delete <name>");
    };

    if auth::delete_profile(&name, args.force, base.json)? {
        update_config_references(&name, None);
    }
    Ok(())
}

fn rename(json: bool, args: RenameArgs) -> Result<()> {
    let old_name = args.name.trim().to_string();
    let new_name = args.new_name.trim().to_string();
    auth::rename_profile(&old_name, &new_name, json)?;
    update_config_references(&old_name, Some(&new_name));
    Ok(())
}

fn update_config_references(old_name: &str, new_name: Option<&str>) {
    if let Err(err) = config::replace_profile_references(old_name, new_name) {
        eprintln!("warning: profile was updated, but its config reference was not: {err}");
    }
}
