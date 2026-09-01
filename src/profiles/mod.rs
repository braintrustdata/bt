use std::fmt::Write as _;

use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use serde::Serialize;

use crate::args::LoginBaseArgs;
use crate::{auth, ui};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt profiles
  bt profiles list --json
  bt profiles doctor
  bt profiles repair --force
  bt profiles repair deleted-profile --force
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
    /// Diagnose orphaned credentials in the plaintext fallback store
    Doctor,
    /// Remove orphaned credentials from the plaintext fallback store
    Repair(RepairArgs),
    /// Delete a saved profile and its credentials
    Delete(DeleteArgs),
    /// Rename a saved profile and move its credentials
    Rename(RenameArgs),
}

#[derive(Debug, Clone, Args)]
struct RepairArgs {
    /// Deleted profile names to remove from the OS keychain and fallback store
    #[arg(value_name = "NAME")]
    names: Vec<String>,

    /// Skip confirmation
    #[arg(long, short = 'f')]
    force: bool,
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

#[derive(Serialize)]
struct CredentialDoctorOutput {
    status: &'static str,
    scope: &'static str,
    orphaned_keys: Vec<String>,
    keychain_checked: bool,
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
        Some(ProfilesCommand::Doctor) => doctor(base.json),
        Some(ProfilesCommand::Repair(args)) => repair(&base, args),
        Some(ProfilesCommand::Delete(args)) => delete(&base, args),
        Some(ProfilesCommand::Rename(args)) => rename(base.json, args),
    }
}

fn doctor(json: bool) -> Result<()> {
    let orphaned_keys = auth::orphaned_plaintext_secret_keys()?;
    let output = CredentialDoctorOutput {
        status: if orphaned_keys.is_empty() {
            "ok"
        } else {
            "orphaned_credentials"
        },
        scope: "plaintext_fallback",
        orphaned_keys,
        keychain_checked: false,
    };
    if json {
        println!("{}", serde_json::to_string(&output)?);
    } else if output.orphaned_keys.is_empty() {
        println!("No orphaned credentials found in the plaintext fallback store.");
        println!("OS keychain entries were not enumerated.");
    } else {
        println!(
            "Found {} orphaned credential entries in the plaintext fallback store:",
            output.orphaned_keys.len()
        );
        for key in &output.orphaned_keys {
            println!("  {key}");
        }
        println!(
            "Run `bt profiles repair` to remove them. OS keychain entries were not enumerated."
        );
    }
    Ok(())
}

fn repair(base: &LoginBaseArgs, args: RepairArgs) -> Result<()> {
    if !args.names.is_empty() {
        if !args.force {
            let term = ui::prompt_term()
                .ok_or_else(|| anyhow::anyhow!("confirmation required; re-run with `--force`"))?;
            let confirmed = dialoguer::Confirm::new()
                .with_prompt(format!(
                    "Remove credentials for {} deleted profile names from the OS keychain and plaintext fallback store?",
                    args.names.len()
                ))
                .default(false)
                .interact_on(&term)?;
            if !confirmed {
                if base.json {
                    println!(
                        r#"{{"status":"cancelled","scope":"named_credentials","repaired_profiles":[]}}"#
                    );
                } else {
                    println!("Cancelled");
                }
                return Ok(());
            }
        }
        let repaired = auth::repair_named_orphaned_credentials(&args.names)?;
        if base.json {
            println!(
                "{}",
                serde_json::json!({
                    "status": "repaired",
                    "scope": "named_credentials",
                    "repaired_profiles": repaired,
                })
            );
        } else {
            println!(
                "Removed credentials for {} deleted profile names from the OS keychain and plaintext fallback store.",
                repaired.len()
            );
        }
        return Ok(());
    }

    let orphaned = auth::orphaned_plaintext_secret_keys()?;
    if orphaned.is_empty() {
        if base.json {
            println!(r#"{{"status":"ok","scope":"plaintext_fallback","removed_keys":[]}}"#);
        } else {
            println!("No orphaned credentials found in the plaintext fallback store.");
        }
        return Ok(());
    }

    if !args.force {
        let term = ui::prompt_term().ok_or_else(|| {
            anyhow::anyhow!("confirmation required; re-run `bt profiles repair --force`")
        })?;
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(format!(
                "Remove {} orphaned plaintext credential entries?",
                orphaned.len()
            ))
            .default(false)
            .interact_on(&term)?;
        if !confirmed {
            if base.json {
                println!(
                    r#"{{"status":"cancelled","scope":"plaintext_fallback","removed_keys":[]}}"#
                );
            } else {
                println!("Cancelled");
            }
            return Ok(());
        }
    }

    let removed = auth::repair_orphaned_plaintext_secrets()?;
    if base.json {
        println!(
            "{}",
            serde_json::json!({
                "status": "repaired",
                "scope": "plaintext_fallback",
                "removed_keys": removed,
            })
        );
    } else {
        println!(
            "Removed {} orphaned credential entries from the plaintext fallback store.",
            removed.len()
        );
        println!("OS keychain entries were not enumerated or changed.");
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
        ui::header("Email"),
        ui::header("App URL"),
    ]);
    ui::apply_column_padding(&mut table, (0, 4));
    for profile in &profiles {
        let email = profile.email.as_deref().unwrap_or("-");
        table.add_row(vec![
            profile.name.as_str(),
            profile.auth.as_str(),
            email,
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

    auth::delete_profile(&name, args.force, base.json)?;
    Ok(())
}

fn rename(json: bool, args: RenameArgs) -> Result<()> {
    let old_name = args.name.trim().to_string();
    let new_name = args.new_name.trim().to_string();
    auth::rename_profile(&old_name, &new_name, json)
}
