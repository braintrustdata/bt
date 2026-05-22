use std::collections::HashSet;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;

use crate::args::{BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL};
use crate::http::build_http_client;
use dialoguer::console::style;

mod agents;
mod auth;
mod copy;
mod env_file;
mod language;
mod prompt;
mod skill_install;

use auth::{WizardSessionAuthClient, WizardSessionComplete};
use copy::{
    build_cleanup_message, no_agent_fallback_note, skill_next_step_hint, terminal_hyperlink,
    wizard_login_prompt, DOCS_URL, NOT_GIT_REPO_WARNING, WIZARD_CANCEL_MESSAGE, WIZARD_TITLE,
};
use env_file::{write_bt_config, write_env_braintrust, BtConfig};
use language::{detect_languages, DetectedLanguage};
use skill_install::{
    install_instrument_code_skill, write_fallback_prompt, InstallOptions, InstallResult,
};

const POLL_TIMEOUT_MARGIN: Duration = Duration::from_secs(5);

#[derive(Debug, Clone, Args)]
pub struct WizardArgs {
    /// Skip installing the instrument-code skill; write a fallback prompt file instead.
    #[arg(long, env = "BRAINTRUST_SETUP_NO_INSTALL_SKILL")]
    pub no_install_skill: bool,

    /// Agent id(s) to exclude from skill install. Repeatable.
    #[arg(
        long = "skip-agent",
        env = "BRAINTRUST_SETUP_SKIP_AGENTS",
        value_delimiter = ','
    )]
    pub skip_agents: Vec<String>,

    /// Override the skill install directory. Writes one SKILL.md into <dir>/instrument-code/.
    #[arg(long, env = "BRAINTRUST_SETUP_SKILL_DIR")]
    pub skill_dir: Option<PathBuf>,

    /// Pair with --api-key to skip device-code login (CI path).
    #[arg(long, env = "BRAINTRUST_SETUP_PROJECT_ID")]
    pub project_id: Option<String>,
}

pub async fn run(base: BaseArgs, args: WizardArgs) -> Result<()> {
    if base.json {
        bail!("`bt setup` is interactive and incompatible with --json. use: a subcommand (skills, instrument, mcp, doctor) or run without --json.");
    }

    let cwd = std::env::current_dir().context("resolving current directory")?;
    let api_url = base
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = base
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let app_url = strip_trailing_slash(&app_url).to_string();
    let api_url = strip_trailing_slash(&api_url).to_string();

    cliclack::set_theme(WizardTheme);
    cliclack::intro(WIZARD_TITLE).context("rendering intro")?;

    let git_root = find_git_root(&cwd);
    if git_root.is_none() {
        cliclack::log::warning(NOT_GIT_REPO_WARNING).ok();
    }

    let session = match login(&base, &args, &api_url, &app_url).await {
        Ok(s) => s,
        Err(err) => {
            cliclack::outro_cancel(format!("{WIZARD_CANCEL_MESSAGE} {err}")).ok();
            return Err(err);
        }
    };

    cliclack::log::success(format!(
        "Browser setup complete.\n  org: {}\n  project: {}",
        style(&session.org_name).green().bright(),
        style(&session.project_name).green().bright()
    ))
    .ok();

    if let Some(root) = &git_root {
        let env_result = write_env_braintrust(root, &session.api_key)?;
        cliclack::log::info(format!(
            "Wrote BRAINTRUST_API_KEY to {}.",
            display_relative(&env_result.env_file_path, &cwd)
        ))
        .ok();
        if env_result.added_to_gitignore {
            cliclack::log::info("Added .env.braintrust to .gitignore.").ok();
        } else if !env_result.already_covered {
            cliclack::log::info(".gitignore unchanged.").ok();
        }

        let cfg = BtConfig {
            org: &session.org_name,
            project: &session.project_name,
            project_id: &session.project_id,
        };
        write_bt_config(root, &cfg)?;
    } else {
        cliclack::log::info(format!(
            "BRAINTRUST_API_KEY={}\nNot in a git repo — set this in your environment manually.",
            session.api_key
        ))
        .ok();
    }

    let languages = detect_languages(&cwd);

    if args.no_install_skill {
        match write_fallback_prompt(&languages) {
            Ok(fallback) => {
                cliclack::note(
                    "Skill",
                    no_agent_fallback_note(&fallback.path.display().to_string()),
                )
                .ok();
            }
            Err(err) => {
                cliclack::log::warning(format!("Couldn't write fallback prompt: {err}")).ok();
            }
        }
    } else {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("failed to resolve HOME"))?;
        let install = install_instrument_code_skill(InstallOptions {
            cwd: &cwd,
            home,
            languages: &languages,
            skip_agents: &args.skip_agents,
            override_dir: args.skill_dir.as_deref(),
        })?;
        report_install(&install, &languages)?;
    }

    cliclack::outro(build_cleanup_message(DOCS_URL)).ok();
    Ok(())
}

async fn login(
    base: &BaseArgs,
    args: &WizardArgs,
    api_url: &str,
    app_url: &str,
) -> Result<WizardSessionComplete> {
    if let (Some(api_key), Some(project_id)) = (base.api_key.as_deref(), args.project_id.as_deref())
    {
        return login_ci(api_url, api_key, project_id).await;
    }
    if base.no_input {
        bail!("credentials required for --no-input mode. use: --api-key and --project-id (env: BRAINTRUST_API_KEY, BRAINTRUST_SETUP_PROJECT_ID), or run from a TTY.");
    }
    if !std::io::stdin().is_terminal() {
        bail!("TTY required for interactive login. use: --api-key and --project-id for non-interactive use, or run from a TTY.");
    }

    let http = build_http_client(Duration::from_secs(60))?;
    let client = WizardSessionAuthClient::new(http, app_url);
    let session = client.create_session().await?;
    let login_url = client.build_login_url(&session);

    cliclack::log::info(terminal_hyperlink(&login_url)).ok();
    cliclack::note("Login", wizard_login_prompt(&session.verification_code)).ok();
    let _ = open::that_detached(&login_url);

    let spinner = cliclack::spinner();
    spinner.start("Waiting for browser login…");
    let result = tokio::time::timeout(
        Duration::from_secs(3 * 60) + POLL_TIMEOUT_MARGIN,
        client.poll_session(&session.session_token, &session.poll_token),
    )
    .await;
    match result {
        Ok(Ok(complete)) => {
            spinner.stop("Logged in.");
            Ok(complete)
        }
        Ok(Err(err)) => {
            spinner.error("Login failed.");
            Err(err)
        }
        Err(_) => {
            spinner.error("Login timed out.");
            Err(anyhow!("Wizard session timed out."))
        }
    }
}

async fn login_ci(api_url: &str, api_key: &str, project_id: &str) -> Result<WizardSessionComplete> {
    let http = build_http_client(crate::http::DEFAULT_HTTP_TIMEOUT)?;
    let project: serde_json::Value = http
        .get(format!(
            "{api_url}/v1/project/{}",
            urlencoding::encode(project_id)
        ))
        .bearer_auth(api_key)
        .send()
        .await
        .with_context(|| format!("GET {api_url}/v1/project/{project_id}"))?
        .error_for_status()
        .with_context(|| format!("looking up project {project_id}"))?
        .json()
        .await
        .context("parsing project response")?;
    let org_id = project
        .get("org_id")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("project response missing org_id"))?;
    let project_name = project
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("project response missing name"))?
        .to_string();
    let org: serde_json::Value = http
        .get(format!(
            "{api_url}/v1/organization/{}",
            urlencoding::encode(org_id)
        ))
        .bearer_auth(api_key)
        .send()
        .await
        .with_context(|| format!("GET {api_url}/v1/organization/{org_id}"))?
        .error_for_status()
        .with_context(|| format!("looking up organization {org_id}"))?
        .json()
        .await
        .context("parsing organization response")?;
    let org_name = org
        .get("name")
        .and_then(|v| v.as_str())
        .ok_or_else(|| anyhow!("organization response missing name"))?
        .to_string();
    Ok(WizardSessionComplete {
        api_key: api_key.to_string(),
        org_id: org_id.to_string(),
        org_name,
        project_id: project_id.to_string(),
        project_name,
    })
}

fn report_install(result: &InstallResult, languages: &[DetectedLanguage]) -> Result<()> {
    if result.written.is_empty() {
        let fallback = write_fallback_prompt(languages)?;
        cliclack::note(
            "Skill",
            no_agent_fallback_note(&fallback.path.display().to_string()),
        )
        .ok();
        return Ok(());
    }
    let project_agents: HashSet<&'static str> = result
        .written
        .iter()
        .filter(|w| w.scope == skill_install::InstallScope::Project)
        .map(|w| w.display_name)
        .collect::<HashSet<_>>();
    let single_agent = if project_agents.len() == 1 {
        project_agents.iter().next().copied()
    } else {
        None
    };
    cliclack::log::info(skill_next_step_hint(single_agent)).ok();
    Ok(())
}

fn find_git_root(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn strip_trailing_slash(url: &str) -> &str {
    url.trim_end_matches('/')
}

/// Theme override: cliclack's default dims note bodies via `input_style`, which
/// flattens our own foreground colors (verification code, etc.) to grey. This
/// theme keeps every default behavior except that dim.
struct WizardTheme;

impl cliclack::Theme for WizardTheme {
    fn input_style(&self, state: &cliclack::ThemeState) -> console::Style {
        match state {
            cliclack::ThemeState::Cancel => console::Style::new().strikethrough(),
            _ => console::Style::new(),
        }
    }
}

fn display_relative(path: &Path, cwd: &Path) -> String {
    pathdiff::diff_paths(path, cwd)
        .map(|p| p.display().to_string())
        .unwrap_or_else(|| path.display().to_string())
}
