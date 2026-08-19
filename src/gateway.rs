//! Configure coding agents to send model requests through Braintrust Gateway.

use std::fs;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Map, Value};
use toml::Value as TomlValue;

use crate::args::{BaseArgs, LoginBaseArgs};
use crate::setup;
use crate::utils::{write_json_atomic_private, write_text_atomic_private};

const DEFAULT_GATEWAY_URL: &str = "https://gateway.braintrust.dev";

#[derive(Debug, Clone, Args)]
pub struct GatewayArgs {
    #[command(subcommand)]
    command: GatewayCommand,
}

/// Gateway setup has no project routing option. Project selection belongs to
/// individual Gateway requests, not persistent agent credentials.
#[derive(Debug, Clone, Args)]
pub struct GatewayBaseArgs {
    #[command(flatten)]
    pub(crate) login: LoginBaseArgs,

    /// Override active org
    #[arg(short = 'o', long = "org", env = "BRAINTRUST_ORG_NAME", global = true)]
    pub(crate) org_name: Option<String>,
}

#[derive(Debug, Clone, Subcommand)]
enum GatewayCommand {
    /// Configure a coding agent to use Braintrust Gateway
    Setup(GatewaySetupArgs),
}

#[derive(Debug, Clone, Args)]
struct GatewaySetupArgs {
    /// Agent to configure (positional form)
    #[arg(value_enum)]
    agent: Option<GatewayAgent>,

    /// Agent to configure
    #[arg(long = "agent", env = "BRAINTRUST_GATEWAY_AGENT", value_enum)]
    agent_flag: Option<GatewayAgent>,

    /// Gateway URL to use for model requests
    #[arg(long, env = "BRAINTRUST_GATEWAY_URL", default_value = DEFAULT_GATEWAY_URL)]
    gateway_url: String,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum GatewayAgent {
    /// Configure Claude Code
    Claude,
    /// Configure Codex
    Codex,
}

pub async fn run(base: GatewayBaseArgs, args: GatewayArgs) -> Result<()> {
    let GatewayCommand::Setup(args) = args.command;
    let gateway_url = normalize_gateway_url(&args.gateway_url)?;
    let home = dirs::home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let agent = args
        .agent
        .or(args.agent_flag)
        .ok_or_else(|| anyhow!("an agent is required; pass `claude`/`codex` or --agent <AGENT>"))?;
    let mut auth_base = BaseArgs {
        login: base.login,
        org_name: base.org_name,
        project: None,
    };
    let api_key = setup::durable_setup_api_key(&mut auth_base).await?;

    let path = match agent {
        GatewayAgent::Claude => {
            let path = home.join(".claude/settings.json");
            configure_claude(&path, &gateway_url, &api_key)?;
            path
        }
        GatewayAgent::Codex => {
            let path = home.join(".codex/config.toml");
            configure_codex(&path, &gateway_url, &api_key)?;
            path
        }
    };

    if auth_base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "agent": agent.as_str(),
                "gateway_url": gateway_url,
                "settings_path": path,
            }))?
        );
    } else if auth_base.verbose {
        eprintln!(
            "Configured {} to use Braintrust Gateway ({})",
            agent.display_name(),
            path.display()
        );
    }

    Ok(())
}

impl GatewayAgent {
    fn as_str(self) -> &'static str {
        match self {
            Self::Claude => "claude",
            Self::Codex => "codex",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Self::Claude => "Claude Code",
            Self::Codex => "Codex",
        }
    }
}

fn normalize_gateway_url(gateway_url: &str) -> Result<String> {
    let gateway_url = gateway_url.trim().trim_end_matches('/');
    if gateway_url.is_empty() {
        return Err(anyhow!("--gateway-url cannot be empty"));
    }
    let parsed = reqwest::Url::parse(gateway_url)
        .with_context(|| format!("invalid --gateway-url '{gateway_url}'"))?;
    if !matches!(parsed.scheme(), "http" | "https") {
        return Err(anyhow!("--gateway-url must use http or https"));
    }
    Ok(gateway_url.to_string())
}

fn configure_claude(path: &Path, gateway_url: &str, api_key: &str) -> Result<()> {
    let mut root = load_json_object(path)?;
    let env = root
        .entry("env".to_string())
        .or_insert_with(|| Value::Object(Map::new()))
        .as_object_mut()
        .ok_or_else(|| anyhow!("field 'env' in {} must be a JSON object", path.display()))?;
    env.insert(
        "ANTHROPIC_BASE_URL".to_string(),
        Value::String(gateway_url.to_string()),
    );
    env.insert(
        "ANTHROPIC_AUTH_TOKEN".to_string(),
        Value::String(api_key.to_string()),
    );
    write_json_atomic_private(path, &root)
}

fn configure_codex(path: &Path, gateway_url: &str, api_key: &str) -> Result<()> {
    let mut root = load_toml_table(path)?;
    let env = root
        .entry("env".to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()))
        .as_table_mut()
        .ok_or_else(|| anyhow!("field 'env' in {} must be a TOML table", path.display()))?;
    env.insert(
        "BRAINTRUST_GATEWAY_API_KEY".to_string(),
        TomlValue::String(api_key.to_string()),
    );
    root.insert(
        "model_provider".to_string(),
        TomlValue::String("braintrust_gateway".to_string()),
    );
    let providers = root
        .entry("model_providers".to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()))
        .as_table_mut()
        .ok_or_else(|| {
            anyhow!(
                "field 'model_providers' in {} must be a TOML table",
                path.display()
            )
        })?;
    let provider = providers
        .entry("braintrust_gateway".to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()))
        .as_table_mut()
        .ok_or_else(|| {
            anyhow!(
                "field 'model_providers.braintrust_gateway' in {} must be a TOML table",
                path.display()
            )
        })?;
    provider.insert(
        "name".to_string(),
        TomlValue::String("Braintrust Gateway".to_string()),
    );
    provider.insert(
        "base_url".to_string(),
        TomlValue::String(gateway_url.to_string()),
    );
    provider.insert(
        "env_key".to_string(),
        TomlValue::String("BRAINTRUST_GATEWAY_API_KEY".to_string()),
    );
    provider.insert(
        "wire_api".to_string(),
        TomlValue::String("responses".to_string()),
    );
    let content = format!("{}\n", toml::to_string_pretty(&TomlValue::Table(root))?);
    write_text_atomic_private(path, &content)
}

fn load_json_object(path: &Path) -> Result<Map<String, Value>> {
    match fs::read_to_string(path) {
        Ok(content) => serde_json::from_str::<Value>(&content)
            .with_context(|| format!("failed to parse JSON file {}", path.display()))?
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow!("{} must contain a JSON object", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

fn load_toml_table(path: &Path) -> Result<toml::map::Map<String, TomlValue>> {
    match fs::read_to_string(path) {
        Ok(content) => content
            .parse::<TomlValue>()
            .with_context(|| format!("failed to parse TOML file {}", path.display()))?
            .as_table()
            .cloned()
            .ok_or_else(|| anyhow!("{} must contain a TOML table", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(toml::map::Map::new()),
        Err(error) => Err(error).with_context(|| format!("failed to read {}", path.display())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn claude_gateway_configuration_preserves_existing_settings() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("settings.json");
        fs::write(
            &path,
            r#"{"permissions":{"allow":["Read"]},"env":{"KEEP":"yes"}}"#,
        )
        .unwrap();

        configure_claude(&path, "https://gateway.test.example", "sk-test-gateway-key").unwrap();

        let settings: Value = serde_json::from_str(&fs::read_to_string(path).unwrap()).unwrap();
        assert_eq!(settings["permissions"]["allow"][0], "Read");
        assert_eq!(settings["env"]["KEEP"], "yes");
        assert_eq!(
            settings["env"]["ANTHROPIC_BASE_URL"],
            "https://gateway.test.example"
        );
        assert_eq!(
            settings["env"]["ANTHROPIC_AUTH_TOKEN"],
            "sk-test-gateway-key"
        );
    }

    #[test]
    fn codex_gateway_configuration_preserves_existing_settings() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("config.toml");
        fs::write(&path, "model = \"test-model\"\n[env]\nKEEP = \"yes\"\n").unwrap();

        configure_codex(&path, "https://gateway.test.example", "sk-test-gateway-key").unwrap();

        let settings: TomlValue = fs::read_to_string(path).unwrap().parse().unwrap();
        assert_eq!(settings["model"].as_str(), Some("test-model"));
        assert_eq!(settings["env"]["KEEP"].as_str(), Some("yes"));
        assert_eq!(
            settings["model_provider"].as_str(),
            Some("braintrust_gateway")
        );
        assert_eq!(
            settings["env"]["BRAINTRUST_GATEWAY_API_KEY"].as_str(),
            Some("sk-test-gateway-key")
        );
        assert_eq!(
            settings["model_providers"]["braintrust_gateway"]["base_url"].as_str(),
            Some("https://gateway.test.example")
        );
        assert_eq!(
            settings["model_providers"]["braintrust_gateway"]["env_key"].as_str(),
            Some("BRAINTRUST_GATEWAY_API_KEY")
        );
    }
}
