use std::path::{Path, PathBuf};

use clap::Args;

pub use braintrust_sdk_rust::{DEFAULT_API_URL, DEFAULT_APP_URL};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgValueSource {
    CommandLine,
    EnvVariable,
}

#[derive(Debug, Clone, Default, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(long, global = true)]
    pub json: bool,

    /// Increase output verbosity
    #[arg(long, short = 'v', env = "BRAINTRUST_VERBOSE", global = true, conflicts_with = "quiet", value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub verbose: bool,

    #[arg(skip)]
    pub verbose_source: Option<ArgValueSource>,

    /// Reduce interactive UI output
    #[arg(long, short = 'q', env = "BRAINTRUST_QUIET", global = true, value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub quiet: bool,

    #[arg(skip)]
    pub quiet_source: Option<ArgValueSource>,

    /// Disable ANSI color output
    #[arg(long, env = "BRAINTRUST_NO_COLOR", global = true, value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub no_color: bool,

    /// Disable all interactive prompts
    #[arg(long, env = "BRAINTRUST_NO_INPUT", global = true, value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub no_input: bool,

    /// Override active org (or via BRAINTRUST_ORG_NAME)
    #[arg(short = 'o', long = "org", env = "BRAINTRUST_ORG_NAME", global = true, value_parser = parse_org_name)]
    pub org_name: Option<String>,

    #[arg(skip)]
    pub org_name_source: Option<ArgValueSource>,

    /// Override active project
    #[arg(
        short = 'p',
        long,
        env = "BRAINTRUST_DEFAULT_PROJECT",
        hide_env_values = true,
        global = true
    )]
    pub project: Option<String>,

    #[arg(skip)]
    pub project_source: Option<ArgValueSource>,

    /// Override stored API key (or via BRAINTRUST_API_KEY)
    #[arg(long, env = "BRAINTRUST_API_KEY", global = true, hide = true)]
    pub api_key: Option<String>,

    #[arg(skip)]
    pub api_key_source: Option<ArgValueSource>,

    /// Exact auth slot selected internally by switch/init.
    #[arg(skip)]
    pub pinned_auth_slot: Option<String>,

    /// Prefer API key credentials for the selected org when available.
    #[arg(long = "prefer-api-key", env = "BRAINTRUST_PREFER_API_KEY", global = true, value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub prefer_api_key: bool,

    /// Override API URL (or via BRAINTRUST_API_URL)
    #[arg(
        long,
        env = "BRAINTRUST_API_URL",
        hide_env_values = true,
        global = true
    )]
    pub api_url: Option<String>,

    /// Override app URL (or via BRAINTRUST_APP_URL)
    #[arg(
        long,
        env = "BRAINTRUST_APP_URL",
        hide_env_values = true,
        global = true
    )]
    pub app_url: Option<String>,

    /// Path to a PEM-encoded CA bundle used for HTTPS requests.
    #[arg(
        long = "ca-cert",
        env = "BRAINTRUST_CA_CERT",
        hide_env_values = true,
        global = true
    )]
    pub ca_cert: Option<PathBuf>,

    /// Path to a .env file to load before running commands.
    #[arg(
        long,
        env = "BRAINTRUST_ENV_FILE",
        hide_env_values = true,
        global = true
    )]
    pub env_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct CLIArgs<T: Args> {
    #[command(flatten)]
    pub args: T,

    #[command(flatten, next_help_heading = "Global options")]
    pub base: BaseArgs,
}

fn parse_org_name(value: &str) -> Result<String, String> {
    Ok(crate::config::normalize_org(value).to_string())
}

pub(crate) fn custom_api_without_app_url(api_url: Option<&str>, app_url: Option<&str>) -> bool {
    app_url.is_none_or(|url| url.trim().is_empty())
        && api_url.is_some_and(|url| {
            !url.trim()
                .trim_end_matches('/')
                .eq_ignore_ascii_case(DEFAULT_API_URL.trim_end_matches('/'))
        })
}

impl BaseArgs {
    pub fn ca_cert(&self) -> Option<&Path> {
        self.ca_cert.as_deref()
    }

    pub fn verbose_explicit(&self) -> bool {
        self.verbose && self.verbose_source.is_some()
    }
}

#[cfg(test)]
mod tests {
    use super::{custom_api_without_app_url, parse_org_name, DEFAULT_API_URL};

    #[test]
    fn org_normalization() {
        for (input, expected) in [
            ("cross-org", ""),
            ("   ", ""),
            (" test-org ", "test-org"),
            (" org_test_123 ", "org_test_123"),
        ] {
            assert_eq!(parse_org_name(input).unwrap(), expected);
        }
        assert!(custom_api_without_app_url(
            Some("https://api.example.test"),
            None
        ));
        assert!(!custom_api_without_app_url(Some(DEFAULT_API_URL), None));
        assert!(!custom_api_without_app_url(
            Some("https://api.example.test"),
            Some("https://app.example.test")
        ));
    }
}
