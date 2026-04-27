use std::ffi::OsString;
use std::path::{Path, PathBuf};

use clap::Args;

pub use braintrust_sdk_rust::{DEFAULT_API_URL, DEFAULT_APP_URL};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArgValueSource {
    CommandLine,
    EnvVariable,
}

#[derive(Debug, Clone, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(long, global = true)]
    pub json: bool,

    /// Increase output verbosity
    #[arg(long, short = 'v', env = "BRAINTRUST_VERBOSE", global = true, conflicts_with = "quiet", value_parser = clap::builder::BoolishValueParser::new(), default_value_t = false)]
    pub verbose: bool,

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

    /// Use a saved login profile (or via BRAINTRUST_PROFILE)
    #[arg(long, env = "BRAINTRUST_PROFILE", global = true)]
    pub profile: Option<String>,

    #[arg(skip = false)]
    pub profile_explicit: bool,

    /// Override active org (or via BRAINTRUST_ORG_NAME)
    #[arg(short = 'o', long = "org", env = "BRAINTRUST_ORG_NAME", global = true)]
    pub org_name: Option<String>,

    /// Override active project
    #[arg(
        short = 'p',
        long,
        env = "BRAINTRUST_DEFAULT_PROJECT",
        hide_env_values = true,
        global = true
    )]
    pub project: Option<String>,

    /// Override stored API key (or via BRAINTRUST_API_KEY)
    #[arg(long, env = "BRAINTRUST_API_KEY", global = true, hide = true)]
    pub api_key: Option<String>,

    #[arg(skip)]
    pub api_key_source: Option<ArgValueSource>,

    /// Prefer profile credentials even if BRAINTRUST_API_KEY/--api-key is set.
    #[arg(long, global = true)]
    pub prefer_profile: bool,

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
    pub base: BaseArgs,

    #[command(flatten)]
    pub args: T,
}

impl BaseArgs {
    pub fn ca_cert(&self) -> Option<&Path> {
        self.ca_cert.as_deref()
    }
}

pub fn has_explicit_profile_arg(args: &[OsString]) -> bool {
    let mut idx = 1usize;
    while idx < args.len() {
        let Some(arg) = args[idx].to_str() else {
            idx += 1;
            continue;
        };

        if arg == "--" {
            break;
        }

        if arg == "--profile" || arg.starts_with("--profile=") {
            return true;
        }

        idx += 1;
    }

    false
}

#[cfg(test)]
mod tests {
    use super::has_explicit_profile_arg;
    use std::ffi::OsString;

    #[test]
    fn has_explicit_profile_arg_detects_split_flag() {
        let args = vec![
            OsString::from("bt"),
            OsString::from("status"),
            OsString::from("--profile"),
            OsString::from("work"),
        ];
        assert!(has_explicit_profile_arg(&args));
    }

    #[test]
    fn has_explicit_profile_arg_detects_equals_flag() {
        let args = vec![
            OsString::from("bt"),
            OsString::from("status"),
            OsString::from("--profile=work"),
        ];
        assert!(has_explicit_profile_arg(&args));
    }

    #[test]
    fn has_explicit_profile_arg_ignores_passthrough_args() {
        let args = vec![
            OsString::from("bt"),
            OsString::from("eval"),
            OsString::from("--"),
            OsString::from("--profile"),
            OsString::from("work"),
        ];
        assert!(!has_explicit_profile_arg(&args));
    }
}
