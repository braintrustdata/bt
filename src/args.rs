use clap::Args;
use std::path::PathBuf;

pub use braintrust_sdk_rust::{DEFAULT_API_URL, DEFAULT_APP_URL};

#[derive(Debug, Clone, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(short = 'j', long, global = true)]
    pub json: bool,

    /// Use a saved login profile (or via BRAINTRUST_PROFILE)
    #[arg(long, env = "BRAINTRUST_PROFILE", global = true)]
    pub profile: Option<String>,

    // Override active org
    #[arg(short = 'o', long, env = "BRAINTRUST_DEFAULT_ORG")]
    pub org: Option<String>,

    /// Override active project
    #[arg(short = 'p', long, env = "BRAINTRUST_DEFAULT_PROJECT", global = true)]
    pub project: Option<String>,

    /// Override organization selection (or via BRAINTRUST_ORG_NAME)
    #[arg(long, env = "BRAINTRUST_ORG_NAME", global = true)]
    pub org_name: Option<String>,

    /// Override stored API key (or via BRAINTRUST_API_KEY)
    #[arg(long, env = "BRAINTRUST_API_KEY", global = true)]
    pub api_key: Option<String>,

    /// Prefer profile credentials even if BRAINTRUST_API_KEY/--api-key is set.
    #[arg(long, global = true)]
    pub prefer_profile: bool,

    /// Override API URL (or via BRAINTRUST_API_URL)
    #[arg(long, env = "BRAINTRUST_API_URL", global = true)]
    pub api_url: Option<String>,

    /// Override app URL (or via BRAINTRUST_APP_URL)
    #[arg(long, env = "BRAINTRUST_APP_URL", global = true)]
    pub app_url: Option<String>,

    /// Path to a .env file to load before running commands.
    #[arg(long, env = "BRAINTRUST_ENV_FILE")]
    pub env_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct CLIArgs<T: Args> {
    #[command(flatten)]
    pub base: BaseArgs,

    #[command(flatten)]
    pub args: T,
}
