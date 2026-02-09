use clap::Args;
use std::path::PathBuf;

#[derive(Debug, Clone, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(short = 'j', long)]
    pub json: bool,

    // Override active org
    #[arg(short = 'o', long, env = "BRAINTRUST_DEFAULT_ORG")]
    pub org: Option<String>,

    /// Override active project
    #[arg(short = 'p', long, env = "BRAINTRUST_DEFAULT_PROJECT")]
    pub project: Option<String>,

    /// Override stored API key (or via BRAINTRUST_API_KEY)
    #[arg(long, env = "BRAINTRUST_API_KEY")]
    pub api_key: Option<String>,

    /// Override API URL (or via BRAINTRUST_API_URL)
    #[arg(long, env = "BRAINTRUST_API_URL")]
    pub api_url: Option<String>,

    /// Override app URL (or via BRAINTRUST_APP_URL)
    #[arg(long, env = "BRAINTRUST_APP_URL")]
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
