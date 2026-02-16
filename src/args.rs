use std::path::PathBuf;

use clap::Args;

#[derive(Debug, Clone, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(short = 'j', long, global = true)]
    pub json: bool,

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
    #[arg(
        long,
        env = "BRAINTRUST_API_KEY",
        hide_env_values = true,
        global = true
    )]
    pub api_key: Option<String>,

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

    /// Override organization name (or via BRAINTRUST_DEFAULT_ORG)
    #[arg(
        short = 'o',
        long,
        env = "BRAINTRUST_DEFAULT_ORG",
        hide_env_values = true,
        global = true
    )]
    pub org: Option<String>,

    /// Path to a .env file to load before running commands.
    #[arg(long, env = "BRAINTRUST_ENV_FILE", hide_env_values = true)]
    pub env_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
pub struct CLIArgs<T: Args> {
    #[command(flatten)]
    pub base: BaseArgs,

    #[command(flatten)]
    pub args: T,
}
