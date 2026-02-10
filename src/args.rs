use clap::Args;
use std::path::PathBuf;

pub use braintrust_sdk_rust::{DEFAULT_API_URL, DEFAULT_APP_URL};

use crate::config::Config;

#[derive(Debug, Clone, Args)]
pub struct BaseArgs {
    /// Output as JSON
    #[arg(short = 'j', long, global = true)]
    pub json: bool,

    /// Use a saved login profile (or via BRAINTRUST_PROFILE)
    #[arg(long, env = "BRAINTRUST_PROFILE", global = true)]
    pub profile: Option<String>,

    /// Override active org
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

impl BaseArgs {
    pub fn with_config_defaults(mut self, cfg: &Config) -> Self {
        if self.org.is_none() {
            self.org = cfg.org.clone();
        }
        if self.project.is_none() {
            self.project = cfg.project.clone();
        }
        self
    }
}

#[derive(Debug, Clone, Args)]
pub struct CLIArgs<T: Args> {
    #[command(flatten)]
    pub base: BaseArgs,

    #[command(flatten)]
    pub args: T,
}

impl<T: Args> CLIArgs<T> {
    pub fn with_config(self, cfg: &Config) -> (BaseArgs, T) {
        (self.base.with_config_defaults(cfg), self.args)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn empty_args() -> BaseArgs {
        BaseArgs {
            json: false,
            org: None,
            project: None,
            api_key: None,
            api_url: None,
            app_url: None,
            env_file: None,
        }
    }

    fn config(org: Option<&str>, project: Option<&str>) -> Config {
        Config {
            org: org.map(String::from),
            project: project.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn empty_args_filled_from_config() {
        let args = empty_args();
        let cfg = config(Some("cfg-org"), Some("cfg-proj"));

        let result = args.with_config_defaults(&cfg);

        assert_eq!(result.org, Some("cfg-org".into()));
        assert_eq!(result.project, Some("cfg-proj".into()));
    }

    #[test]
    fn existing_args_not_overwritten() {
        let args = BaseArgs {
            org: Some("cli-org".into()),
            project: Some("cli-proj".into()),
            ..empty_args()
        };
        let cfg = config(Some("cfg-org"), Some("cfg-proj"));

        let result = args.with_config_defaults(&cfg);

        assert_eq!(result.org, Some("cli-org".into()));
        assert_eq!(result.project, Some("cli-proj".into()));
    }

    #[test]
    fn partial_fill_org_set_project_from_config() {
        let args = BaseArgs {
            org: Some("cli-org".into()),
            project: None,
            ..empty_args()
        };
        let cfg = config(Some("cfg-org"), Some("cfg-proj"));

        let result = args.with_config_defaults(&cfg);

        assert_eq!(result.org, Some("cli-org".into()));
        assert_eq!(result.project, Some("cfg-proj".into()));
    }

    #[test]
    fn empty_config_leaves_args_unchanged() {
        let args = BaseArgs {
            org: Some("cli-org".into()),
            project: None,
            ..empty_args()
        };
        let cfg = config(None, None);

        let result = args.with_config_defaults(&cfg);

        assert_eq!(result.org, Some("cli-org".into()));
        assert_eq!(result.project, None);
    }
}
