use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::Args;
use tokio::process::Command;

use crate::args::BaseArgs;

#[derive(Debug, Clone, Args)]
pub struct EvalArgs {
    /// One or more eval files to execute (e.g. foo.eval.ts)
    #[arg(required = true)]
    pub files: Vec<String>,
}

pub async fn run(base: BaseArgs, args: EvalArgs) -> Result<()> {
    let runner = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("scripts")
        .join("eval-runner.ts");

    let mut cmd = if let Some(tsx_path) = find_tsx_binary() {
        let mut command = Command::new(tsx_path);
        command.arg(runner).args(&args.files);
        command
    } else {
        let mut command = Command::new("npx");
        command
            .arg("--yes")
            .arg("tsx")
            .arg(runner)
            .args(&args.files);
        command
    };

    cmd.envs(build_env(base));

    let status = cmd
        .status()
        .await
        .context("failed to start eval runner (npx tsx)")?;

    if !status.success() {
        anyhow::bail!("eval runner exited with status {status}");
    }

    Ok(())
}

fn build_env(base: BaseArgs) -> Vec<(String, String)> {
    let mut envs = Vec::new();
    if let Some(api_key) = base.api_key {
        envs.push(("BRAINTRUST_API_KEY".to_string(), api_key));
    }
    if let Some(api_url) = base.api_url {
        envs.push(("BRAINTRUST_API_URL".to_string(), api_url));
    }
    if let Some(project) = base.project {
        envs.push(("BRAINTRUST_DEFAULT_PROJECT".to_string(), project));
    }
    envs
}

fn find_tsx_binary() -> Option<PathBuf> {
    if let Ok(path) = std::env::var("BT_EVAL_TSX") {
        let candidate = PathBuf::from(path);
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    if let Some(paths) = std::env::var_os("PATH") {
        for dir in std::env::split_paths(&paths) {
            let candidate = dir.join("tsx");
            if candidate.is_file() {
                return Some(candidate);
            }
        }
    }

    let fallback = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("braintrust")
        .join("node_modules")
        .join(".bin")
        .join("tsx");
    if fallback.is_file() {
        return Some(fallback);
    }

    None
}
