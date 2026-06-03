use std::ffi::OsString;
use std::io::Cursor;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::Args;
use flate2::read::GzDecoder;
use tar::Archive;
use tokio::process::Command;

use crate::args::BaseArgs;
use crate::utils::write_bytes_atomic;

const CLI_MJS: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/spark/cli.mjs"));
const HARNESS_TGZ: &[u8] = include_bytes!(concat!(env!("OUT_DIR"), "/spark/harness.tgz"));
const ASSET_HASH: &str = include_str!(concat!(env!("OUT_DIR"), "/spark/asset_hash"));

const BT_VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Debug, Clone, Args)]
#[command(
    about = "Run the embedded spark wizard",
    after_help = "Arguments after `bt spark` are forwarded verbatim to the spark CLI.\n\
                  Example: bt spark --help",
    disable_help_flag = true,
    trailing_var_arg = true,
    allow_hyphen_values = true
)]
pub struct SparkArgs {
    /// Arguments forwarded to the embedded spark CLI.
    #[arg(num_args = 0.., value_name = "SPARK_ARG")]
    pub forwarded: Vec<OsString>,
}

pub async fn run(base: BaseArgs, args: SparkArgs) -> Result<()> {
    if !is_embedded() {
        bail!(
            "spark was not embedded in this build. Build spark first (`pnpm build:sea` in ../spark), \
             or rebuild bt with BT_SPARK_DIR pointing to a built spark checkout."
        );
    }

    let cache_dir = resolve_cache_dir()?;
    let cli_path = cache_dir.join("cli.mjs");
    let harness_root = cache_dir.join("spark-harness");
    let harness_bin = harness_root.join("bin").join("spark-harness.mjs");

    materialize_cli(&cli_path)?;
    materialize_harness(&cache_dir, &harness_root, &harness_bin)?;

    let mut command = Command::new("node");
    command.arg(&cli_path);
    for arg in &args.forwarded {
        command.arg(arg);
    }

    forward_braintrust_env(&mut command, &base);
    command.env("BT_WIZARD_HARNESS_BIN", &harness_bin);

    let status = command.status().await.context(NODE_SPAWN_HINT)?;

    if !status.success() {
        let code = status.code().unwrap_or(1);
        std::process::exit(code);
    }
    Ok(())
}

fn is_embedded() -> bool {
    !CLI_MJS.is_empty() && !HARNESS_TGZ.is_empty()
}

fn resolve_cache_dir() -> Result<PathBuf> {
    let base = dirs::cache_dir()
        .or_else(|| dirs::home_dir().map(|h| h.join(".cache")))
        .context("could not resolve a user cache directory for spark")?;
    let asset_hash = ASSET_HASH.trim();
    Ok(base
        .join("bt")
        .join("spark")
        .join(format!("{BT_VERSION}-{asset_hash}")))
}

fn materialize_cli(cli_path: &Path) -> Result<()> {
    if cli_matches(cli_path) {
        return Ok(());
    }
    if let Some(parent) = cli_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create spark cache dir {}", parent.display()))?;
    }
    write_bytes_atomic(cli_path, CLI_MJS)
        .with_context(|| format!("failed to write spark cli.mjs at {}", cli_path.display()))
}

fn cli_matches(cli_path: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(cli_path) else {
        return false;
    };
    metadata.len() == CLI_MJS.len() as u64
}

fn materialize_harness(cache_dir: &Path, harness_root: &Path, harness_bin: &Path) -> Result<()> {
    if harness_bin.is_file() {
        return Ok(());
    }

    std::fs::create_dir_all(cache_dir)
        .with_context(|| format!("failed to create spark cache dir {}", cache_dir.display()))?;

    let staging = tempfile::Builder::new()
        .prefix(".spark-harness-stage-")
        .tempdir_in(cache_dir)
        .with_context(|| format!("failed to create staging dir in {}", cache_dir.display()))?;

    let decoder = GzDecoder::new(Cursor::new(HARNESS_TGZ));
    let mut archive = Archive::new(decoder);
    archive
        .unpack(staging.path())
        .context("failed to unpack embedded spark harness tarball")?;

    let staged_root = staging.path().join("spark-harness");
    if !staged_root.is_dir() {
        bail!("embedded spark harness archive did not contain a spark-harness/ root directory");
    }

    if harness_root.exists() {
        return Ok(());
    }

    match std::fs::rename(&staged_root, harness_root) {
        Ok(()) => Ok(()),
        Err(_) if harness_bin.is_file() => Ok(()),
        Err(err) => Err(err).with_context(|| {
            format!(
                "failed to move staged harness into {}",
                harness_root.display()
            )
        }),
    }
}

fn forward_braintrust_env(command: &mut Command, base: &BaseArgs) {
    if let Some(api_url) = &base.api_url {
        command.env("BRAINTRUST_API_URL", api_url);
    }
    if let Some(app_url) = &base.app_url {
        command.env("BRAINTRUST_APP_URL", app_url);
    }
    if let Some(api_key) = &base.api_key {
        command.env("BRAINTRUST_API_KEY", api_key);
    }
    if let Some(profile) = &base.profile {
        command.env("BRAINTRUST_PROFILE", profile);
    }
    if let Some(org) = &base.org_name {
        command.env("BRAINTRUST_ORG_NAME", org);
    }
    if let Some(project) = &base.project {
        command.env("BRAINTRUST_DEFAULT_PROJECT", project);
    }
    if let Some(ca_cert) = base.ca_cert() {
        command.env("BRAINTRUST_CA_CERT", ca_cert);
    }
}

const NODE_SPAWN_HINT: &str =
    "failed to spawn `node` for the embedded spark CLI; install Node.js (>= 22) and ensure `node` is on PATH";
