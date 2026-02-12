use anyhow::{bail, Result};

use crate::ui::{print_command_status, CommandStatus};

fn resolve_target(global: bool, local: bool) -> Result<std::path::PathBuf> {
    if global {
        return super::global_path();
    }
    if local {
        return match super::local_path() {
            Some(p) => Ok(p),
            None => bail!("No local .bt directory found"),
        };
    }
    match super::write_target()? {
        super::WriteTarget::Global(p) | super::WriteTarget::Local(p) => Ok(p),
    }
}

pub fn run(key: &str, value: &str, global: bool, local: bool) -> Result<()> {
    let path = resolve_target(global, local)?;
    let mut cfg = super::load_file(&path);

    cfg.set_field(key, value.to_string());

    super::save_file(&path, &cfg)?;

    print_command_status(CommandStatus::Success, &format!("Set {key} = {value}"));
    Ok(())
}

pub fn unset(key: &str, global: bool, local: bool) -> Result<()> {
    let path = resolve_target(global, local)?;
    let mut cfg = super::load_file(&path);

    cfg.unset_field(key);

    super::save_file(&path, &cfg)?;

    print_command_status(CommandStatus::Success, &format!("Unset {key}"));
    Ok(())
}
