use anyhow::Result;

use crate::ui::{print_command_status, CommandStatus};

pub fn run(key: &str, value: &str, global: bool, local: bool) -> Result<()> {
    let path = super::resolve_write_path(global, local)?;
    let mut cfg = super::load_file(&path);

    cfg.set_field(key, value.to_string());

    super::save_file(&path, &cfg)?;

    print_command_status(CommandStatus::Success, &format!("Set {key} = {value}"));
    Ok(())
}

pub fn unset(key: &str, global: bool, local: bool) -> Result<()> {
    let path = super::resolve_write_path(global, local)?;
    let mut cfg = super::load_file(&path);

    cfg.unset_field(key);

    super::save_file(&path, &cfg)?;

    print_command_status(CommandStatus::Success, &format!("Unset {key}"));
    Ok(())
}
