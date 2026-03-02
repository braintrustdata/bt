use anyhow::Result;

use crate::ui::{print_command_status, CommandStatus};

pub fn run(key: &str, value: &str, global: bool, local: bool) -> Result<()> {
    let path = super::resolve_write_path(global, local)?;
    let value_owned = value.to_string();
    super::update_file_with_lock(&path, move |cfg| {
        cfg.set_field(key, value_owned.clone());
        true
    })?;

    print_command_status(CommandStatus::Success, &format!("Set {key} = {value}"));
    Ok(())
}

pub fn unset(key: &str, global: bool, local: bool) -> Result<()> {
    let path = super::resolve_write_path(global, local)?;
    super::update_file_with_lock(&path, |cfg| {
        cfg.unset_field(key);
        true
    })?;

    print_command_status(CommandStatus::Success, &format!("Unset {key}"));
    Ok(())
}
