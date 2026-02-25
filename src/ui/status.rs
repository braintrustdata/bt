use dialoguer::console::style;

use super::is_quiet;

pub enum CommandStatus {
    Success,
    Error,
    Warning,
}

pub fn print_command_status(status: CommandStatus, message: &str) {
    if is_quiet() {
        return;
    }

    let indicator = match &status {
        CommandStatus::Success => style("✓").green(),
        CommandStatus::Error => style("✗").red(),
        CommandStatus::Warning => style("!").dim(),
    };

    eprintln!("{indicator} {message}");
}
