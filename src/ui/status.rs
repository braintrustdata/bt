use dialoguer::console::style;

pub enum CommandStatus {
    Success,
    Error,
    Warning,
}

pub fn print_command_status(status: CommandStatus, message: &str) {
    let indicator = match &status {
        CommandStatus::Success => style("✓").green(),
        CommandStatus::Error => style("✗").red(),
        CommandStatus::Warning => style("!").dim(),
    };

    eprintln!("{indicator} {message}");
}
