use dialoguer::console::style;

pub enum CommandStatus {
    Success,
    Error,
}

pub fn print_command_status(status: CommandStatus, message: &str) {
    let indicator = match status {
        CommandStatus::Success => style("✓").green(),
        CommandStatus::Error => style("✗").red(),
    };
    println!("{indicator} {message}");
}
