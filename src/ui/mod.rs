use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};

mod pager;
mod select;
mod spinner;
mod status;
mod table;

static NO_INPUT: AtomicBool = AtomicBool::new(false);

pub fn set_no_input(val: bool) {
    NO_INPUT.store(val, Ordering::Relaxed);
}

pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal() && !NO_INPUT.load(Ordering::Relaxed)
}

pub use pager::print_with_pager;
pub use select::{fuzzy_select, select_project_interactive};

pub use spinner::{with_spinner, with_spinner_visible};
pub use status::{print_command_status, CommandStatus};
pub use table::{apply_column_padding, header, styled_table, truncate};
