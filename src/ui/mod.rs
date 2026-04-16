use std::io::IsTerminal;
use std::sync::atomic::{AtomicBool, Ordering};

use dialoguer::console::Term;

mod pager;
pub mod prompt_render;
mod select;
mod spinner;
mod status;
mod table;

static NO_INPUT: AtomicBool = AtomicBool::new(false);
static QUIET: AtomicBool = AtomicBool::new(false);
static ANIMATIONS_ENABLED: AtomicBool = AtomicBool::new(true);

pub fn set_no_input(val: bool) {
    NO_INPUT.store(val, Ordering::Relaxed);
}

pub fn set_quiet(val: bool) {
    QUIET.store(val, Ordering::Relaxed);
}

pub fn is_quiet() -> bool {
    QUIET.load(Ordering::Relaxed)
}

pub fn set_animations_enabled(val: bool) {
    ANIMATIONS_ENABLED.store(val, Ordering::Relaxed);
}

pub fn animations_enabled() -> bool {
    ANIMATIONS_ENABLED.load(Ordering::Relaxed)
}

pub fn prompt_term() -> Option<Term> {
    if NO_INPUT.load(Ordering::Relaxed) {
        return None;
    }

    select::tty_term()
}

pub fn can_prompt() -> bool {
    prompt_term().is_some()
}

pub fn is_interactive() -> bool {
    std::io::stdin().is_terminal() && !NO_INPUT.load(Ordering::Relaxed)
}

pub use pager::print_with_pager;
pub use select::{fuzzy_select, select_project_interactive};

pub use spinner::{with_spinner, with_spinner_visible};
pub use status::{print_command_status, CommandStatus};
pub use table::{apply_column_padding, header, styled_table, truncate};
