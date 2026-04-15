use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::console::Term;
use dialoguer::{theme::ColorfulTheme, FuzzySelect};

use crate::{http::ApiClient, projects::api, ui::with_spinner};

/// Open a Term for interactive prompts.
///
/// Prefers stderr (already a TTY in the common case). Falls back to `/dev/tty`
/// so that prompts still work when stdin/stderr are redirected — e.g. when bt
/// is invoked from a shell script: `echo "bt setup" | sh`.
///
/// Returns `None` when no interactive terminal is available at all (headless CI).
pub(crate) fn tty_term() -> Option<Term> {
    if std::io::stderr().is_terminal() {
        return Some(Term::stderr());
    }
    #[cfg(unix)]
    {
        use std::fs::OpenOptions;
        if let Ok(tty) = OpenOptions::new().read(true).write(true).open("/dev/tty") {
            if let Ok(tty2) = tty.try_clone() {
                return Some(Term::read_write_pair(tty, tty2));
            }
        }
    }
    None
}

/// Fuzzy select from a list of items. Requires an interactive terminal.
/// Works even when stdin is piped (e.g. `echo "bt setup" | sh`) because
/// it falls back to /dev/tty for both display and keyboard input.
pub fn fuzzy_select<T: ToString>(prompt: &str, items: &[T], default: usize) -> Result<usize> {
    let Some(selection) = fuzzy_select_opt(prompt, items, default)? else {
        bail!("selection cancelled by user");
    };
    Ok(selection)
}

/// Fuzzy select from a list of items, returning `None` when the user cancels.
/// Uses the same TTY fallback behavior as [`fuzzy_select`].
pub fn fuzzy_select_opt<T: ToString>(
    prompt: &str,
    items: &[T],
    default: usize,
) -> Result<Option<usize>> {
    let Some(term) = tty_term() else {
        bail!("interactive mode requires TTY");
    };

    if items.is_empty() {
        bail!("no items to select from");
    }

    let labels: Vec<String> = items.iter().map(|i| i.to_string()).collect();

    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(default)
        .max_length(12)
        .interact_on_opt(&term)?;

    Ok(selection)
}

/// Interactive selector for project data
pub async fn select_project_interactive(
    client: &ApiClient,
    select_label: Option<&str>,
    current: Option<&str>,
) -> Result<String> {
    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if projects.is_empty() {
        bail!("no projects found in org '{}'", &client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();
    let default = current
        .and_then(|c| names.iter().position(|n| *n == c))
        .unwrap_or(0);

    let label = select_label.unwrap_or("Select project");
    let selection = fuzzy_select(label, &names, default)?;
    Ok(projects[selection].name.clone())
}
