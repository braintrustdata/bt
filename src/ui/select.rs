use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::console::{style, Key, Term};
use dialoguer::{theme::ColorfulTheme, FuzzySelect, Input};
use fuzzy_matcher::{skim::SkimMatcherV2, FuzzyMatcher};

const MAX_VISIBLE_ITEMS: usize = 12;

use crate::{
    http::ApiClient,
    projects::{
        api,
        create::{create_project_checked, CreateProjectOutcome},
    },
    ui::with_spinner,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectSelectMode {
    ExistingOnly,
    #[allow(dead_code)]
    AllowCreateWithDefaultProjectNote,
}

/// Open a Term for interactive prompts.
///
/// Prefers stderr and falls back to `/dev/tty` when available so prompts still
/// work when stdin/stderr are redirected.
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

/// Fuzzy select from a list of items. Requires TTY.
pub fn fuzzy_select<T: ToString>(prompt: &str, items: &[T], default: usize) -> Result<usize> {
    let Some(selection) = fuzzy_select_opt(prompt, items, default)? else {
        bail!("selection cancelled by user");
    };
    Ok(selection)
}

/// Fuzzy select from a list of items, returning `None` when the user cancels.
pub fn fuzzy_select_opt<T: ToString>(
    prompt: &str,
    items: &[T],
    default: usize,
) -> Result<Option<usize>> {
    let Some(term) = super::prompt_term() else {
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
        .max_length(MAX_VISIBLE_ITEMS)
        .interact_on_opt(&term)?;

    Ok(selection)
}

enum PinnedSelectResult {
    /// User selected the pinned item; the String is what they typed in the search box.
    Pinned(String),
    /// User selected the item at this index in the original (unfiltered) items slice.
    Item(usize),
}

/// Fuzzy select where the first item is always pinned (visible regardless of search text).
///
/// `items` are the filterable choices. `pinned_label` is always shown at position 0.
/// `default_sel` is the initially-highlighted row (0 = pinned, k = items[k-1]).
fn fuzzy_select_with_pinned_first(
    prompt: &str,
    items: &[String],
    pinned_label: &str,
    max_length: usize,
    default_sel: usize,
) -> Result<Option<PinnedSelectResult>> {
    let Some(term) = super::prompt_term() else {
        bail!("interactive mode requires TTY");
    };

    let matcher = SkimMatcherV2::default();
    let mut search_term = String::new();
    let mut sel = default_sel;
    let mut starting_row: usize = 0;
    // max visible filtered items = max_length - 1 (one slot reserved for pinned)
    let max_filtered_visible = max_length.saturating_sub(1);
    let mut lines_drawn: usize = 0;

    term.hide_cursor()?;
    // Restore the cursor on any exit path (return, ?, or SIGINT drop).
    struct ShowCursorOnDrop<'a>(&'a Term);
    impl Drop for ShowCursorOnDrop<'_> {
        fn drop(&mut self) {
            let _ = self.0.show_cursor();
        }
    }
    let _guard = ShowCursorOnDrop(&term);

    loop {
        // Build the filtered+sorted list of (original_index, display_name).
        let filtered: Vec<(usize, &String)> = if search_term.is_empty() {
            items.iter().enumerate().collect()
        } else {
            let mut scored: Vec<(i64, usize, &String)> = items
                .iter()
                .enumerate()
                .filter_map(|(i, item)| {
                    matcher
                        .fuzzy_match(item, &search_term)
                        .map(|score| (score, i, item))
                })
                .collect();
            scored.sort_unstable_by(|(s1, ..), (s2, ..)| s2.cmp(s1));
            scored.into_iter().map(|(_, i, item)| (i, item)).collect()
        };

        let total = 1 + filtered.len(); // 1 for the always-visible pinned item

        // Clamp sel and starting_row to valid ranges.
        if sel >= total {
            sel = total.saturating_sub(1);
        }
        let max_starting_row = filtered.len().saturating_sub(max_filtered_visible);
        if starting_row > max_starting_row {
            starting_row = max_starting_row;
        }

        let visible_filtered =
            max_filtered_visible.min(filtered.len().saturating_sub(starting_row));

        // Clear lines from the previous render before drawing the new frame.
        if lines_drawn > 0 {
            term.clear_last_lines(lines_drawn)?;
        }

        // Prompt line.
        term.write_line(&format!(
            "{} {} › {}",
            style("?").cyan().bold(),
            prompt,
            search_term
        ))?;

        // Pinned item (always at the top).
        if sel == 0 {
            term.write_line(&format!(
                "  {} {}",
                style("❯").green().bold(),
                style(pinned_label).green()
            ))?;
        } else {
            term.write_line(&format!("    {}", pinned_label))?;
        }

        // Filtered project items.
        for display_idx in 0..visible_filtered {
            let filtered_idx = starting_row + display_idx;
            let (_, item) = filtered[filtered_idx];
            // sel == 0 is pinned; sel == k means filtered[k-1] is selected.
            if sel == filtered_idx + 1 {
                term.write_line(&format!(
                    "  {} {}",
                    style("❯").green().bold(),
                    style(item.as_str()).green()
                ))?;
            } else {
                term.write_line(&format!("    {}", item))?;
            }
        }

        term.flush()?;
        lines_drawn = 1 + 1 + visible_filtered; // prompt + pinned + filtered

        match term.read_key()? {
            Key::Escape | Key::CtrlC => {
                term.clear_last_lines(lines_drawn)?;
                return Ok(None);
            }
            Key::Enter => {
                term.clear_last_lines(lines_drawn)?;
                if sel == 0 {
                    return Ok(Some(PinnedSelectResult::Pinned(search_term)));
                }
                let (orig_idx, _) = filtered[sel - 1];
                return Ok(Some(PinnedSelectResult::Item(orig_idx)));
            }
            Key::ArrowUp | Key::BackTab => {
                if sel == 0 {
                    // Wrap from pinned to the last filtered item.
                    sel = total - 1;
                    if sel > 0 {
                        starting_row =
                            (sel - 1).saturating_sub(max_filtered_visible.saturating_sub(1));
                    }
                } else {
                    sel -= 1;
                    if sel == 0 {
                        starting_row = 0;
                    } else if sel - 1 < starting_row {
                        starting_row = sel - 1;
                    }
                }
            }
            Key::ArrowDown | Key::Tab => {
                sel = (sel + 1) % total;
                if sel == 0 || sel == 1 {
                    // Wrapped to pinned, or moved from pinned to first filtered item.
                    starting_row = 0;
                } else if sel > starting_row + max_filtered_visible {
                    starting_row += 1;
                }
            }
            Key::Backspace if !search_term.is_empty() => {
                search_term.pop();
                sel = 0;
                starting_row = 0;
            }
            Key::Char(c) if !c.is_ascii_control() => {
                search_term.push(c);
                sel = 0;
                starting_row = 0;
            }
            _ => {}
        }
    }
}

/// Interactive selector for project data.
pub async fn select_project(
    client: &ApiClient,
    current: Option<&str>,
    select_label: Option<&str>,
    mode: ProjectSelectMode,
) -> Result<api::Project> {
    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;
    projects.sort_by(|a, b| a.name.cmp(&b.name));

    let label = select_label.unwrap_or("Select project");

    if mode_allows_create(mode) {
        let names = project_display_names(&projects, mode);
        let default_sel = default_project_selection(&projects, current, mode)?;

        return match fuzzy_select_with_pinned_first(
            label,
            &names,
            "+ Create new project",
            MAX_VISIBLE_ITEMS,
            default_sel,
        )? {
            None => bail!("selection cancelled by user"),
            Some(PinnedSelectResult::Item(orig_idx)) => Ok(projects[orig_idx].clone()),
            Some(PinnedSelectResult::Pinned(search_term)) => {
                let name = if search_term.trim().is_empty() {
                    // Nothing typed — fall back to prompting.
                    let default_name = default_new_project_name();
                    let n: String = Input::with_theme(&ColorfulTheme::default())
                        .with_prompt("Project name")
                        .default(default_name)
                        .interact_text_on(
                            &super::prompt_term()
                                .ok_or_else(|| anyhow::anyhow!("interactive mode requires TTY"))?,
                        )?;
                    n
                } else {
                    search_term.trim().to_string()
                };
                if name.trim().is_empty() {
                    bail!("project name cannot be empty");
                }
                match create_project_checked(client, name.trim()).await? {
                    CreateProjectOutcome::Created(project)
                    | CreateProjectOutcome::Existing(project) => Ok(project),
                }
            }
        };
    }

    // ExistingOnly: use the standard fuzzy select.
    let names = project_selection_labels(&projects);
    let default = default_project_selection(&projects, current, mode)?;
    let selection = fuzzy_select(label, &names, default)?;
    Ok(projects[selection].clone())
}

/// Display names for the filterable project list (without the pinned create option).
fn project_display_names(projects: &[api::Project], mode: ProjectSelectMode) -> Vec<String> {
    let show_default_project_note =
        matches!(mode, ProjectSelectMode::AllowCreateWithDefaultProjectNote)
            && projects.len() == 1
            && projects[0].name == "My Project";

    projects
        .iter()
        .map(|project| {
            if show_default_project_note && project.name == "My Project" {
                "My Project (default starter project)".to_string()
            } else {
                project.name.clone()
            }
        })
        .collect()
}

fn project_selection_labels(projects: &[api::Project]) -> Vec<String> {
    projects
        .iter()
        .map(|project| project.name.clone())
        .collect()
}

fn default_project_selection(
    projects: &[api::Project],
    current: Option<&str>,
    mode: ProjectSelectMode,
) -> Result<usize> {
    if projects.is_empty() {
        if mode_allows_create(mode) {
            return Ok(0);
        }
        bail!("no projects found");
    }

    Ok(current
        .and_then(|c| projects.iter().position(|project| project.name == c))
        .map(|idx| {
            if mode_allows_create(mode) {
                idx + 1
            } else {
                idx
            }
        })
        .unwrap_or(0))
}

fn mode_allows_create(mode: ProjectSelectMode) -> bool {
    matches!(mode, ProjectSelectMode::AllowCreateWithDefaultProjectNote)
}

fn default_new_project_name() -> String {
    let output = std::process::Command::new("whoami").output();
    let user = output
        .ok()
        .filter(|result| result.status.success())
        .and_then(|result| String::from_utf8(result.stdout).ok())
        .map(|stdout| stdout.trim().to_string())
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "braintrust".to_string());
    format!("{user}-project")
}

#[cfg(test)]
mod tests {
    use super::{
        default_new_project_name, default_project_selection, project_display_names,
        project_selection_labels, ProjectSelectMode,
    };
    use crate::projects::api::Project;

    fn project(name: &str) -> Project {
        Project {
            id: format!("id-{name}"),
            name: name.to_string(),
            org_id: "org".to_string(),
            description: None,
        }
    }

    #[test]
    fn default_project_selection_prefers_current_project() {
        let projects = vec![project("alpha"), project("beta")];
        assert_eq!(
            default_project_selection(&projects, Some("beta"), ProjectSelectMode::ExistingOnly)
                .expect("default selection"),
            1
        );
    }

    #[test]
    fn default_project_selection_falls_back_to_first_project() {
        let projects = vec![project("alpha"), project("beta")];
        assert_eq!(
            default_project_selection(&projects, Some("missing"), ProjectSelectMode::ExistingOnly)
                .expect("default selection"),
            0
        );
    }

    #[test]
    fn default_project_selection_rejects_empty_project_list() {
        let err = default_project_selection(&[], None, ProjectSelectMode::ExistingOnly)
            .expect_err("empty projects should fail");
        assert!(err.to_string().contains("no projects found"));
    }

    #[test]
    fn project_selection_labels_returns_project_names() {
        let labels = project_selection_labels(&[project("alpha")]);
        assert_eq!(labels, vec!["alpha".to_string()]);
    }

    #[test]
    fn allow_create_defaults_to_create_when_project_list_is_empty() {
        assert_eq!(
            default_project_selection(
                &[],
                None,
                ProjectSelectMode::AllowCreateWithDefaultProjectNote,
            )
            .expect("default selection"),
            0
        );
    }

    #[test]
    fn default_new_project_name_has_project_suffix() {
        assert!(default_new_project_name().ends_with("-project"));
    }

    #[test]
    fn project_display_names_returns_names_without_create_option() {
        let names = project_display_names(
            &[project("alpha"), project("beta")],
            ProjectSelectMode::AllowCreateWithDefaultProjectNote,
        );
        assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);
    }

    #[test]
    fn project_display_names_annotates_my_project_when_it_is_the_only_project() {
        let names = project_display_names(
            &[project("My Project")],
            ProjectSelectMode::AllowCreateWithDefaultProjectNote,
        );
        assert_eq!(
            names,
            vec!["My Project (default starter project)".to_string()]
        );
    }

    #[test]
    fn project_display_names_hides_note_when_there_are_multiple_projects() {
        let names = project_display_names(
            &[project("My Project"), project("alpha")],
            ProjectSelectMode::AllowCreateWithDefaultProjectNote,
        );
        assert_eq!(names, vec!["My Project".to_string(), "alpha".to_string()]);
    }
}
