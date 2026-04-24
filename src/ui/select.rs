use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::console::Term;
use dialoguer::{theme::ColorfulTheme, FuzzySelect, Input};

use crate::{
    http::ApiClient,
    projects::{api, create::create_project_checked},
    ui::with_spinner,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectSelectMode {
    ExistingOnly,
    AllowCreate,
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
        .max_length(12)
        .interact_on_opt(&term)?;

    Ok(selection)
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

    let names = project_selection_labels(&projects, mode);
    let default = default_project_selection(&projects, current, mode)?;
    let label = select_label.unwrap_or("Select project");
    let selection = fuzzy_select(label, &names, default)?;

    if matches!(mode, ProjectSelectMode::AllowCreate) && selection == 0 {
        let default_name = default_new_project_name();
        let name: String = Input::with_theme(&ColorfulTheme::default())
            .with_prompt("Project name")
            .default(default_name)
            .interact_text_on(
                &super::prompt_term()
                    .ok_or_else(|| anyhow::anyhow!("interactive mode requires TTY"))?,
            )?;
        let trimmed = name.trim();
        if trimmed.is_empty() {
            bail!("project name cannot be empty");
        }
        return create_project_checked(client, trimmed).await;
    }

    let project_index = selected_project_index(selection, mode);
    Ok(projects[project_index].clone())
}

fn project_selection_labels(projects: &[api::Project], mode: ProjectSelectMode) -> Vec<String> {
    if matches!(mode, ProjectSelectMode::AllowCreate) {
        let mut labels = vec!["+ Create new project".to_string()];
        labels.extend(projects.iter().map(|project| project.name.clone()));
        return labels;
    }
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
        if matches!(mode, ProjectSelectMode::AllowCreate) {
            return Ok(0);
        }
        bail!("no projects found");
    }

    Ok(current
        .and_then(|c| projects.iter().position(|project| project.name == c))
        .map(|idx| {
            if matches!(mode, ProjectSelectMode::AllowCreate) {
                idx + 1
            } else {
                idx
            }
        })
        .unwrap_or(0))
}

fn selected_project_index(selection: usize, mode: ProjectSelectMode) -> usize {
    if matches!(mode, ProjectSelectMode::AllowCreate) {
        selection - 1
    } else {
        selection
    }
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
        default_new_project_name, default_project_selection, project_selection_labels,
        selected_project_index, ProjectSelectMode,
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
    fn allow_create_adds_create_option() {
        let labels = project_selection_labels(&[project("alpha")], ProjectSelectMode::AllowCreate);
        assert_eq!(
            labels,
            vec!["+ Create new project".to_string(), "alpha".to_string()]
        );
    }

    #[test]
    fn existing_only_does_not_add_create_option() {
        let labels = project_selection_labels(&[project("alpha")], ProjectSelectMode::ExistingOnly);
        assert_eq!(labels, vec!["alpha".to_string()]);
    }

    #[test]
    fn allow_create_defaults_to_create_when_project_list_is_empty() {
        assert_eq!(
            default_project_selection(&[], None, ProjectSelectMode::AllowCreate)
                .expect("default selection"),
            0
        );
    }

    #[test]
    fn default_new_project_name_has_project_suffix() {
        assert!(default_new_project_name().ends_with("-project"));
    }

    #[test]
    fn allow_create_project_selection_skips_create_row() {
        assert_eq!(selected_project_index(1, ProjectSelectMode::AllowCreate), 0);
    }

    #[test]
    fn existing_only_project_selection_uses_same_index() {
        assert_eq!(
            selected_project_index(1, ProjectSelectMode::ExistingOnly),
            1
        );
    }
}
