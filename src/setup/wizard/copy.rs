pub const WIZARD_TITLE: &str = "Braintrust Setup";

pub const NOT_GIT_REPO_WARNING: &str =
    "Heads up: this folder is not a git repository. The wizard may edit files; consider running it inside a checked-in repo.";

pub const DOCS_URL: &str = "https://www.braintrust.dev/docs";

pub const WIZARD_CANCEL_MESSAGE: &str = "Setup cancelled.";

pub fn wizard_login_prompt(login_url: &str, verification_code: &str) -> String {
    format!(
        "Open this URL in your browser to finish signing in:\n  {login_url}\n\nVerification code: {verification_code}\n\nPick the org and project you want to use; the wizard will resume here."
    )
}

pub fn skill_next_step_hint(agent_display_name: Option<&str>) -> String {
    match agent_display_name {
        Some(name) => format!("Open {name} in this repo and run the /instrument-code skill."),
        None => {
            "Open your coding agent in this repo and run the /instrument-code skill.".to_string()
        }
    }
}

pub fn no_agent_fallback_note(path: &str) -> String {
    format!("No coding agent detected on this machine. Wrote the instrument-code prompt to:\n  {path}\nPaste it into your agent of choice.")
}

pub fn build_cleanup_message(docs_url: &str) -> String {
    let mut lines = vec![
        "Setup complete.".to_string(),
        String::new(),
        "For production runs, set the BRAINTRUST_API_KEY environment variable.".to_string(),
        format!("Docs: {docs_url}"),
    ];
    lines.retain(|_| true);
    lines.join("\n")
}
