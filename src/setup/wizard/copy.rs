pub const WIZARD_TITLE: &str = "Braintrust Setup";

pub const NOT_GIT_REPO_WARNING: &str =
    "Heads up: this folder is not a git repository. The wizard may edit files; consider running it inside a checked-in repo.";

pub const DOCS_URL: &str = "https://www.braintrust.dev/docs";

pub const WIZARD_CANCEL_MESSAGE: &str = "Setup cancelled.";

pub fn terminal_hyperlink(url: &str) -> String {
    // Emit an OSC 8 hyperlink when the terminal advertises support; otherwise
    // print the URL as plain text. Detection is via `supports-hyperlinks`.
    if supports_hyperlinks::on(supports_hyperlinks::Stream::Stderr) {
        format!("\x1b]8;;{url}\x1b\\{url}\x1b]8;;\x1b\\")
    } else {
        url.to_string()
    }
}

pub fn wizard_login_prompt(verification_code: &str) -> String {
    let code = dialoguer::console::style(verification_code)
        .color256(231)
        .bold();
    format!(
        "Open the URL above in your browser to finish signing in.\n\nAfter signing in, verify this code matches the one shown in your browser: {code}\n\nPick the org and project you want to use; the wizard will resume here."
    )
}

pub fn skill_next_step_hint(agent_display_name: Option<&str>) -> String {
    let action = dialoguer::console::style("run the /instrument-code skill")
        .red()
        .bright();
    match agent_display_name {
        Some(name) => format!("Open {name} in this repo and {action}."),
        None => format!("Open your coding agent in this repo and {action}."),
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
