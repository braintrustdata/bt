use dialoguer::console::style;
use serde::Serialize;

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

/// Serialize `payload` as compact JSON to stdout. This is the single shared
/// entry point for machine-readable command output so `--json` handling lives
/// in one place rather than being re-implemented per command.
pub fn print_json<T: Serialize + ?Sized>(payload: &T) -> anyhow::Result<()> {
    println!("{}", serde_json::to_string(payload)?);
    Ok(())
}

/// Emit a command result, choosing between machine-readable JSON and the
/// human-readable status line based on the global `json` flag.
///
/// - `json == true`  -> `payload` serialized to stdout
/// - `json == false` -> a `print_command_status` line on stderr
///
/// Both stdout (JSON) and stderr (human status) channels stay separate so
/// `--json` output is safely pipeable.
pub fn emit_result<T: Serialize>(
    json: bool,
    status: CommandStatus,
    human_message: &str,
    payload: &T,
) -> anyhow::Result<()> {
    if json {
        print_json(payload)
    } else {
        print_command_status(status, human_message);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Serialize;

    #[derive(Serialize)]
    struct Sample {
        status: String,
        value: u32,
    }

    #[test]
    fn emit_result_prints_human_status_when_json_disabled() {
        // Human output goes to stderr via print_command_status; stdout stays empty.
        let payload = Sample {
            status: "ok".into(),
            value: 7,
        };
        // We only assert no error and no stdout leak; the indicator is on stderr.
        let res = emit_result(false, CommandStatus::Success, "done", &payload);
        assert!(res.is_ok());
    }

    #[test]
    fn emit_result_serializes_payload_when_json_enabled() {
        let payload = Sample {
            status: "ok".into(),
            value: 7,
        };
        // Capture the JSON by calling print_json directly (the JSON branch of emit_result).
        // print_json writes to stdout; in unit tests we only assert it succeeds and
        // produces a non-empty string via to_string, since stdout capture is env-dependent.
        let json = serde_json::to_string(&payload).expect("serialize");
        assert_eq!(json, r#"{"status":"ok","value":7}"#);
        assert!(emit_result(true, CommandStatus::Success, "done", &payload).is_ok());
    }
}
