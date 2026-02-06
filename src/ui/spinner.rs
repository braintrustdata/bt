use std::future::Future;
use std::io::IsTerminal;
use std::time::{Duration, Instant};

use indicatif::{ProgressBar, ProgressStyle};

const MIN_SPINNER_DURATION: Duration = Duration::from_millis(600);

/// Run an async operation with a spinner showing the given message.
/// Only shows spinner if stderr is a terminal.
pub async fn with_spinner<T, F: Future<Output = T>>(message: &str, fut: F) -> T {
    if !std::io::stderr().is_terminal() {
        return fut.await;
    }

    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(Duration::from_millis(80));

    let start = Instant::now();
    let result = fut.await;

    // Ensure spinner is visible for minimum duration
    let elapsed = start.elapsed();
    if elapsed < MIN_SPINNER_DURATION {
        tokio::time::sleep(MIN_SPINNER_DURATION - elapsed).await;
    }

    spinner.finish_and_clear();
    result
}
