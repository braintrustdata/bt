use std::future::Future;
use std::io::IsTerminal;
use std::pin::pin;
use std::time::Duration;

use indicatif::{ProgressBar, ProgressStyle};

const SPINNER_DELAY: Duration = Duration::from_millis(300);

/// Run an async operation with a spinner showing the given message.
/// Only shows spinner if the operation takes longer than 300ms.
pub async fn with_spinner<T, F: Future<Output = T>>(message: &str, fut: F) -> T {
    if !std::io::stderr().is_terminal() {
        return fut.await;
    }

    let mut fut = pin!(fut);

    // Wait up to SPINNER_DELAY for the future to complete
    tokio::select! {
        biased;
        result = &mut fut => return result,
        _ = tokio::time::sleep(SPINNER_DELAY) => {}
    }

    // Operation is taking a while, show spinner
    let spinner = ProgressBar::new_spinner();
    spinner.set_style(
        ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
            .template("{spinner:.cyan} {msg}")
            .unwrap(),
    );
    spinner.set_message(message.to_string());
    spinner.enable_steady_tick(Duration::from_millis(80));

    let result = fut.await;
    spinner.finish_and_clear();
    result
}

pub async fn with_spinner_visible<T, F: Future<Output = T>>(
    message: &str,
    fut: F,
    min_duration: Duration,
) -> T {
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

    let start = std::time::Instant::now();
    let result = fut.await;

    let elapsed = start.elapsed();
    if elapsed < min_duration {
        tokio::time::sleep(min_duration - elapsed).await;
    }
    spinner.finish_and_clear();
    result
}
