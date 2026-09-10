use std::fmt;

/// An expected error caused by command input rather than an internal failure.
#[derive(Debug)]
pub(crate) struct UserError {
    message: String,
}

impl fmt::Display for UserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.message)
    }
}

impl std::error::Error for UserError {}

pub(crate) fn user_error(error: anyhow::Error) -> anyhow::Error {
    let message = error.to_string();
    error.context(UserError { message })
}
