use std::fmt;

/// An expected error caused by command input rather than an internal failure.
#[derive(Debug)]
pub(crate) struct UserError {
    source: Box<dyn std::error::Error + Send + Sync>,
}

impl From<anyhow::Error> for UserError {
    fn from(error: anyhow::Error) -> Self {
        Self {
            source: error.into_boxed_dyn_error(),
        }
    }
}

impl fmt::Display for UserError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.source.fmt(formatter)
    }
}

impl std::error::Error for UserError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(self.source.as_ref())
    }
}
