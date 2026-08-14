use std::path::PathBuf;

/// Root directory for `bt`'s on-disk caches.
///
/// Path discovery, not configuration: standard `XDG_CACHE_HOME`/`HOME` only, no
/// bt-specific variable. Falls back to the temp directory.
pub(crate) fn bt_cache_root() -> PathBuf {
    let root = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(std::env::temp_dir);

    root.join("bt")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cache_root_lives_under_a_bt_directory() {
        assert_eq!(
            bt_cache_root().file_name().and_then(|name| name.to_str()),
            Some("bt")
        );
    }
}
