use anyhow::{bail, Result};

pub(crate) fn slug_from_name(name: &str, label: &str) -> Result<String> {
    let mut slug = String::new();
    let mut pending_separator = false;

    for ch in name.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(ch.to_ascii_lowercase());
            pending_separator = false;
        } else if !slug.is_empty() {
            pending_separator = true;
        }
    }

    if slug.is_empty() {
        bail!("{label} name must contain at least one ASCII letter or number");
    }
    Ok(slug)
}
