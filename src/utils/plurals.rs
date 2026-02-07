pub fn pluralize(count: &usize, singular: &str, plural: Option<&str>) -> String {
    if *count == 1 {
        return singular.to_string();
    }

    match plural {
        Some(p) => p.to_string(),
        None => format!("{singular}s"),
    }
}
