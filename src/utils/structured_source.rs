use anyhow::{bail, Context, Result};
use serde_json::{Map, Value};

use super::read_text_source;

pub(crate) fn read_yaml_object_source(
    source: &str,
    description: &str,
) -> Result<Map<String, Value>> {
    let raw = read_text_source(source, description)?;
    let value: Value =
        yaml_serde::from_str(&raw).with_context(|| format!("invalid YAML in {description}"))?;
    match value {
        Value::Object(object) => Ok(object),
        _ => bail!("{description} must be a YAML mapping/object"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_inline_yaml_object() {
        let value =
            read_yaml_object_source("owner: test-team\nsettings:\n  enabled: true\n", "metadata")
                .expect("metadata");

        assert_eq!(value["owner"], "test-team");
        assert_eq!(value["settings"]["enabled"], true);
    }

    #[test]
    fn rejects_yaml_array() {
        let error =
            read_yaml_object_source("- one\n- two\n", "metadata").expect_err("array should fail");
        assert!(error.to_string().contains("mapping/object"));
    }
}
