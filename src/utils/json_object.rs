use serde_json::{Map, Value};

pub(crate) fn lookup_object_path<'a, P>(
    object: &'a Map<String, Value>,
    path: &[P],
) -> Option<&'a Value>
where
    P: AsRef<str>,
{
    let mut current = object.get(path.first()?.as_ref())?;
    for part in path.iter().skip(1) {
        current = current.as_object()?.get(part.as_ref())?;
    }
    Some(current)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn lookup_object_path_finds_nested_values() {
        let object = json!({
            "metadata": {
                "case.id": "case-1"
            }
        })
        .as_object()
        .expect("object")
        .clone();

        assert_eq!(
            lookup_object_path(&object, &["metadata", "case.id"]).and_then(Value::as_str),
            Some("case-1")
        );
    }

    #[test]
    fn lookup_object_path_returns_none_for_missing_path() {
        let object = json!({
            "metadata": {
                "case_id": "case-1"
            }
        })
        .as_object()
        .expect("object")
        .clone();

        assert!(lookup_object_path(&object, &["metadata", "missing"]).is_none());
    }

    #[test]
    fn lookup_object_path_returns_none_for_non_object_intermediate() {
        let object = json!({
            "metadata": "not an object"
        })
        .as_object()
        .expect("object")
        .clone();

        assert!(lookup_object_path(&object, &["metadata", "case_id"]).is_none());
    }

    #[test]
    fn lookup_object_path_returns_none_for_empty_path() {
        let object = json!({"id": "case-1"}).as_object().expect("object").clone();
        let path: [&str; 0] = [];

        assert!(lookup_object_path(&object, &path).is_none());
    }
}
