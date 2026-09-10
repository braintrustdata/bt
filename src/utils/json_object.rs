use serde_json::{Map, Value};

pub(crate) fn merge_json_objects(target: &mut Map<String, Value>, source: &Map<String, Value>) {
    for (key, value) in source {
        match (target.get_mut(key), value) {
            (Some(Value::Object(target_inner)), Value::Object(source_inner)) => {
                merge_json_objects(target_inner, source_inner);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

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
    fn merge_json_objects_deep_merges_nested_maps() {
        let mut target = json!({
            "prompt_data": { "options": { "model": "gpt-test" } }
        })
        .as_object()
        .expect("object")
        .clone();
        let source = json!({
            "prompt_data": { "options": { "params": { "temperature": 0 } } }
        })
        .as_object()
        .expect("object")
        .clone();

        merge_json_objects(&mut target, &source);

        assert_eq!(target["prompt_data"]["options"]["model"], "gpt-test");
        assert_eq!(target["prompt_data"]["options"]["params"]["temperature"], 0);
    }

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
