use serde_json::Value;

use crate::utils::merge_json_objects;

/// Replace a partial `prompt_data` patch with the full merged object.
///
/// The function and prompt PATCH endpoints merge top-level fields but replace
/// `prompt_data` wholesale. Materializing it before the request preserves fields
/// that the user did not change.
pub(crate) fn materialize_prompt_data_patch(
    patch: &mut Value,
    existing_prompt_data: Option<&Value>,
) {
    let Some(patch_prompt_data) = patch.get("prompt_data").and_then(Value::as_object).cloned()
    else {
        return;
    };
    let mut merged = existing_prompt_data
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    merge_json_objects(&mut merged, &patch_prompt_data);

    if let (Some(requested), Some(parser)) = (
        patch_prompt_data.get("parser").and_then(Value::as_object),
        merged.get_mut("parser").and_then(Value::as_object_mut),
    ) {
        if let Some(scores) = requested.get("choice_scores") {
            parser.remove("choice");
            parser.remove("allow_no_match");
            parser.insert("choice_scores".to_string(), scores.clone());
        } else if requested.contains_key("choice") {
            parser.remove("choice_scores");
        }
    }

    patch["prompt_data"] = Value::Object(merged);
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn materializes_complete_prompt_data_for_patch() {
        let existing = json!({
            "prompt": {"type": "chat", "messages": []},
            "parser": {"type": "llm_classifier", "choice_scores": {"old": 0}},
            "options": {"model": "test-model", "params": {"temperature": 0.5}}
        });
        let mut patch = json!({
            "prompt_data": {
                "parser": {"choice_scores": {"new": 1}},
                "options": {"params": {"temperature": 0.2}}
            }
        });

        materialize_prompt_data_patch(&mut patch, Some(&existing));

        assert_eq!(patch["prompt_data"]["prompt"], existing["prompt"]);
        assert_eq!(
            patch["prompt_data"]["parser"]["choice_scores"],
            json!({"new": 1})
        );
        assert_eq!(patch["prompt_data"]["options"]["model"], "test-model");
        assert_eq!(
            patch["prompt_data"]["options"]["params"]["temperature"],
            0.2
        );
    }
}
