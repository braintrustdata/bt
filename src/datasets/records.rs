use std::{
    collections::HashSet,
    fs,
    io::{self, IsTerminal, Read},
    path::Path,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde::{Deserialize, Deserializer};
use serde_json::{Map, Value};

use crate::utils::new_uuid_id;

pub(crate) const DATASET_UPLOAD_BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedDatasetRecord {
    pub id: String,
    pub input: Option<Value>,
    pub expected: Option<Value>,
    pub metadata: Option<Map<String, Value>>,
    pub tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct DatasetRecordInput {
    #[serde(default, deserialize_with = "deserialize_optional_record_id")]
    id: Option<String>,
    input: Option<Value>,
    expected: Option<Value>,
    metadata: Option<Map<String, Value>>,
    tags: Option<Vec<String>>,
}

impl PreparedDatasetRecord {
    pub fn to_upload_row(&self, dataset_id: &str) -> Map<String, Value> {
        let mut row = Map::new();
        row.insert("id".to_string(), Value::String(self.id.clone()));
        row.insert(
            "dataset_id".to_string(),
            Value::String(dataset_id.to_string()),
        );
        row.insert(
            "created".to_string(),
            Value::String(Utc::now().to_rfc3339()),
        );
        if let Some(input) = &self.input {
            row.insert("input".to_string(), input.clone());
        }
        if let Some(expected) = &self.expected {
            row.insert("expected".to_string(), expected.clone());
        }
        if let Some(metadata) = &self.metadata {
            row.insert("metadata".to_string(), Value::Object(metadata.clone()));
        }
        if let Some(tags) = &self.tags {
            row.insert(
                "tags".to_string(),
                Value::Array(tags.iter().cloned().map(Value::String).collect()),
            );
        }
        row
    }
}

pub(crate) fn load_optional_upload_records(
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
    id_field: &str,
) -> Result<Option<Vec<PreparedDatasetRecord>>> {
    let Some(raw) = load_optional_record_objects(input_path, inline_rows)? else {
        return Ok(None);
    };
    Ok(Some(prepare_records(raw, id_field, false)?))
}

pub(crate) fn load_refresh_records(
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
    id_field: &str,
) -> Result<Vec<PreparedDatasetRecord>> {
    let raw = load_required_record_objects(input_path, inline_rows)?;
    prepare_records(raw, id_field, true)
}

fn load_required_record_objects(
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
) -> Result<Vec<Map<String, Value>>> {
    load_optional_record_objects(input_path, inline_rows)?.ok_or_else(|| {
        anyhow!(
            "dataset input required. Pass --file <path>, --rows <json>, or pipe JSON/JSONL into stdin"
        )
    })
}

fn load_optional_record_objects(
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
) -> Result<Option<Vec<Map<String, Value>>>> {
    let Some(contents) = read_input_contents(input_path, inline_rows)? else {
        return Ok(None);
    };
    if contents.trim().is_empty() && input_path.is_none() && inline_rows.is_none() {
        return Ok(None);
    }
    Ok(Some(parse_record_objects(&contents)?))
}

fn parse_record_objects(contents: &str) -> Result<Vec<Map<String, Value>>> {
    let trimmed = contents.trim();
    if trimmed.is_empty() {
        bail!("dataset input is empty");
    }

    if trimmed.starts_with('[') {
        return parse_json_records(trimmed);
    }

    if trimmed.starts_with('{') {
        let json_result = parse_json_records(trimmed);
        if json_result.is_ok() {
            return json_result;
        }

        if trimmed.lines().skip(1).any(|line| !line.trim().is_empty()) {
            if let Ok(records) = parse_jsonl_records(trimmed) {
                return Ok(records);
            }
        }

        return json_result;
    }

    parse_jsonl_records(trimmed)
}

fn read_input_contents(
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
) -> Result<Option<String>> {
    match (input_path, inline_rows) {
        (Some(path), None) => fs::read_to_string(path)
            .with_context(|| format!("failed to read dataset input {}", path.display()))
            .map(Some),
        (None, Some(rows)) => Ok(Some(rows.to_string())),
        (Some(_), Some(_)) => bail!("pass either --file or --rows, not both"),
        (None, None) => {
            if io::stdin().is_terminal() {
                return Ok(None);
            }
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .context("failed to read dataset input from stdin")?;
            Ok(Some(buffer))
        }
    }
}

fn parse_json_records(contents: &str) -> Result<Vec<Map<String, Value>>> {
    let value: Value = serde_json::from_str(contents).context("invalid dataset JSON input")?;
    match value {
        Value::Array(values) => values
            .into_iter()
            .enumerate()
            .map(|(index, value)| expect_record_object(value, Some(index + 1)))
            .collect(),
        Value::Object(mut object) => {
            if let Some(rows) = object.remove("rows") {
                match rows {
                    Value::Array(values) => values
                        .into_iter()
                        .enumerate()
                        .map(|(index, value)| expect_record_object(value, Some(index + 1)))
                        .collect(),
                    _ => bail!("dataset JSON field 'rows' must be an array of objects"),
                }
            } else {
                Ok(vec![object])
            }
        }
        _ => bail!("dataset JSON input must be an object, an array of objects, or an object with a 'rows' array"),
    }
}

fn parse_jsonl_records(contents: &str) -> Result<Vec<Map<String, Value>>> {
    let mut rows = Vec::new();
    for (line_index, raw_line) in contents.lines().enumerate() {
        let line = raw_line.trim();
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_str(line)
            .with_context(|| format!("invalid JSON on line {}", line_index + 1))?;
        rows.push(expect_record_object(value, Some(line_index + 1))?);
    }

    if rows.is_empty() {
        bail!("dataset input did not contain any records");
    }

    Ok(rows)
}

fn expect_record_object(value: Value, line_number: Option<usize>) -> Result<Map<String, Value>> {
    match value {
        Value::Object(object) => Ok(object),
        _ => match line_number {
            Some(line_number) => {
                bail!("dataset record on line {line_number} must be a JSON object")
            }
            None => bail!("dataset record must be a JSON object"),
        },
    }
}

fn prepare_records(
    raw_records: Vec<Map<String, Value>>,
    id_field: &str,
    require_ids: bool,
) -> Result<Vec<PreparedDatasetRecord>> {
    let id_path = parse_id_field_path(id_field)?;
    let id_root = id_path.first().cloned().expect("id path must be non-empty");
    let mut records = Vec::with_capacity(raw_records.len());
    let mut seen_ids = HashSet::new();

    for (row_index, raw_record) in raw_records.into_iter().enumerate() {
        let raw_record = strip_ignored_system_fields(raw_record, &id_root);
        let (input_record, explicit_id) =
            deserialize_input_record(raw_record, &id_path, &id_root, row_index)?;
        let record =
            prepare_record_from_input(input_record, explicit_id, &id_path, require_ids, row_index)?;
        if !seen_ids.insert(record.id.clone()) {
            bail!("duplicate dataset record id '{}' in input", record.id);
        }
        records.push(record);
    }

    if records.is_empty() {
        bail!("dataset input did not contain any records");
    }

    Ok(records)
}

fn strip_ignored_system_fields(
    mut object: Map<String, Value>,
    id_root: &str,
) -> Map<String, Value> {
    for field in ["dataset_id", "created"] {
        if field != id_root {
            object.remove(field);
        }
    }
    object
}

fn deserialize_input_record(
    object: Map<String, Value>,
    id_path: &[String],
    id_root: &str,
    row_index: usize,
) -> Result<(DatasetRecordInput, Option<String>)> {
    let explicit_id = if id_path == ["id"] {
        None
    } else {
        lookup_object_path(&object, id_path)
            .map(parse_id_value)
            .transpose()?
    };
    let mut record_object = object;
    if !is_sdk_record_field(id_root) {
        record_object.remove(id_root);
    }

    let input = serde_json::from_value(Value::Object(record_object)).map_err(|err| {
        anyhow!(
            "dataset record {} does not match the supported record shape: {err}",
            row_index + 1
        )
    })?;
    Ok((input, explicit_id))
}

fn is_sdk_record_field(field: &str) -> bool {
    matches!(field, "id" | "input" | "expected" | "metadata" | "tags")
}

fn prepare_record_from_input(
    input_record: DatasetRecordInput,
    explicit_id: Option<String>,
    id_path: &[String],
    require_id: bool,
    row_index: usize,
) -> Result<PreparedDatasetRecord> {
    let DatasetRecordInput {
        id: standard_id,
        input,
        expected,
        metadata,
        tags,
    } = input_record;
    let explicit_id = explicit_id.or(standard_id);

    let id = match explicit_id {
        Some(id) => id,
        None if require_id => bail!(
            "dataset record {} is missing a stable id at '{}'. `bt datasets update`/`add`/`refresh` require explicit ids; include an id field or pass --id-field",
            row_index + 1,
            id_path.join(".")
        ),
        None => new_uuid_id(),
    };

    Ok(PreparedDatasetRecord {
        id,
        input,
        expected,
        metadata,
        tags,
    })
}

fn deserialize_optional_record_id<'de, D>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<Value>::deserialize(deserializer)?;
    value
        .as_ref()
        .map(parse_id_value)
        .transpose()
        .map_err(serde::de::Error::custom)
}

fn parse_id_field_path(id_field: &str) -> Result<Vec<String>> {
    let path = id_field
        .split('.')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    if path.is_empty() {
        bail!("id field path cannot be empty");
    }
    Ok(path)
}

fn lookup_object_path<'a>(object: &'a Map<String, Value>, path: &[String]) -> Option<&'a Value> {
    let mut current = object.get(path.first()?.as_str())?;
    for part in path.iter().skip(1) {
        current = current.as_object()?.get(part.as_str())?;
    }
    Some(current)
}

fn parse_id_value(value: &Value) -> Result<String> {
    match value {
        Value::String(value) => {
            if value.is_empty() {
                bail!("dataset record id cannot be empty");
            }
            Ok(value.clone())
        }
        Value::Null => bail!("dataset record id cannot be null"),
        _ => Err(anyhow!("dataset record id must be a string")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_json_rows_wrapper_extracts_rows() {
        let records =
            parse_json_records(r#"{"dataset":{"id":"ds"},"rows":[{"id":"a"},{"id":"b"}]}"#)
                .expect("parse rows wrapper");
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].get("id"), Some(&Value::String("a".to_string())));
    }

    #[test]
    fn parse_record_objects_accepts_jsonl_objects() {
        let records = parse_record_objects(
            r#"{"id":"a"}
{"id":"b"}
"#,
        )
        .expect("parse jsonl records");
        assert_eq!(records.len(), 2);
        assert_eq!(records[1].get("id"), Some(&Value::String("b".to_string())));
    }

    #[test]
    fn read_input_contents_prefers_inline_rows() {
        let records = read_input_contents(None, Some(r#"[{"id":"case-1"}]"#))
            .expect("read inline rows")
            .expect("inline rows present");
        assert_eq!(records, r#"[{"id":"case-1"}]"#);
    }

    #[test]
    fn prepare_records_uses_nested_id_field() {
        let records = prepare_records(
            vec![serde_json::from_value(serde_json::json!({
                "metadata": {"case_id": "case-1"},
                "input": {"text": "hello"},
                "expected": "world"
            }))
            .expect("map")],
            "metadata.case_id",
            true,
        )
        .expect("prepare records");
        assert_eq!(records[0].id, "case-1");
    }

    #[test]
    fn prepare_records_generates_uuid_id_when_missing() {
        let source: Map<String, Value> = serde_json::from_value(serde_json::json!({
            "input": {"text": "hello"},
            "expected": "world"
        }))
        .expect("map");
        let first = prepare_records(vec![source.clone()], "id", false).expect("first");
        let second = prepare_records(vec![source], "id", false).expect("second");
        assert_ne!(first[0].id, second[0].id);
    }

    #[test]
    fn load_refresh_records_requires_explicit_ids() {
        let err = load_refresh_records(None, Some(r#"[{"input":{"text":"hello"}}]"#), "id")
            .expect_err("refresh should require explicit ids");
        assert!(err.to_string().contains("missing a stable id at 'id'"));
        assert!(err
            .to_string()
            .contains("update`/`add`/`refresh` require explicit ids"));
    }

    #[test]
    fn prepare_records_rejects_duplicate_ids() {
        let first = serde_json::from_value(serde_json::json!({"id": "dup"})).expect("map");
        let second = serde_json::from_value(serde_json::json!({"id": "dup"})).expect("map");
        let err = prepare_records(vec![first, second], "id", true).expect_err("duplicate ids");
        assert!(err
            .to_string()
            .contains("duplicate dataset record id 'dup'"));
    }

    #[test]
    fn prepare_records_rejects_unsupported_top_level_fields() {
        let record =
            serde_json::from_value(serde_json::json!({"id": "case-1", "foo": "bar"})).expect("map");
        let err = prepare_records(vec![record], "id", true)
            .expect_err("unsupported top-level field should error");
        let message = err.to_string();
        assert!(message.contains("dataset record 1 does not match the supported record shape"));
        assert!(message.contains("unknown field `foo`"));
    }

    #[test]
    fn prepare_records_rejects_non_object_metadata() {
        let record = serde_json::from_value(serde_json::json!({
            "id": "case-1",
            "metadata": "bad"
        }))
        .expect("map");
        let err =
            prepare_records(vec![record], "id", true).expect_err("metadata should be an object");
        let message = err.to_string();
        assert!(message.contains("dataset record 1 does not match the supported record shape"));
        assert!(message.contains("expected a map"));
    }

    #[test]
    fn prepare_records_rejects_non_string_tags() {
        let record = serde_json::from_value(serde_json::json!({
            "id": "case-1",
            "tags": ["smoke", 1]
        }))
        .expect("map");
        let err = prepare_records(vec![record], "id", true).expect_err("tags should be strings");
        let message = err.to_string();
        assert!(message.contains("dataset record 1 does not match the supported record shape"));
        assert!(message.contains("expected a string"));
    }

    #[test]
    fn prepare_records_rejects_non_string_id() {
        let record = serde_json::from_value(serde_json::json!({
            "id": 123,
            "input": {"prompt": "hello"}
        }))
        .expect("map");
        let err = prepare_records(vec![record], "id", true).expect_err("id should be a string");
        let message = err.to_string();
        assert!(message.contains("dataset record 1 does not match the supported record shape"));
        assert!(message.contains("dataset record id must be a string"));
    }

    #[test]
    fn prepare_records_rejects_output_field() {
        let record = serde_json::from_value(serde_json::json!({
            "id": "case-1",
            "output": "gold"
        }))
        .expect("map");
        let err = prepare_records(vec![record], "id", true).expect_err("output should be rejected");
        let message = err.to_string();
        assert!(message.contains("dataset record 1 does not match the supported record shape"));
        assert!(message.contains("unknown field `output`"));
    }

    #[test]
    fn prepare_records_allows_custom_id_root_field() {
        let record = serde_json::from_value(serde_json::json!({
            "custom": {"record_id": "case-1"},
            "input": {"prompt": "hello"},
            "expected": "world"
        }))
        .expect("map");
        let prepared = prepare_records(vec![record], "custom.record_id", true)
            .expect("custom id-field root should be allowed");
        assert_eq!(prepared[0].id, "case-1");
    }

    #[test]
    fn prepare_records_uploads_expected_field() {
        let record = serde_json::from_value(serde_json::json!({
            "id": "case-1",
            "expected": "gold"
        }))
        .expect("map");
        let prepared = prepare_records(vec![record], "id", true).expect("prepare records");
        assert_eq!(
            prepared[0].expected,
            Some(Value::String("gold".to_string()))
        );

        let row = prepared[0].to_upload_row("dataset_1");
        assert_eq!(
            row.get("expected"),
            Some(&Value::String("gold".to_string()))
        );
        assert!(row.get("output").is_none());
    }

    #[test]
    fn prepare_records_ignores_dataset_view_system_fields() {
        let record = serde_json::from_value(serde_json::json!({
            "id": "case-1",
            "input": {"prompt": "hello"},
            "expected": "world",
            "dataset_id": "dataset_1",
            "created": "2026-01-01T00:00:00Z"
        }))
        .expect("map");
        let prepared = prepare_records(vec![record], "id", true).expect("prepare records");
        assert_eq!(prepared[0].id, "case-1");
        assert_eq!(
            prepared[0].expected,
            Some(Value::String("world".to_string()))
        );
    }

    #[test]
    fn prepare_records_keeps_dataset_id_when_used_as_id_field() {
        let record = serde_json::from_value(serde_json::json!({
            "dataset_id": "case-1",
            "input": {"prompt": "hello"},
            "expected": "world"
        }))
        .expect("map");
        let prepared = prepare_records(vec![record], "dataset_id", true).expect("prepare records");
        assert_eq!(prepared[0].id, "case-1");
    }
}
