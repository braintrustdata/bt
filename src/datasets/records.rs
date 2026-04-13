use std::{
    collections::{HashMap, HashSet},
    fs,
    io::{self, IsTerminal, Read},
    path::Path,
};

use anyhow::{anyhow, bail, Context, Result};
use chrono::Utc;
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use super::api::DatasetRow;

pub(crate) const DATASET_UPLOAD_BATCH_SIZE: usize = 1000;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PreparedDatasetRecord {
    pub id: String,
    pub input: Option<Value>,
    pub expected: Option<Value>,
    pub metadata: Option<Map<String, Value>>,
    pub tags: Option<Vec<String>>,
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

pub(crate) fn remote_records_by_id(
    rows: Vec<DatasetRow>,
) -> Result<HashMap<String, PreparedDatasetRecord>> {
    let mut records = HashMap::new();
    for row in rows {
        if let Some(record) = prepared_record_from_remote_row(&row)? {
            let record_id = record.id.clone();
            if records.insert(record_id.clone(), record).is_some() {
                bail!("remote dataset contains duplicate record id '{record_id}'");
            }
        }
    }
    Ok(records)
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
        validate_supported_fields(&raw_record, &id_root, row_index)?;
        let record =
            prepared_record_from_input_object(raw_record, &id_path, require_ids, row_index)?;
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

fn validate_supported_fields(
    object: &Map<String, Value>,
    id_root: &str,
    row_index: usize,
) -> Result<()> {
    const BASE_ALLOWED_FIELDS: [&str; 6] =
        ["id", "input", "expected", "output", "metadata", "tags"];
    let mut unsupported = object
        .keys()
        .filter(|field| !BASE_ALLOWED_FIELDS.contains(&field.as_str()) && field.as_str() != id_root)
        .cloned()
        .collect::<Vec<_>>();

    if unsupported.is_empty() {
        return Ok(());
    }

    unsupported.sort();
    let id_clause = if BASE_ALLOWED_FIELDS.contains(&id_root) {
        String::new()
    } else {
        format!(", and id root '{}'", id_root)
    };
    bail!(
        "dataset record {} contains unsupported top-level field(s): {}. Allowed fields are id, input, expected, output, metadata, tags{}",
        row_index + 1,
        unsupported.join(", "),
        id_clause
    );
}

fn prepared_record_from_input_object(
    object: Map<String, Value>,
    id_path: &[String],
    require_id: bool,
    row_index: usize,
) -> Result<PreparedDatasetRecord> {
    let explicit_id = lookup_object_path(&object, id_path)
        .map(coerce_id_value)
        .transpose()?;

    let id = match explicit_id {
        Some(id) => id,
        None if require_id => bail!(
            "dataset record {} is missing a stable id at '{}'. `bt datasets update`/`add`/`refresh` require explicit ids; include an id field or pass --id-field",
            row_index + 1,
            id_path.join(".")
        ),
        None => generated_record_id(&object, row_index)?,
    };

    Ok(PreparedDatasetRecord {
        id,
        input: object.get("input").cloned(),
        expected: object
            .get("expected")
            .cloned()
            .or_else(|| object.get("output").cloned()),
        metadata: parse_metadata(object.get("metadata"))?,
        tags: parse_tags(object.get("tags"))?,
    })
}

fn prepared_record_from_remote_row(row: &DatasetRow) -> Result<Option<PreparedDatasetRecord>> {
    let Some(id_value) = row.get("id").or_else(|| row.get("span_id")) else {
        return Ok(None);
    };
    let id = coerce_id_value(id_value)?;
    Ok(Some(PreparedDatasetRecord {
        id,
        input: row.get("input").cloned(),
        expected: row
            .get("expected")
            .cloned()
            .or_else(|| row.get("output").cloned()),
        metadata: parse_metadata(row.get("metadata"))?,
        tags: parse_tags(row.get("tags"))?,
    }))
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

fn coerce_id_value(value: &Value) -> Result<String> {
    match value {
        Value::String(value) => {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                bail!("dataset record id cannot be empty");
            }
            Ok(trimmed.to_string())
        }
        Value::Number(value) => Ok(value.to_string()),
        Value::Bool(value) => Ok(value.to_string()),
        Value::Null => bail!("dataset record id cannot be null"),
        _ => Err(anyhow!(
            "dataset record id must be a string, number, or boolean"
        )),
    }
}

fn parse_metadata(value: Option<&Value>) -> Result<Option<Map<String, Value>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Object(metadata)) => Ok(Some(metadata.clone())),
        Some(_) => bail!("dataset record metadata must be a JSON object"),
    }
}

fn parse_tags(value: Option<&Value>) -> Result<Option<Vec<String>>> {
    match value {
        None | Some(Value::Null) => Ok(None),
        Some(Value::Array(values)) => values
            .iter()
            .enumerate()
            .map(|(index, value)| match value {
                Value::String(value) => Ok(value.clone()),
                _ => bail!("dataset record tags[{index}] must be a string"),
            })
            .collect::<Result<Vec<_>>>()
            .map(Some),
        Some(_) => bail!("dataset record tags must be an array of strings"),
    }
}

fn generated_record_id(object: &Map<String, Value>, row_index: usize) -> Result<String> {
    let payload = serde_json::to_vec(object).context("failed to serialize dataset record")?;
    let digest = Sha256::digest(payload);
    let mut short_hash = String::with_capacity(16);
    for byte in digest.iter().take(8) {
        short_hash.push_str(&format!("{byte:02x}"));
    }
    Ok(format!("bt-dataset-{row_index:06}-{short_hash}"))
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
    fn prepare_records_generates_stable_id_when_missing() {
        let source: Map<String, Value> = serde_json::from_value(serde_json::json!({
            "input": {"text": "hello"},
            "expected": "world"
        }))
        .expect("map");
        let first = prepare_records(vec![source.clone()], "id", false).expect("first");
        let second = prepare_records(vec![source], "id", false).expect("second");
        assert_eq!(first[0].id, second[0].id);
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
    fn remote_record_prefers_expected_over_output() {
        let row = serde_json::from_value::<Map<String, Value>>(serde_json::json!({
            "id": "case-1",
            "expected": "expected",
            "output": "output"
        }))
        .expect("map");
        let record = prepared_record_from_remote_row(&row)
            .expect("parse remote")
            .expect("record");
        assert_eq!(record.expected, Some(Value::String("expected".to_string())));
    }

    #[test]
    fn remote_records_by_id_rejects_duplicate_ids() {
        let rows = vec![
            serde_json::from_value(serde_json::json!({"id": "dup", "expected": "a"}))
                .expect("first row"),
            serde_json::from_value(serde_json::json!({"id": "dup", "expected": "b"}))
                .expect("second row"),
        ];
        let err = remote_records_by_id(rows).expect_err("duplicate remote ids");
        assert!(err
            .to_string()
            .contains("remote dataset contains duplicate record id 'dup'"));
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
        assert!(err
            .to_string()
            .contains("unsupported top-level field(s): foo"));
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
}
