use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CommandStatus {
    Success,
    Partial,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileStatus {
    Success,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HardFailureReason {
    AuthFailed,
    RequestFailed,
    ResponseInvalid,
    UserCancelled,
    OutputDirInvalid,
    AtomicWriteFailed,
    UnsafeOutputPath,
    RunnerSpawnFailed,
    RunnerExitNonzero,
    ManifestInvalidJson,
    ManifestSchemaInvalid,
    ManifestPathMissing,
    UploadSlotFailed,
    BundleUploadFailed,
    InsertFunctionsFailed,
    SelectorNotFound,
    PaginationUnsupported,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SoftSkipReason {
    DirtyTarget,
    ExistingNonGitNoForce,
    MalformedRecord,
    UnsupportedFunctionType,
    SupersededVersion,
    TerminatedAfterFailure,
    IfExistsIgnored,
    NoDefinitionsFound,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum WarningReason {
    PaginationNotSnapshotConsistent,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReportWarning {
    pub reason: WarningReason,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReportError {
    pub reason: HardFailureReason,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushFileReport {
    pub source_file: String,
    pub status: FileStatus,
    pub uploaded_entries: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<SoftSkipReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<HardFailureReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bundle_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PushSummary {
    pub status: CommandStatus,
    pub total_files: usize,
    pub uploaded_files: usize,
    pub failed_files: usize,
    pub skipped_files: usize,
    pub ignored_entries: usize,
    pub files: Vec<PushFileReport>,
    pub warnings: Vec<ReportWarning>,
    pub errors: Vec<ReportError>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullFileReport {
    pub output_file: String,
    pub status: FileStatus,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub skipped_reason: Option<SoftSkipReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_reason: Option<HardFailureReason>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PullSummary {
    pub status: CommandStatus,
    pub projects_total: usize,
    pub files_written: usize,
    pub files_skipped: usize,
    pub files_failed: usize,
    pub functions_seen: usize,
    pub functions_materialized: usize,
    pub malformed_records_skipped: usize,
    pub unsupported_records_skipped: usize,
    pub files: Vec<PullFileReport>,
    pub warnings: Vec<ReportWarning>,
    pub errors: Vec<ReportError>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn enums_serialize_as_snake_case() {
        let reason = serde_json::to_string(&HardFailureReason::ManifestInvalidJson)
            .expect("serialize reason");
        assert_eq!(reason, "\"manifest_invalid_json\"");

        let status = serde_json::to_string(&CommandStatus::Partial).expect("serialize status");
        assert_eq!(status, "\"partial\"");

        let warning = serde_json::to_string(&WarningReason::PaginationNotSnapshotConsistent)
            .expect("serialize warning");
        assert_eq!(warning, "\"pagination_not_snapshot_consistent\"");
    }

    #[test]
    fn push_summary_roundtrip() {
        let summary = PushSummary {
            status: CommandStatus::Partial,
            total_files: 2,
            uploaded_files: 1,
            failed_files: 0,
            skipped_files: 1,
            ignored_entries: 1,
            files: vec![PushFileReport {
                source_file: "a.ts".to_string(),
                status: FileStatus::Skipped,
                uploaded_entries: 0,
                skipped_reason: Some(SoftSkipReason::IfExistsIgnored),
                error_reason: None,
                bundle_id: None,
                message: None,
            }],
            warnings: vec![],
            errors: vec![],
        };

        let encoded = serde_json::to_string(&summary).expect("encode");
        let decoded: PushSummary = serde_json::from_str(&encoded).expect("decode");
        assert_eq!(decoded, summary);
    }
}
