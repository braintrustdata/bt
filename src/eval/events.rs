use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug)]
pub(super) enum EvalEvent {
    Processing(ProcessingEventData),
    Start(ExperimentStart),
    Summary(ExperimentSummary),
    Progress(SseProgressEventData),
    Dependencies {
        files: Vec<String>,
    },
    Done,
    Error {
        message: String,
        stack: Option<String>,
        status: Option<u16>,
    },
    Console {
        stream: String,
        message: String,
    },
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ProcessingEventData {
    #[serde(default)]
    pub(super) evaluators: usize,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExperimentStart {
    #[serde(default, alias = "project_name")]
    pub(super) project_name: Option<String>,
    #[serde(default, alias = "experiment_name")]
    pub(super) experiment_name: Option<String>,
    #[serde(default, alias = "project_id")]
    pub(super) project_id: Option<String>,
    #[serde(default, alias = "experiment_id")]
    pub(super) experiment_id: Option<String>,
    #[serde(default, alias = "project_url")]
    pub(super) project_url: Option<String>,
    #[serde(default, alias = "experiment_url")]
    pub(super) experiment_url: Option<String>,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ExperimentSummary {
    pub(super) project_name: String,
    pub(super) experiment_name: String,
    pub(super) project_id: Option<String>,
    pub(super) experiment_id: Option<String>,
    pub(super) project_url: Option<String>,
    pub(super) experiment_url: Option<String>,
    pub(super) comparison_experiment_name: Option<String>,
    pub(super) scores: HashMap<String, ScoreSummary>,
    pub(super) metrics: Option<HashMap<String, MetricSummary>>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct ScoreSummary {
    pub(super) name: String,
    pub(super) score: f64,
    pub(super) diff: Option<f64>,
    #[serde(default)]
    pub(super) improvements: i64,
    #[serde(default)]
    pub(super) regressions: i64,
}

#[derive(Debug, Deserialize)]
pub(super) struct EvalErrorPayload {
    pub(super) message: String,
    pub(super) stack: Option<String>,
    pub(super) status: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize)]
pub(super) struct MetricSummary {
    pub(super) name: String,
    pub(super) metric: f64,
    #[serde(default)]
    pub(super) unit: String,
    pub(super) diff: Option<f64>,
    #[serde(default)]
    pub(super) improvements: i64,
    #[serde(default)]
    pub(super) regressions: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
pub(super) struct SseProgressEventData {
    pub(super) id: String,
    pub(super) object_type: String,
    pub(super) origin: Option<Value>,
    pub(super) format: String,
    pub(super) output_type: String,
    pub(super) name: String,
    pub(super) event: String,
    pub(super) data: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct EvalProgressData {
    #[serde(rename = "type")]
    pub(super) kind_type: String,
    pub(super) kind: String,
    pub(super) total: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub(super) struct SseConsoleEventData {
    pub(super) stream: String,
    pub(super) message: String,
}

#[derive(Debug, Deserialize)]
pub(super) struct SseDependenciesEventData {
    pub(super) files: Vec<String>,
}
