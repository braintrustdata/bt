use std::collections::HashSet;
use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Result;
use indicatif::{MultiProgress, ProgressDrawTarget};
use serde::{Deserialize, Serialize};

use super::{
    animations_enabled, is_quiet, EvalEvent, EvalProgressData, ExperimentStart, ExperimentSummary,
};

pub(super) const REPORTER_PROTOCOL_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalRun {
    pub run_id: String,
    pub evaluator_count: usize,
    pub protocol_version: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalInfo {
    pub run_id: String,
    pub eval_id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub experiment: Option<ExperimentStart>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalCaseInfo {
    pub eval_id: String,
    pub case_id: String,
    pub index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip, default)]
    pub synthetic: bool,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum CaseStatus {
    Completed,
    Errored,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CaseError {
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalCaseResult {
    #[serde(flatten)]
    pub info: EvalCaseInfo,
    pub status: CaseStatus,
    pub duration_ms: u64,
    pub scores: std::collections::HashMap<String, f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<CaseError>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum EvalStatus {
    Completed,
    Errored,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CaseCounts {
    pub completed: usize,
    pub errored: usize,
    pub skipped: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalEnd {
    pub eval_id: String,
    pub status: EvalStatus,
    pub duration_ms: u64,
    pub case_counts: CaseCounts,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub summary: Option<ExperimentSummary>,
    pub errors: Vec<ReporterError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct EvalRunEnd {
    pub run_id: String,
    pub status: EvalStatus,
    pub duration_ms: u64,
    pub errors: Vec<ReporterError>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ErrorScope {
    pub run_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub case_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ReporterError {
    pub scope: ErrorScope,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stack: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<u16>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum ConsoleStream {
    Stdout,
    Stderr,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ConsoleEvent {
    pub stream: ConsoleStream,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eval_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct ProgressEvent {
    pub eval_id: String,
    pub total_cases: u64,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub(super) enum DeltaKind {
    Text,
    Json,
    Reasoning,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct CaseDelta {
    pub eval_id: String,
    pub case_id: String,
    pub kind: DeltaKind,
    pub data: String,
    #[serde(skip, default)]
    pub legacy_progress: Option<super::SseProgressEventData>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub(super) enum EvalReporterEvent {
    #[serde(rename = "run:start")]
    RunStart { run: EvalRun },
    #[serde(rename = "eval:start")]
    EvalStart { eval: EvalInfo },
    #[serde(rename = "case:start")]
    CaseStart { case: EvalCaseInfo },
    #[serde(rename = "case:end")]
    CaseEnd { case: EvalCaseResult },
    #[serde(rename = "eval:end")]
    EvalEnd { eval: EvalEnd },
    #[serde(rename = "run:end")]
    RunEnd { run: EvalRunEnd },
    #[serde(rename = "error")]
    Error { error: ReporterError },
    #[serde(rename = "console")]
    Console { log: ConsoleEvent },
    #[serde(rename = "eval:progress")]
    Progress { progress: ProgressEvent },
    #[serde(rename = "case:delta")]
    CaseDelta { delta: CaseDelta },
}

#[derive(Clone)]
pub(super) struct Terminal {
    progress: Arc<MultiProgress>,
    interactive: bool,
}

impl Terminal {
    pub fn new() -> Self {
        let interactive = std::io::stderr().is_terminal() && animations_enabled() && !is_quiet();
        let target = if interactive {
            ProgressDrawTarget::stderr_with_hz(10)
        } else {
            ProgressDrawTarget::stderr()
        };
        Self {
            progress: Arc::new(MultiProgress::with_draw_target(target)),
            interactive,
        }
    }

    pub fn println(&self, line: impl AsRef<str>) {
        let line = line.as_ref();
        self.progress.suspend(|| eprintln!("{line}"));
    }

    pub fn multiline(&self, text: impl AsRef<str>) {
        let text = text.as_ref();
        self.progress.suspend(|| {
            for line in text.lines() {
                eprintln!("{line}");
            }
        });
    }

    pub fn live_region(&self) -> Arc<MultiProgress> {
        Arc::clone(&self.progress)
    }

    pub fn is_interactive(&self) -> bool {
        self.interactive
    }

    pub fn clear(&self) {
        let _ = self.progress.clear();
    }
}

pub(super) struct EvalReporterContext {
    pub terminal: Terminal,
    pub profile: Option<String>,
    pub output_file: Option<std::path::PathBuf>,
}

pub(super) trait EvalReporter: Send {
    fn name(&self) -> &'static str {
        "reporter"
    }
    fn claims_stdout(&self) -> bool {
        false
    }
    fn wants_case_delta(&self) -> bool {
        false
    }
    fn on_event(&mut self, _event: &EvalReporterEvent) -> Result<()> {
        Ok(())
    }
    fn on_init(&mut self, _ctx: &EvalReporterContext) -> Result<()> {
        Ok(())
    }
    fn on_run_start(&mut self, _run: &EvalRun) -> Result<()> {
        Ok(())
    }
    fn on_eval_start(&mut self, _eval: &EvalInfo) -> Result<()> {
        Ok(())
    }
    fn on_case_start(&mut self, _case: &EvalCaseInfo) -> Result<()> {
        Ok(())
    }
    fn on_case_end(&mut self, _case: &EvalCaseResult) -> Result<()> {
        Ok(())
    }
    fn on_eval_end(&mut self, _eval: &EvalEnd) -> Result<()> {
        Ok(())
    }
    fn on_run_end(&mut self, _run: &EvalRunEnd) -> Result<Option<bool>> {
        Ok(None)
    }
    fn on_error(&mut self, _error: &ReporterError) -> Result<()> {
        Ok(())
    }
    fn on_console(&mut self, _log: &ConsoleEvent) -> Result<()> {
        Ok(())
    }
    fn on_progress(&mut self, _progress: &ProgressEvent) -> Result<()> {
        Ok(())
    }
    fn on_case_delta(&mut self, _delta: &CaseDelta) -> Result<()> {
        Ok(())
    }
    fn finish(&mut self) -> Result<()> {
        Ok(())
    }
}

pub(super) struct ReporterManager {
    reporters: Vec<Box<dyn EvalReporter>>,
    terminal: Terminal,
    failed_reporters: HashSet<usize>,
    run_id: String,
    run_ended: bool,
    finished: bool,
    vetoed: bool,
    run_errors: Vec<ReporterError>,
}

impl ReporterManager {
    pub fn new(
        reporters: Vec<Box<dyn EvalReporter>>,
        profile: Option<String>,
        output_file: Option<std::path::PathBuf>,
    ) -> Result<Self> {
        let terminal = Terminal::new();
        let context = EvalReporterContext {
            terminal: terminal.clone(),
            profile,
            output_file,
        };
        let stdout_reporters: Vec<&str> = reporters
            .iter()
            .filter(|reporter| reporter.claims_stdout())
            .map(|reporter| reporter.name())
            .collect();
        if stdout_reporters.len() > 1 {
            anyhow::bail!(
                "reporters {} all claim stdout; select only one machine-output reporter",
                stdout_reporters.join(", ")
            );
        }
        let mut manager = Self {
            reporters,
            terminal,
            failed_reporters: HashSet::new(),
            // Legacy streams carry no run identity. A stable synthetic ID keeps
            // machine event output deterministic while remaining unique within this run.
            run_id: "legacy-run".to_string(),
            run_ended: false,
            finished: false,
            vetoed: false,
            run_errors: Vec::new(),
        };
        for index in 0..manager.reporters.len() {
            let result = manager.reporters[index].on_init(&context);
            manager.record_failure(index, result);
        }
        Ok(manager)
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn wants_case_delta(&self) -> bool {
        self.reporters.iter().enumerate().any(|(index, reporter)| {
            !self.failed_reporters.contains(&index) && reporter.wants_case_delta()
        })
    }

    pub fn dispatch(&mut self, event: &EvalReporterEvent) {
        if self.finished {
            return;
        }
        if let EvalReporterEvent::Error { error } = event {
            if error.scope.eval_id.is_none() {
                self.run_errors.push(error.clone());
            }
        }
        if matches!(event, EvalReporterEvent::RunEnd { .. }) {
            if self.run_ended {
                return;
            }
            self.run_ended = true;
        }

        for index in 0..self.reporters.len() {
            if self.failed_reporters.contains(&index) {
                continue;
            }
            let result = self.reporters[index].on_event(event);
            if result.is_err() {
                self.record_failure(index, result);
                continue;
            }
            let result = match event {
                EvalReporterEvent::RunStart { run } => self.reporters[index].on_run_start(run),
                EvalReporterEvent::EvalStart { eval } => self.reporters[index].on_eval_start(eval),
                EvalReporterEvent::CaseStart { case } => self.reporters[index].on_case_start(case),
                EvalReporterEvent::CaseEnd { case } => self.reporters[index].on_case_end(case),
                EvalReporterEvent::EvalEnd { eval } => self.reporters[index].on_eval_end(eval),
                EvalReporterEvent::RunEnd { run } => match self.reporters[index].on_run_end(run) {
                    Ok(Some(false)) => {
                        self.vetoed = true;
                        Ok(())
                    }
                    Ok(_) => Ok(()),
                    Err(error) => Err(error),
                },
                EvalReporterEvent::Error { error } => self.reporters[index].on_error(error),
                EvalReporterEvent::Console { log } => self.reporters[index].on_console(log),
                EvalReporterEvent::Progress { progress } => {
                    self.reporters[index].on_progress(progress)
                }
                EvalReporterEvent::CaseDelta { delta } => {
                    self.reporters[index].on_case_delta(delta)
                }
            };
            self.record_failure(index, result);
        }
    }

    fn record_failure(&mut self, index: usize, result: Result<()>) {
        if let Err(error) = result {
            if self.failed_reporters.insert(index) {
                self.terminal.println(format!(
                    "Reporter '{}' failed: {error:#}",
                    self.reporters[index].name()
                ));
            }
        }
    }

    pub fn finish(&mut self, status: EvalStatus) -> bool {
        if self.finished {
            return self.vetoed;
        }
        if !self.run_ended {
            self.dispatch(&EvalReporterEvent::RunEnd {
                run: EvalRunEnd {
                    run_id: self.run_id.clone(),
                    status,
                    duration_ms: 0,
                    errors: self.run_errors.clone(),
                },
            });
        }
        self.terminal.clear();
        for index in 0..self.reporters.len() {
            if self.failed_reporters.contains(&index) {
                continue;
            }
            let result = self.reporters[index].finish();
            self.record_failure(index, result);
        }
        self.finished = true;
        self.vetoed
    }
}

impl Drop for ReporterManager {
    fn drop(&mut self) {
        self.finish(EvalStatus::Errored);
    }
}

pub(super) struct LegacyEventAdapter {
    run_id: String,
    next_case: usize,
}

pub(super) fn decode_canonical_sse_event(
    event_name: &str,
    data: &str,
) -> Option<EvalReporterEvent> {
    if let Ok(event) = serde_json::from_str::<EvalReporterEvent>(data) {
        return Some(mark_synthetic_case(event));
    }
    let event = match event_name {
        "run:start" => serde_json::from_str(data)
            .ok()
            .map(|run| EvalReporterEvent::RunStart { run }),
        "eval:start" => serde_json::from_str(data)
            .ok()
            .map(|eval| EvalReporterEvent::EvalStart { eval }),
        "case:start" => serde_json::from_str(data)
            .ok()
            .map(|case| EvalReporterEvent::CaseStart { case }),
        "case:end" => serde_json::from_str(data)
            .ok()
            .map(|case| EvalReporterEvent::CaseEnd { case }),
        "eval:end" => serde_json::from_str(data)
            .ok()
            .map(|eval| EvalReporterEvent::EvalEnd { eval }),
        "run:end" => serde_json::from_str(data)
            .ok()
            .map(|run| EvalReporterEvent::RunEnd { run }),
        "error" => serde_json::from_str(data)
            .ok()
            .map(|error| EvalReporterEvent::Error { error }),
        "console" => serde_json::from_str(data)
            .ok()
            .map(|log| EvalReporterEvent::Console { log }),
        "eval:progress" => serde_json::from_str(data)
            .ok()
            .map(|progress| EvalReporterEvent::Progress { progress }),
        "case:delta" => serde_json::from_str(data)
            .ok()
            .map(|delta| EvalReporterEvent::CaseDelta { delta }),
        _ => None,
    }?;
    Some(mark_synthetic_case(event))
}

fn mark_synthetic_case(mut event: EvalReporterEvent) -> EvalReporterEvent {
    match &mut event {
        EvalReporterEvent::CaseStart { case } if case.case_id.starts_with("synthetic-") => {
            case.synthetic = true;
        }
        EvalReporterEvent::CaseEnd { case } if case.info.case_id.starts_with("synthetic-") => {
            case.info.synthetic = true;
        }
        _ => {}
    }
    event
}

impl LegacyEventAdapter {
    pub fn new(run_id: String) -> Self {
        Self {
            run_id,
            next_case: 0,
        }
    }

    pub fn translate(&mut self, event: &EvalEvent) -> Option<EvalReporterEvent> {
        match event {
            EvalEvent::Reporter(event) => Some(event.clone()),
            EvalEvent::Processing(payload) => Some(EvalReporterEvent::RunStart {
                run: EvalRun {
                    run_id: self.run_id.clone(),
                    evaluator_count: payload.evaluators,
                    protocol_version: 0,
                },
            }),
            EvalEvent::Start(start) => {
                let name = start
                    .experiment_name
                    .clone()
                    .unwrap_or_else(|| "evaluation".to_string());
                Some(EvalReporterEvent::EvalStart {
                    eval: EvalInfo {
                        run_id: self.run_id.clone(),
                        eval_id: name.clone(),
                        name,
                        experiment: Some(start.clone()),
                    },
                })
            }
            EvalEvent::Summary(summary) => Some(EvalReporterEvent::EvalEnd {
                eval: EvalEnd {
                    eval_id: summary.experiment_name.clone(),
                    status: EvalStatus::Completed,
                    duration_ms: 0,
                    case_counts: CaseCounts::default(),
                    summary: Some(summary.clone()),
                    errors: Vec::new(),
                },
            }),
            EvalEvent::Progress(progress) => {
                let Ok(payload) = serde_json::from_str::<EvalProgressData>(&progress.data) else {
                    let kind = if progress.event.contains("json") {
                        DeltaKind::Json
                    } else if progress.event.contains("reason") {
                        DeltaKind::Reasoning
                    } else {
                        DeltaKind::Text
                    };
                    return Some(EvalReporterEvent::CaseDelta {
                        delta: CaseDelta {
                            eval_id: progress.name.clone(),
                            case_id: progress.id.clone(),
                            kind,
                            data: progress.data.clone(),
                            legacy_progress: Some(progress.clone()),
                        },
                    });
                };
                if payload.kind_type != "eval_progress" {
                    return None;
                }
                match payload.kind.as_str() {
                    "increment" => {
                        self.next_case += 1;
                        Some(EvalReporterEvent::CaseEnd {
                            case: EvalCaseResult {
                                info: EvalCaseInfo {
                                    eval_id: progress.name.clone(),
                                    case_id: format!("synthetic-{}", self.next_case),
                                    index: self.next_case - 1,
                                    name: None,
                                    synthetic: true,
                                },
                                status: CaseStatus::Completed,
                                duration_ms: 0,
                                scores: Default::default(),
                                error: None,
                            },
                        })
                    }
                    "start" | "set_total" => {
                        payload
                            .total
                            .map(|total_cases| EvalReporterEvent::Progress {
                                progress: ProgressEvent {
                                    eval_id: progress.name.clone(),
                                    total_cases,
                                },
                            })
                    }
                    "stop" => None,
                    _ => None,
                }
            }
            EvalEvent::Error {
                message,
                stack,
                status,
            } => Some(EvalReporterEvent::Error {
                error: ReporterError {
                    scope: ErrorScope {
                        run_id: self.run_id.clone(),
                        eval_id: None,
                        case_id: None,
                    },
                    message: message.clone(),
                    stack: stack.clone(),
                    status: *status,
                },
            }),
            EvalEvent::Console { stream, message } => Some(EvalReporterEvent::Console {
                log: ConsoleEvent {
                    stream: if stream == "stderr" {
                        ConsoleStream::Stderr
                    } else {
                        ConsoleStream::Stdout
                    },
                    message: message.clone(),
                    eval_id: None,
                },
            }),
            EvalEvent::Done => Some(EvalReporterEvent::RunEnd {
                run: EvalRunEnd {
                    run_id: self.run_id.clone(),
                    status: EvalStatus::Completed,
                    duration_ms: 0,
                    errors: Vec::new(),
                },
            }),
            EvalEvent::Dependencies { .. } => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RecordingReporter {
        run_ends: usize,
        veto: bool,
    }

    impl EvalReporter for RecordingReporter {
        fn on_run_end(&mut self, _run: &EvalRunEnd) -> Result<Option<bool>> {
            self.run_ends += 1;
            Ok((self.veto).then_some(false))
        }
    }

    #[test]
    fn canonical_event_serializes_with_camel_case() {
        let event = EvalReporterEvent::RunStart {
            run: EvalRun {
                run_id: "run-test".into(),
                evaluator_count: 2,
                protocol_version: REPORTER_PROTOCOL_VERSION,
            },
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            serde_json::json!({
                "type": "run:start",
                "run": {"runId":"run-test", "evaluatorCount":2, "protocolVersion":1}
            })
        );
    }

    #[test]
    fn manager_synthesizes_run_end_and_finishes_once() {
        let reporter = Box::new(RecordingReporter {
            run_ends: 0,
            veto: true,
        });
        let mut manager = ReporterManager::new(vec![reporter], None, None).unwrap();
        assert!(manager.finish(EvalStatus::Errored));
        assert!(manager.finish(EvalStatus::Completed));
    }
}
