use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Write as _;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};
use std::time::UNIX_EPOCH;

use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::{anyhow, bail, Context, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use swc_bundler::{
    Bundle, BundleKind, Bundler, Config as SwcBundlerConfig, Hook, Load, ModuleData, ModuleRecord,
    ModuleType,
};
use swc_common::{comments::NoopComments, sync::Lrc, FileName, Globals, Mark, SourceMap, Span};
use swc_ecma_ast::{EsVersion, KeyValueProp, Module, Program};
use swc_ecma_codegen::to_code_default;
use swc_ecma_loader::{
    resolve::{Resolution, Resolve},
    resolvers::node::NodeModulesResolver,
    TargetEnv as SwcTargetEnv,
};
use swc_ecma_parser::{parse_file_as_module, EsSyntax, Syntax, TsSyntax};
use swc_ecma_transforms_base::helpers::Helpers;
use swc_ecma_transforms_base::{fixer::fixer, resolver};
use swc_ecma_transforms_react::{react, Options as ReactOptions, Runtime as ReactRuntime};
use swc_ecma_transforms_typescript::strip as strip_typescript;
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::auth;
use crate::datasets::api as datasets_api;
use crate::functions::{self, api as functions_api, IfExistsMode};
use crate::http::ApiClient;
use crate::project_context::{resolve_project_optional, resolve_required_project};
use crate::projects::api::{get_project_by_name, list_projects, Project};
use crate::ui::{self, with_spinner};
use crate::utils::{app_project_url, app_project_url_with_encoded_path};

const VIEWS_JS_RUNNER_SOURCE: &str = include_str!("../scripts/views-runner.mjs");
const VIEWS_JS_SDK_SOURCE: &str = include_str!("../scripts/views-sdk.ts");
const VIEWS_PREVIEW_HTML_TEMPLATE: &str = include_str!("../scripts/views-preview.html");
const VIEWS_PREVIEW_REACT_SOURCE: &str =
    include_str!(concat!(env!("OUT_DIR"), "/views-preview-react.js"));
const VIEWS_PREVIEW_REACT_DOM_SOURCE: &str =
    include_str!(concat!(env!("OUT_DIR"), "/views-preview-react-dom.js"));
const VIEWS_PREVIEW_TAILWIND_SOURCE: &str = include_str!(concat!(
    env!("OUT_DIR"),
    "/views-preview-tailwindcss-browser.js"
));
const DEFAULT_TRACE_PREVIEW_LIMIT: usize = 1000;
const DEFAULT_CUSTOM_VIEWS_DIR: &str = "braintrust-custom-views";
const CUSTOM_VIEW_TSCONFIG_FILE: &str = "tsconfig.json";
const CUSTOM_VIEW_TYPES_FILE: &str = "custom-view-env.d.ts";
const VIEW_FILE_PATTERN_HELP: &str =
    "*.view.tsx, *.view.ts, *.view.jsx, *.view.js, *-view.tsx, *-view.ts, *-view.jsx, or *-view.js";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt views push ./views
  bt views push ./conversation.view.tsx --if-exists replace
  bt views trace bootstrap 'Trace Review'
  bt views dataset bootstrap 'Dataset Review' --dataset test-dataset
  bt views trace preview ./conversation.trace-view.tsx --url <BRAINTRUST_TRACE_URL>
  bt views trace preview ./conversation.trace-view.tsx --trace-id <ROOT_SPAN_ID>
  bt views dataset preview ./dataset.dataset-view.tsx --dataset test-dataset --row-index 0
")]
pub struct ViewsArgs {
    #[command(subcommand)]
    command: ViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum ViewsCommands {
    /// Push local custom view definitions
    Push(ViewsPushArgs),
    /// Work with trace custom views
    Trace(TraceViewsArgs),
    /// Work with dataset custom views
    Dataset(DatasetViewsArgs),
}

#[derive(Debug, Clone, Args)]
struct ViewsPushArgs {
    /// File or directory path(s) to scan for custom view definitions.
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,

    /// File or directory path(s) to scan for custom view definitions.
    #[arg(
        long = "file",
        env = "BT_VIEWS_PUSH_FILES",
        value_name = "PATH",
        value_delimiter = ','
    )]
    file_flag: Vec<PathBuf>,

    /// Behavior when a custom view with the same slug already exists.
    #[arg(
        long = "if-exists",
        env = "BT_VIEWS_PUSH_IF_EXISTS",
        value_enum,
        default_value = "error"
    )]
    if_exists: IfExistsMode,

    /// Skip confirmation prompt.
    #[arg(
        long,
        short = 'y',
        env = "BT_VIEWS_PUSH_YES",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    yes: bool,
}

impl ViewsPushArgs {
    fn resolved_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.paths.clone();
        paths.extend(self.file_flag.iter().cloned());
        if paths.is_empty() {
            vec![PathBuf::from(".")]
        } else {
            paths
        }
    }
}

#[derive(Debug, Clone, Args)]
struct TraceViewsArgs {
    #[command(subcommand)]
    command: TraceViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum TraceViewsCommands {
    /// Create a starter trace custom view file
    Bootstrap(TraceViewBootstrapArgs),
    /// Preview one trace custom view locally
    Preview(TraceViewPreviewArgs),
}

#[derive(Debug, Clone, Args)]
struct DatasetViewsArgs {
    #[command(subcommand)]
    command: DatasetViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum DatasetViewsCommands {
    /// Create a starter dataset custom view file
    Bootstrap(DatasetViewBootstrapArgs),
    /// Preview one dataset custom view locally
    Preview(DatasetViewPreviewArgs),
}

#[derive(Debug, Clone, Args)]
struct BootstrapCommonArgs {
    /// Custom view name.
    #[arg(value_name = "NAME")]
    name: String,

    /// Output file or directory path. Defaults to braintrust-custom-views/<name>.<type>-view.tsx.
    #[arg(long = "file", env = "BT_VIEWS_BOOTSTRAP_FILE", value_name = "PATH")]
    file_flag: Option<PathBuf>,

    /// Overwrite an existing file.
    #[arg(
        long,
        short = 'f',
        env = "BT_VIEWS_BOOTSTRAP_FORCE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct TraceViewBootstrapArgs {
    #[command(flatten)]
    common: BootstrapCommonArgs,
}

#[derive(Debug, Clone, Args)]
struct DatasetViewBootstrapArgs {
    #[command(flatten)]
    common: BootstrapCommonArgs,

    /// Dataset name to reference in the starter view.
    #[arg(
        long,
        env = "BT_VIEWS_BOOTSTRAP_DATASET",
        value_name = "NAME",
        conflicts_with = "dataset_id"
    )]
    dataset: Option<String>,

    /// Dataset id to reference in the starter view.
    #[arg(
        long = "dataset-id",
        env = "BT_VIEWS_BOOTSTRAP_DATASET_ID",
        value_name = "ID",
        conflicts_with = "dataset"
    )]
    dataset_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct BootstrapResult {
    path: String,
    tsconfig_path: String,
    tsconfig_created: bool,
    types_path: String,
    types_created: bool,
    view_type: ViewType,
}

#[derive(Debug, Clone, Args)]
struct PreviewCommonArgs {
    /// Custom view file to preview.
    #[arg(value_name = "PATH")]
    path: PathBuf,

    /// View slug or name to preview.
    #[arg(long, env = "BT_VIEWS_PREVIEW_VIEW")]
    view: Option<String>,

    /// Local port to bind. Defaults to an ephemeral port.
    #[arg(long, env = "BT_VIEWS_PREVIEW_PORT", default_value_t = 0)]
    port: u16,

    /// Do not open a browser.
    #[arg(
        long,
        env = "BT_VIEWS_PREVIEW_NO_OPEN",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    no_open: bool,
}

#[derive(Debug, Clone, Args)]
struct TraceViewPreviewArgs {
    #[command(flatten)]
    common: PreviewCommonArgs,

    #[command(flatten)]
    target: TracePreviewTargetArgs,
}

#[derive(Debug, Clone, Args)]
struct TracePreviewTargetArgs {
    /// Braintrust app URL to resolve trace preview data from.
    #[arg(long, env = "BT_VIEWS_PREVIEW_URL")]
    url: Option<String>,

    /// Project ID to query for trace preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_PROJECT_ID")]
    project_id: Option<String>,

    /// Root span id for trace preview data.
    #[arg(
        long = "trace-id",
        alias = "root-span-id",
        env = "BT_VIEWS_PREVIEW_TRACE_ID"
    )]
    trace_id: Option<String>,

    /// Selected span id or row id for trace preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_SPAN_ID")]
    span_id: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct DatasetViewPreviewArgs {
    #[command(flatten)]
    common: PreviewCommonArgs,

    #[command(flatten)]
    target: DatasetPreviewTargetArgs,
}

#[derive(Debug, Clone, Args)]
struct DatasetPreviewTargetArgs {
    /// Dataset name or id for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_DATASET")]
    dataset: Option<String>,

    /// Dataset row id for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_ROW_ID")]
    row_id: Option<String>,

    /// Dataset row index for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_ROW_INDEX")]
    row_index: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct ViewsManifest {
    runtime_context: ViewsRuntimeContext,
    #[serde(default)]
    files: Vec<ViewsManifestFile>,
}

#[derive(Debug, Serialize)]
struct ViewsDiscoveryInput {
    files: Vec<ViewsDiscoveryInputFile>,
}

#[derive(Debug, Serialize)]
struct ViewsDiscoveryInputFile {
    source_file: String,
    bundle_file: String,
}

#[derive(Debug, Deserialize)]
struct ViewsRuntimeContext {
    runtime: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct ViewsManifestFile {
    source_file: String,
    #[allow(dead_code)]
    #[serde(default)]
    dependencies: Vec<String>,
    #[serde(default)]
    entries: Vec<ViewManifestEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct ViewManifestEntry {
    view_type: ViewType,
    name: String,
    slug: String,
    code: String,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
    #[serde(default)]
    dataset_id: Option<String>,
    #[serde(default)]
    dataset_name: Option<String>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ViewType {
    Trace,
    Dataset,
}

impl ViewType {
    fn label(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Dataset => "dataset",
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedView {
    source_file: String,
    entry: ViewManifestEntry,
    project: Project,
    dataset: Option<datasets_api::Dataset>,
}

#[derive(Debug, Serialize)]
struct PushedView {
    source_file: String,
    name: String,
    slug: String,
    view_type: ViewType,
    project_id: String,
    project_name: String,
    dataset_id: Option<String>,
    function_id: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BtqlResponse {
    data: Vec<Map<String, Value>>,
}

struct PreviewServerState {
    client: ApiClient,
    project: Option<Project>,
    target: PreviewTarget,
    source: PreviewSource,
    title: String,
    dependency_paths: Mutex<Vec<PathBuf>>,
    trace_project_id: Mutex<Option<String>>,
    trace_data: Mutex<Option<Value>>,
}

struct PreviewContext {
    client: ApiClient,
    project: Option<Project>,
    source: PreviewSource,
}

struct TracePreviewData {
    project_id: String,
    data: Value,
}

#[derive(Clone)]
struct PreviewSource {
    path: PathBuf,
    root: PathBuf,
    view: Option<String>,
    view_type: ViewType,
}

#[derive(Clone)]
enum PreviewTarget {
    Trace(TracePreviewTargetArgs),
    Dataset(DatasetPreviewTargetArgs),
}

#[derive(Debug, Deserialize)]
struct SpanFieldsRequest {
    #[serde(rename = "spanIds")]
    span_ids: Vec<String>,
    fields: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
struct PreviewDataRequest {
    view_type: ViewType,
    name: String,
    slug: String,
    project_id: Option<String>,
    project_name: Option<String>,
    dataset_id: Option<String>,
    dataset_name: Option<String>,
}

pub async fn run(base: BaseArgs, args: ViewsArgs) -> Result<()> {
    match args.command {
        ViewsCommands::Push(push_args) => push(base, push_args).await,
        ViewsCommands::Trace(trace_args) => match trace_args.command {
            TraceViewsCommands::Bootstrap(bootstrap_args) => bootstrap_trace(base, bootstrap_args),
            TraceViewsCommands::Preview(preview_args) => preview_trace(base, preview_args).await,
        },
        ViewsCommands::Dataset(dataset_args) => match dataset_args.command {
            DatasetViewsCommands::Bootstrap(bootstrap_args) => {
                bootstrap_dataset(base, bootstrap_args)
            }
            DatasetViewsCommands::Preview(preview_args) => {
                preview_dataset(base, preview_args).await
            }
        },
    }
}

fn bootstrap_trace(base: BaseArgs, args: TraceViewBootstrapArgs) -> Result<()> {
    let slug = bootstrap_slug(&args.common.name)?;
    let default_file_name = format!("{slug}.trace-view.tsx");
    let content = trace_view_bootstrap_template(&args.common.name, &slug);
    let result = write_bootstrap_scaffold(args.common, &default_file_name, &content)?;
    print_bootstrap_result(base.json, ViewType::Trace, &result)
}

fn bootstrap_dataset(base: BaseArgs, args: DatasetViewBootstrapArgs) -> Result<()> {
    let slug = bootstrap_slug(&args.common.name)?;
    let dataset_ref = match (
        args.dataset_id.as_deref().map(str::trim),
        args.dataset.as_deref().map(str::trim),
    ) {
        (Some(dataset_id), _) if !dataset_id.is_empty() => {
            format!("{{ id: {dataset_id:?} }}")
        }
        (_, Some(dataset)) if !dataset.is_empty() => format!("{{ name: {dataset:?} }}"),
        _ => "{ name: \"test-dataset\" }".to_string(),
    };
    let default_file_name = format!("{slug}.dataset-view.tsx");
    let content = dataset_view_bootstrap_template(&args.common.name, &slug, &dataset_ref);
    let result = write_bootstrap_scaffold(args.common, &default_file_name, &content)?;
    print_bootstrap_result(base.json, ViewType::Dataset, &result)
}

fn bootstrap_slug(name: &str) -> Result<String> {
    let mut slug = String::new();
    let mut pending_separator = false;

    for ch in name.trim().chars() {
        if ch.is_ascii_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.push(ch.to_ascii_lowercase());
            pending_separator = false;
        } else if !slug.is_empty() {
            pending_separator = true;
        }
    }

    if slug.is_empty() {
        bail!("custom view name must contain at least one ASCII letter or number");
    }
    Ok(slug)
}

struct BootstrapWriteResult {
    view_path: PathBuf,
    tsconfig_path: PathBuf,
    tsconfig_status: BootstrapSupportFileStatus,
    types_path: PathBuf,
    types_status: BootstrapSupportFileStatus,
}

impl BootstrapWriteResult {
    fn tsconfig_created(&self) -> bool {
        matches!(self.tsconfig_status, BootstrapSupportFileStatus::Created)
    }

    fn types_created(&self) -> bool {
        matches!(self.types_status, BootstrapSupportFileStatus::Created)
    }
}

#[derive(Debug, Clone, Copy)]
enum BootstrapSupportFileStatus {
    Created,
    Reused,
    Updated,
}

impl BootstrapSupportFileStatus {
    fn label(self) -> &'static str {
        match self {
            Self::Created => "Created",
            Self::Reused => "Reused",
            Self::Updated => "Updated",
        }
    }
}

fn write_bootstrap_scaffold(
    args: BootstrapCommonArgs,
    default_file_name: &str,
    content: &str,
) -> Result<BootstrapWriteResult> {
    let selected_path = args
        .file_flag
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CUSTOM_VIEWS_DIR));
    let (view_path, config_dir) = if selected_path.is_dir() || selected_path.extension().is_none() {
        (selected_path.join(default_file_name), Some(selected_path))
    } else {
        let config_dir = selected_path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf);
        (selected_path, config_dir)
    };
    let tsconfig_path = config_dir
        .as_ref()
        .map(|dir| dir.join(CUSTOM_VIEW_TSCONFIG_FILE))
        .unwrap_or_else(|| PathBuf::from(CUSTOM_VIEW_TSCONFIG_FILE));
    let types_path = config_dir
        .as_ref()
        .map(|dir| dir.join(CUSTOM_VIEW_TYPES_FILE))
        .unwrap_or_else(|| PathBuf::from(CUSTOM_VIEW_TYPES_FILE));

    let is_tsx_view_file = view_path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".view.tsx") || name.ends_with("-view.tsx"));
    if !is_tsx_view_file {
        bail!("custom view bootstrap path must end with .view.tsx or -view.tsx");
    }
    if view_path.exists() && !args.force {
        bail!(
            "custom view file already exists: {}. Use --force to overwrite.",
            view_path.display()
        );
    }
    if let Some(parent) = view_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    let tsconfig_exists = tsconfig_path.exists();
    if tsconfig_exists && !tsconfig_path.is_file() {
        bail!(
            "custom view tsconfig path already exists and is not a file: {}",
            tsconfig_path.display()
        );
    }
    let types_exists = types_path.exists();
    if types_exists && !types_path.is_file() {
        bail!(
            "custom view types path already exists and is not a file: {}",
            types_path.display()
        );
    }
    std::fs::write(&view_path, content)
        .with_context(|| format!("failed to write custom view file {}", view_path.display()))?;

    let tsconfig_status = if !tsconfig_exists || args.force {
        std::fs::write(&tsconfig_path, CUSTOM_VIEW_TSCONFIG_TEMPLATE).with_context(|| {
            format!(
                "failed to write custom view tsconfig {}",
                tsconfig_path.display()
            )
        })?;
        if tsconfig_exists {
            BootstrapSupportFileStatus::Updated
        } else {
            BootstrapSupportFileStatus::Created
        }
    } else {
        BootstrapSupportFileStatus::Reused
    };

    let types_status = if !types_exists || args.force {
        std::fs::write(&types_path, CUSTOM_VIEW_TYPES_TEMPLATE).with_context(|| {
            format!("failed to write custom view types {}", types_path.display())
        })?;
        if types_exists {
            BootstrapSupportFileStatus::Updated
        } else {
            BootstrapSupportFileStatus::Created
        }
    } else {
        BootstrapSupportFileStatus::Reused
    };

    Ok(BootstrapWriteResult {
        view_path,
        tsconfig_path,
        tsconfig_status,
        types_path,
        types_status,
    })
}

fn print_bootstrap_result(
    json_output: bool,
    view_type: ViewType,
    result: &BootstrapWriteResult,
) -> Result<()> {
    let tsconfig_action = result.tsconfig_status.label();
    let types_action = result.types_status.label();
    let result = BootstrapResult {
        path: result.view_path.display().to_string(),
        tsconfig_path: result.tsconfig_path.display().to_string(),
        tsconfig_created: result.tsconfig_created(),
        types_path: result.types_path.display().to_string(),
        types_created: result.types_created(),
        view_type,
    };
    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "Created {} custom view starter at {}",
            view_type.label(),
            result.path
        );
        println!(
            "{tsconfig_action} TypeScript config at {}",
            result.tsconfig_path
        );
        println!(
            "{types_action} custom view type declarations at {}",
            result.types_path
        );
        println!(
            "Preview it with: bt views {} preview {}",
            view_type.label(),
            result.path
        );
    }
    Ok(())
}

const CUSTOM_VIEW_TSCONFIG_TEMPLATE: &str = r#"{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["DOM", "DOM.Iterable", "ES2020"],
    "module": "ESNext",
    "moduleResolution": "Bundler",
    "jsx": "react-jsx",
    "strict": true,
    "noEmit": true,
    "esModuleInterop": true,
    "allowSyntheticDefaultImports": true,
    "allowJs": true,
    "skipLibCheck": true,
    "types": []
  },
  "include": [
    "**/*.view.ts",
    "**/*.view.tsx",
    "**/*.view.js",
    "**/*.view.jsx",
    "**/*-view.ts",
    "**/*-view.tsx",
    "**/*-view.js",
    "**/*-view.jsx",
    "**/*.d.ts"
  ]
}
"#;

const CUSTOM_VIEW_TYPES_TEMPLATE: &str = r#"type CustomViewElementProps = {
  [propName: string]: unknown;
};

type CustomViewSelectChangeEvent = {
  target: { value: string };
  currentTarget: { value: string };
  preventDefault: () => void;
  stopPropagation: () => void;
};

interface CustomViewSelectProps extends CustomViewElementProps {
  value?: string | number | readonly string[];
  defaultValue?: string | number | readonly string[];
  onChange?: (event: CustomViewSelectChangeEvent) => void;
}

interface CustomViewOptionProps extends CustomViewElementProps {
  value?: string | number;
}

interface CustomViewIntrinsicElements {
  select: CustomViewSelectProps;
  option: CustomViewOptionProps;
  [elementName: string]: CustomViewElementProps;
}

declare namespace JSX {
  interface IntrinsicElements extends CustomViewIntrinsicElements {}
}

declare module "react" {
  type ReactNode = any;
  type ComponentType<Props = any> = (props: Props) => ReactNode;
  type Dispatch<Value> = (value: Value) => void;
  type SetStateAction<Value> = Value | ((previous: Value) => Value);

  const React: {
    createElement: (...args: any[]) => ReactNode;
    Fragment: any;
  };

  export default React;
  export const Children: any;
  export const Component: any;
  export const Fragment: any;
  export const Profiler: any;
  export const PureComponent: any;
  export const StrictMode: any;
  export const Suspense: any;
  export const cloneElement: (...args: any[]) => ReactNode;
  export const createContext: (...args: any[]) => any;
  export const createElement: (...args: any[]) => ReactNode;
  export const createRef: (...args: any[]) => any;
  export const forwardRef: (component: any) => any;
  export const isValidElement: (value: any) => boolean;
  export const lazy: (loader: any) => any;
  export const memo: <Props = any>(component: ComponentType<Props>) => ComponentType<Props>;
  export const startTransition: (callback: () => void) => void;
  export const useCallback: <Value extends (...args: any[]) => any>(callback: Value, deps?: any[]) => Value;
  export const useContext: (context: any) => any;
  export const useDebugValue: (...args: any[]) => void;
  export const useDeferredValue: <Value>(value: Value) => Value;
  export const useEffect: (effect: () => void | (() => void), deps?: any[]) => void;
  export const useId: () => string;
  export const useImperativeHandle: (...args: any[]) => void;
  export const useInsertionEffect: (effect: () => void | (() => void), deps?: any[]) => void;
  export const useLayoutEffect: (effect: () => void | (() => void), deps?: any[]) => void;
  export const useMemo: <Value>(factory: () => Value, deps?: any[]) => Value;
  export const useReducer: (...args: any[]) => any;
  export const useRef: <Value>(initialValue: Value) => { current: Value };
  export const useState: <Value>(initialValue: Value | (() => Value)) => [Value, Dispatch<SetStateAction<Value>>];
  export const useSyncExternalStore: (...args: any[]) => any;
  export const useTransition: () => [boolean, (callback: () => void) => void];
}

declare module "react/jsx-runtime" {
  export namespace JSX {
    interface IntrinsicElements extends CustomViewIntrinsicElements {}
  }

  export const Fragment: any;
  export const jsx: (...args: any[]) => any;
  export const jsxs: (...args: any[]) => any;
}

declare module "react/jsx-dev-runtime" {
  export namespace JSX {
    interface IntrinsicElements extends CustomViewIntrinsicElements {}
  }

  export const Fragment: any;
  export const jsxDEV: (...args: any[]) => any;
}
"#;

const TRACE_VIEW_BOOTSTRAP_TEMPLATE: &str = r##"import { customTraceView } from "braintrust/custom-views";

function pretty(value: unknown) {
  return value === undefined ? "" : JSON.stringify(value, null, 2);
}

export default customTraceView(
  {
    name: __VIEW_NAME__,
    slug: __VIEW_SLUG__,
  },
  ({ trace, span, selectSpan }) => {
    const spanIds = trace.spanOrder.slice(0, 100);

    return (
      <div style={{ fontFamily: "Inter, system-ui, sans-serif", padding: 16, color: "#111827" }}>
        <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
          <strong>Trace starter view</strong>
          <select value={span.span_id} onChange={(event) => selectSpan?.(event.target.value)}>
            {spanIds.map((spanId) => (
              <option key={spanId} value={spanId}>
                {spanId}
              </option>
            ))}
          </select>
        </div>

        <section style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, marginBottom: 12 }}>
          <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Selected span</div>
          <div><strong>row id:</strong> {span.id}</div>
          <div><strong>span id:</strong> {span.span_id}</div>
          <div><strong>children:</strong> {span.children.length}</div>
        </section>

        <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
          <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
            {pretty(span.data.input)}
          </pre>
          <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
            {pretty(span.data.output)}
          </pre>
        </section>
      </div>
    );
  },
);
"##;

const DATASET_VIEW_BOOTSTRAP_TEMPLATE: &str = r##"import { customDatasetView } from "braintrust/custom-views";

function pretty(value: unknown) {
  return value === undefined ? "" : JSON.stringify(value, null, 2);
}

export default customDatasetView(
  {
    name: __VIEW_NAME__,
    slug: __VIEW_SLUG__,
    dataset: __DATASET_REF__,
  },
  ({ id, input, expected, metadata, tags = [] }) => {
    return (
      <div style={{ fontFamily: "Inter, system-ui, sans-serif", padding: 16, color: "#111827" }}>
        <div style={{ marginBottom: 16 }}>
          <strong>Dataset starter view</strong>
          <div style={{ color: "#6b7280", fontSize: 12 }}>row id: {id}</div>
          {tags.length > 0 ? (
            <div style={{ color: "#6b7280", fontSize: 12 }}>tags: {tags.join(", ")}</div>
          ) : null}
        </div>

        <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
          <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
            {pretty(input)}
          </pre>
          <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
            {pretty(expected)}
          </pre>
          <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
            {pretty(metadata)}
          </pre>
        </section>
      </div>
    );
  },
);
"##;

fn js_string_literal(value: &str) -> String {
    serde_json::to_string(value).expect("strings serialize to JSON")
}

fn trace_view_bootstrap_template(name: &str, slug: &str) -> String {
    TRACE_VIEW_BOOTSTRAP_TEMPLATE
        .replace("__VIEW_NAME__", &js_string_literal(name))
        .replace("__VIEW_SLUG__", &js_string_literal(slug))
}

fn dataset_view_bootstrap_template(name: &str, slug: &str, dataset_ref: &str) -> String {
    DATASET_VIEW_BOOTSTRAP_TEMPLATE
        .replace("__VIEW_NAME__", &js_string_literal(name))
        .replace("__VIEW_SLUG__", &js_string_literal(slug))
        .replace("__DATASET_REF__", dataset_ref)
}

async fn push(base: BaseArgs, args: ViewsPushArgs) -> Result<()> {
    let auth_ctx = functions::resolve_auth_context(&base).await?;
    let default_project = resolve_required_project(&base, &auth_ctx.client, true).await?;
    let files = collect_view_files(&args.resolved_paths())?;
    if files.is_empty() {
        bail!("no custom view files found; expected files matching {VIEW_FILE_PATTERN_HELP}");
    }

    let manifest = run_views_runner(&files)?;
    validate_manifest_runtime(&manifest)?;
    let prepared = prepare_views(&auth_ctx.client, &default_project, &manifest).await?;
    if prepared.is_empty() {
        bail!("no custom views were registered by the selected files");
    }

    if !args.yes && ui::can_prompt() {
        let prompt = format!("Push {} custom view(s)?", prepared.len());
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(prompt)
            .default(true)
            .interact()?;
        if !confirmed {
            bail!("custom view push cancelled");
        }
    }

    let events = prepared
        .iter()
        .map(|view| build_insert_event(view, args.if_exists))
        .collect::<Vec<_>>();

    let ignored = with_spinner(
        "Pushing custom views...",
        functions_api::insert_functions(&auth_ctx.client, &events),
    )
    .await
    .map_err(|err| anyhow!(format_views_insert_error(&prepared, args.if_exists, &err)))?
    .ignored_entries
    .unwrap_or(0);

    let pushed = resolve_pushed_views(&auth_ctx.client, &auth_ctx.app_url, &prepared).await?;
    if base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "pushed": pushed,
                "ignored": ignored,
            }))?
        );
        return Ok(());
    }

    eprintln!(
        "{} Pushed {} custom view(s)",
        dialoguer::console::style("Success:").green(),
        pushed.len()
    );
    for view in &pushed {
        let mut details = format!(
            "{} {} ({})",
            view.view_type.label(),
            view.name,
            view.project_name
        );
        if let Some(dataset_id) = &view.dataset_id {
            write!(details, ", dataset {dataset_id}").ok();
        }
        if let Some(url) = &view.url {
            write!(details, ": {url}").ok();
        }
        eprintln!("  {}", details);
    }
    if ignored > 0 {
        eprintln!("  Ignored {} existing custom view(s)", ignored);
    }
    Ok(())
}

async fn preview_trace(base: BaseArgs, args: TraceViewPreviewArgs) -> Result<()> {
    let context = resolve_preview_context(
        &base,
        &args.common,
        ViewType::Trace,
        args.target.url.as_deref(),
    )
    .await?;
    serve_preview(
        base,
        &args.common,
        context.client,
        context.project,
        PreviewTarget::Trace(args.target),
        context.source,
    )
    .await
}

async fn preview_dataset(base: BaseArgs, args: DatasetViewPreviewArgs) -> Result<()> {
    let context = resolve_preview_context(&base, &args.common, ViewType::Dataset, None).await?;
    serve_preview(
        base,
        &args.common,
        context.client,
        context.project,
        PreviewTarget::Dataset(args.target),
        context.source,
    )
    .await
}

async fn resolve_preview_context(
    base: &BaseArgs,
    args: &PreviewCommonArgs,
    view_type: ViewType,
    trace_url: Option<&str>,
) -> Result<PreviewContext> {
    let source = prepare_preview_source(&args.path, args.view.clone(), view_type)?;

    let auth_ctx = auth::login_read_only(&base).await?;
    let client = ApiClient::new(&auth_ctx)?;
    let project = if trace_url_supplies_project(view_type, trace_url) {
        None
    } else if view_type == ViewType::Trace && trace_url.is_some() {
        resolve_project_optional(&base, &client, true).await?
    } else {
        Some(resolve_required_project(&base, &client, true).await?)
    };
    Ok(PreviewContext {
        client,
        project,
        source,
    })
}

async fn serve_preview(
    base: BaseArgs,
    args: &PreviewCommonArgs,
    client: ApiClient,
    project: Option<Project>,
    target: PreviewTarget,
    source: PreviewSource,
) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", args.port))
        .with_context(|| format!("failed to bind preview server on port {}", args.port))?;
    let addr = listener
        .local_addr()
        .context("failed to read preview address")?;
    let url = format!("http://{addr}");
    let title = preview_title(&source);
    let state = web::Data::new(PreviewServerState {
        client,
        project,
        target,
        source,
        title,
        dependency_paths: Mutex::new(Vec::new()),
        trace_project_id: Mutex::new(None),
        trace_data: Mutex::new(None),
    });
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/", web::get().to(preview_index))
            .route("/preview-assets/{asset}.js", web::get().to(preview_asset))
            .route("/preview-module/{tail:.*}", web::get().to(preview_module))
            .route(
                "/preview-virtual/{module}.js",
                web::get().to(preview_virtual_module),
            )
            .route("/preview-version", web::get().to(preview_version))
            .route("/preview-data", web::post().to(preview_data))
            .route("/span-fields", web::post().to(preview_span_fields))
    })
    .workers(1)
    .listen(listener)?
    .run();
    let handle = server.handle();
    tokio::spawn(server);

    if base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "url": url,
                "view": {
                    "path": args.path.display().to_string(),
                    "selector": args.view,
                },
            }))?
        );
    } else {
        println!("Previewing {} at {}", args.path.display(), url);
    }

    if !args.no_open {
        open::that(&url)?;
    }

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for Ctrl+C")?;
    handle.stop(true).await;
    Ok(())
}

async fn preview_index(state: web::Data<PreviewServerState>) -> HttpResponse {
    let dependency_paths = state
        .dependency_paths
        .lock()
        .map(|paths| paths.clone())
        .unwrap_or_default();
    let version = preview_source_version(&state.source, &dependency_paths);
    let html = render_preview_html(&state.title, &state.source, &version);
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

async fn preview_asset(asset: web::Path<String>) -> HttpResponse {
    match preview_asset_source(&asset) {
        Some(code) => HttpResponse::Ok()
            .content_type("application/javascript; charset=utf-8")
            .body(code),
        None => HttpResponse::NotFound().json(json!({
            "error": format!("unknown preview asset '{}'", asset.as_str()),
        })),
    }
}

async fn preview_module(
    state: web::Data<PreviewServerState>,
    tail: web::Path<String>,
) -> HttpResponse {
    match preview_module_result(&state, &tail.into_inner()) {
        Ok(code) => HttpResponse::Ok()
            .content_type("application/javascript; charset=utf-8")
            .body(code),
        Err(err) => HttpResponse::BadRequest().json(json!({
            "error": format!("{err:#}"),
        })),
    }
}

async fn preview_virtual_module(module: web::Path<String>) -> HttpResponse {
    match preview_virtual_module_source(&module) {
        Some(code) => HttpResponse::Ok()
            .content_type("application/javascript; charset=utf-8")
            .body(code),
        None => HttpResponse::NotFound().json(json!({
            "error": format!("unknown preview virtual module '{}'", module.as_str()),
        })),
    }
}

async fn preview_version(state: web::Data<PreviewServerState>) -> HttpResponse {
    let dependency_paths = state
        .dependency_paths
        .lock()
        .map(|paths| paths.clone())
        .unwrap_or_default();
    HttpResponse::Ok().json(json!({
        "version": preview_source_version(&state.source, &dependency_paths),
    }))
}

async fn preview_data(
    state: web::Data<PreviewServerState>,
    body: web::Json<PreviewDataRequest>,
) -> HttpResponse {
    match preview_data_result(&state, body.into_inner()).await {
        Ok(data) => HttpResponse::Ok().json(data),
        Err(err) => HttpResponse::BadRequest().json(json!({
            "error": format!("{err:#}"),
        })),
    }
}

async fn preview_span_fields(
    state: web::Data<PreviewServerState>,
    body: web::Json<SpanFieldsRequest>,
) -> HttpResponse {
    let project_id = match state.trace_project_id.lock() {
        Ok(project_id) => project_id.clone(),
        Err(_) => None,
    };
    let Some(project_id) = project_id else {
        return HttpResponse::BadRequest().json(json!({
            "error": "fetchSpanFields is only available for trace previews"
        }));
    };

    let mut response = Map::new();
    let trace_data = match state.trace_data.lock() {
        Ok(trace_data) => trace_data.clone(),
        Err(_) => None,
    };
    let spans = trace_data
        .as_ref()
        .and_then(|data| data.get("trace"))
        .and_then(|trace| trace.get("spans"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    for span_id in &body.span_ids {
        let row_id = resolve_preview_row_id(&spans, span_id).unwrap_or_else(|| span_id.clone());
        match fetch_full_span_row(&state.client, &project_id, &row_id).await {
            Ok(Some(row)) => {
                response.insert(
                    span_id.clone(),
                    fields_from_row(row, body.fields.as_deref()),
                );
            }
            Ok(None) => {
                response.insert(span_id.clone(), json!({}));
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(json!({
                    "error": format!("{err:#}")
                }));
            }
        }
    }

    HttpResponse::Ok().json(Value::Object(response))
}

fn prepare_preview_source(
    path: &Path,
    view: Option<String>,
    view_type: ViewType,
) -> Result<PreviewSource> {
    if !path.exists() {
        bail!("custom view file not found: {}", path.display());
    }
    if !path.is_file() {
        bail!(
            "custom view preview requires a single view file; got {}",
            path.display()
        );
    }
    if !is_view_file(path) {
        bail!("custom view preview path must match {VIEW_FILE_PATTERN_HELP}");
    }
    let path = std::fs::canonicalize(path)
        .with_context(|| format!("failed to resolve custom view file {}", path.display()))?;
    let root = path
        .parent()
        .ok_or_else(|| {
            anyhow!(
                "custom view file has no parent directory: {}",
                path.display()
            )
        })?
        .to_path_buf();
    Ok(PreviewSource {
        path,
        root,
        view,
        view_type,
    })
}

fn preview_title(source: &PreviewSource) -> String {
    source
        .path
        .file_name()
        .and_then(|name| name.to_str())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| source.view_type.label().to_string())
}

async fn preview_data_result(
    state: &PreviewServerState,
    request: PreviewDataRequest,
) -> Result<Value> {
    if request.view_type != state.source.view_type {
        bail!(
            "selected view is {}, but this preview expects {}",
            request.view_type.label(),
            state.source.view_type.label()
        );
    }
    if let Some(selector) = state.source.view.as_deref() {
        if request.slug != selector && request.name != selector {
            bail!(
                "{} custom view '{selector}' not found in selected file",
                state.source.view_type.label()
            );
        }
    }

    let entry = ViewManifestEntry {
        view_type: request.view_type,
        name: request.name,
        slug: request.slug,
        code: String::new(),
        project_id: request.project_id,
        project_name: request.project_name,
        dataset_id: request.dataset_id,
        dataset_name: request.dataset_name,
    };

    match &state.target {
        PreviewTarget::Trace(args) => {
            let project = match state.project.as_ref() {
                Some(default_project) => {
                    Some(resolve_project_for_entry(&state.client, default_project, &entry).await?)
                }
                None => None,
            };
            let trace_data =
                build_trace_preview_data(&state.client, project.as_ref(), args).await?;
            if let Ok(mut project_id) = state.trace_project_id.lock() {
                *project_id = Some(trace_data.project_id.clone());
            }
            if let Ok(mut data) = state.trace_data.lock() {
                *data = Some(trace_data.data.clone());
            }
            Ok(trace_data.data)
        }
        PreviewTarget::Dataset(args) => {
            let default_project = state
                .project
                .as_ref()
                .ok_or_else(|| anyhow!("dataset preview requires a project"))?;
            let project = resolve_project_for_entry(&state.client, default_project, &entry).await?;
            build_dataset_preview_data(&state.client, &project, &entry, args).await
        }
    }
}

fn preview_module_result(state: &PreviewServerState, tail: &str) -> Result<String> {
    let path = preview_module_path_from_route(&state.source, tail)?;
    let code = compile_preview_module(&path)?;
    if let Ok(mut dependency_paths) = state.dependency_paths.lock() {
        dependency_paths.push(path);
        dependency_paths.sort();
        dependency_paths.dedup();
    }
    Ok(code)
}

fn preview_asset_source(asset: &str) -> Option<&'static str> {
    match asset {
        "react" => Some(VIEWS_PREVIEW_REACT_SOURCE),
        "react-dom" => Some(VIEWS_PREVIEW_REACT_DOM_SOURCE),
        "tailwindcss-browser" => Some(VIEWS_PREVIEW_TAILWIND_SOURCE),
        _ => None,
    }
}

fn preview_virtual_module_source(module: &str) -> Option<&'static str> {
    match module {
        "custom-views" => Some(
            r#"export function customTraceView(definition, component) {
  return { ...definition, component, kind: "trace" };
}
export function customDatasetView(definition, component) {
  return { ...definition, component, kind: "dataset" };
}
"#,
        ),
        "react" => Some(
            r#"const ReactValue = globalThis.React;
export default ReactValue;
export const Children = ReactValue.Children;
export const Component = ReactValue.Component;
export const Fragment = ReactValue.Fragment;
export const Profiler = ReactValue.Profiler;
export const PureComponent = ReactValue.PureComponent;
export const StrictMode = ReactValue.StrictMode;
export const Suspense = ReactValue.Suspense;
export const cloneElement = ReactValue.cloneElement;
export const createContext = ReactValue.createContext;
export const createElement = ReactValue.createElement;
export const createRef = ReactValue.createRef;
export const forwardRef = ReactValue.forwardRef;
export const isValidElement = ReactValue.isValidElement;
export const lazy = ReactValue.lazy;
export const memo = ReactValue.memo;
export const startTransition = ReactValue.startTransition;
export const useCallback = ReactValue.useCallback;
export const useContext = ReactValue.useContext;
export const useDebugValue = ReactValue.useDebugValue;
export const useDeferredValue = ReactValue.useDeferredValue;
export const useEffect = ReactValue.useEffect;
export const useId = ReactValue.useId;
export const useImperativeHandle = ReactValue.useImperativeHandle;
export const useInsertionEffect = ReactValue.useInsertionEffect;
export const useLayoutEffect = ReactValue.useLayoutEffect;
export const useMemo = ReactValue.useMemo;
export const useReducer = ReactValue.useReducer;
export const useRef = ReactValue.useRef;
export const useState = ReactValue.useState;
export const useSyncExternalStore = ReactValue.useSyncExternalStore;
export const useTransition = ReactValue.useTransition;
"#,
        ),
        "react-jsx-runtime" => Some(
            r#"const ReactValue = globalThis.React;
export const Fragment = ReactValue.Fragment;
export function jsx(type, props, key) {
  return ReactValue.createElement(type, key === undefined ? props : { ...props, key });
}
export const jsxs = jsx;
export const jsxDEV = jsx;
"#,
        ),
        _ => None,
    }
}

fn compile_preview_module(path: &Path) -> Result<String> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read preview module {}", path.display()))?;
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
    {
        let json: Value = serde_json::from_str(&source)
            .with_context(|| format!("failed to parse preview JSON module {}", path.display()))?;
        return Ok(format!("export default {};\n", script_json(&json)));
    }

    let cm = Lrc::new(SourceMap::default());
    let fm = cm.new_source_file(Lrc::new(FileName::Real(path.to_path_buf())), source);
    let globals = Globals::new();
    let module = swc_common::GLOBALS
        .set(&globals, || {
            parse_and_transform_swc_module(&cm, &fm, swc_syntax_for_path(path))
        })
        .with_context(|| format!("failed to compile preview module {}", path.display()))?;
    Ok(to_code_default(cm, None, &module))
}

fn preview_module_path_from_route(source: &PreviewSource, tail: &str) -> Result<PathBuf> {
    if tail.trim().is_empty() {
        bail!("preview module path is empty");
    }
    let decoded = urlencoding::decode(tail)
        .with_context(|| format!("failed to decode preview module path '{tail}'"))?;
    let raw = preview_route_tail_to_path(&decoded);
    preview_source_path_from_raw(source, &raw)
}

fn preview_route_tail_to_path(tail: &str) -> PathBuf {
    #[cfg(windows)]
    {
        PathBuf::from(tail.replace('/', "\\"))
    }
    #[cfg(not(windows))]
    {
        Path::new("/").join(tail.trim_start_matches('/'))
    }
}

fn preview_module_url(path: &Path) -> String {
    let mut url = String::from("/preview-module");
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => {
                url.push('/');
                url.push_str(&encode(&prefix.as_os_str().to_string_lossy()));
            }
            std::path::Component::RootDir | std::path::Component::CurDir => {}
            std::path::Component::Normal(part) => {
                url.push('/');
                url.push_str(&encode(&part.to_string_lossy()));
            }
            std::path::Component::ParentDir => {}
        }
    }
    url
}

#[cfg(test)]
fn preview_source_path_from_request(source: &PreviewSource, requested: &str) -> Result<PathBuf> {
    let path = PathBuf::from(requested);
    preview_source_path_from_raw(source, &path)
}

fn preview_source_path_from_raw(source: &PreviewSource, path: &Path) -> Result<PathBuf> {
    for candidate in preview_resolution_candidates(path) {
        if candidate.is_file() {
            let candidate = std::fs::canonicalize(&candidate).with_context(|| {
                format!("failed to resolve preview source {}", candidate.display())
            })?;
            ensure_preview_path_allowed(source, &candidate)?;
            return Ok(candidate);
        }
    }

    bail!("preview source not found: {}", path.display());
}

fn ensure_preview_path_allowed(source: &PreviewSource, path: &Path) -> Result<()> {
    if !path.starts_with(&source.root) {
        bail!(
            "preview source must be inside {}: {}",
            source.root.display(),
            path.display()
        );
    }
    Ok(())
}

#[cfg(test)]
fn resolve_preview_source_path(
    source: &PreviewSource,
    importer: Option<&str>,
    specifier: &str,
) -> Result<PathBuf> {
    let raw = if Path::new(specifier).is_absolute() {
        PathBuf::from(specifier)
    } else if specifier.starts_with('.') {
        let importer = importer
            .map(PathBuf::from)
            .unwrap_or_else(|| source.path.clone());
        let importer = preview_source_path_from_request(source, &importer.display().to_string())?;
        importer
            .parent()
            .ok_or_else(|| anyhow!("preview importer has no parent: {}", importer.display()))?
            .join(specifier)
    } else {
        bail!("preview only supports relative local imports; unsupported import '{specifier}'");
    };

    preview_source_path_from_raw(source, &raw)
        .with_context(|| format!("preview import '{specifier}' not found"))
}

fn preview_resolution_candidates(raw: &Path) -> Vec<PathBuf> {
    const EXTENSIONS: &[&str] = &["tsx", "ts", "jsx", "js", "mjs", "cjs", "json"];
    let mut candidates = Vec::new();
    candidates.push(raw.to_path_buf());
    if raw.extension().is_none() {
        for extension in EXTENSIONS {
            candidates.push(raw.with_extension(extension));
        }
    }
    for extension in EXTENSIONS {
        candidates.push(raw.join(format!("index.{extension}")));
    }
    candidates
}

fn preview_source_version(source: &PreviewSource, dependency_paths: &[PathBuf]) -> String {
    let mut parts = Vec::new();
    push_preview_file_version(&mut parts, &source.path);
    for dependency in dependency_paths {
        if dependency != &source.path {
            push_preview_file_version(&mut parts, dependency);
        }
    }
    parts.join("|")
}

fn push_preview_file_version(parts: &mut Vec<String>, path: &Path) {
    match std::fs::metadata(path) {
        Ok(metadata) => {
            let modified = metadata
                .modified()
                .ok()
                .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                .map(|duration| duration.as_nanos())
                .unwrap_or(0);
            parts.push(format!("{}:{modified}:{}", path.display(), metadata.len()));
        }
        Err(err) => {
            parts.push(format!("{}:error:{err}", path.display()));
        }
    }
}

fn render_preview_html(title: &str, source: &PreviewSource, version: &str) -> String {
    let config_json = script_json(&json!({
        "title": title,
        "version": version,
        "sourcePath": source.path.display().to_string(),
        "sourceModuleUrl": preview_module_url(&source.path),
        "viewType": source.view_type.label(),
        "viewSelector": source.view,
    }));
    VIEWS_PREVIEW_HTML_TEMPLATE
        .replace("__HTML_TITLE__", &html_escape(title))
        .replace("__PREVIEW_CONFIG__", &config_json)
}

fn script_json(value: &Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "null".to_string())
        .replace("</", "<\\/")
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn collect_view_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        let path = path.as_path();
        if !path.exists() {
            bail!("custom view path not found: {}", path.display());
        }
        if path.is_file() {
            if is_view_file(path) {
                files.push(path.to_path_buf());
            }
            continue;
        }
        collect_view_files_in_dir(path, &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_view_files_in_dir(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if should_skip_dir(&path) {
                continue;
            }
            collect_view_files_in_dir(&path, files)?;
        } else if path.is_file() && is_view_file(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn should_skip_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | ".bt" | "node_modules" | "target" | "dist" | "build" | ".venv" | "venv"
            )
        })
}

fn is_view_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    [
        ".view.tsx",
        ".view.ts",
        ".view.jsx",
        ".view.js",
        "-view.tsx",
        "-view.ts",
        "-view.jsx",
        "-view.js",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix))
}

fn run_views_runner(files: &[PathBuf]) -> Result<ViewsManifest> {
    let temp_dir = tempfile::tempdir().context("failed to create custom views temp directory")?;
    let mut input_files = Vec::new();
    let mut bundled_files = BTreeMap::new();
    for file in files {
        let source_file = std::fs::canonicalize(file)
            .with_context(|| format!("failed to resolve custom view file {}", file.display()))?;
        let source_key = source_file.display().to_string();
        let safe_name = source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("view")
            .replace(|character: char| !character.is_ascii_alphanumeric(), "_");

        let discovery =
            swc_bundle_custom_view(&source_file, temp_dir.path(), SwcBundleTarget::Discovery)?;
        let discovery_bundle = temp_dir.path().join(format!("{safe_name}.discovery.cjs"));
        std::fs::write(
            &discovery_bundle,
            module_exports_from_swc_iife(&discovery.code),
        )
        .with_context(|| {
            format!(
                "failed to write custom view discovery bundle {}",
                discovery_bundle.display()
            )
        })?;

        let browser_entry = temp_dir
            .path()
            .join(format!("{safe_name}.browser-entry.ts"));
        std::fs::write(
            &browser_entry,
            format!(
                "import view from {};\nexport default view.component;\n",
                serde_json::to_string(&source_key)?
            ),
        )
        .with_context(|| {
            format!(
                "failed to write custom view browser entry {}",
                browser_entry.display()
            )
        })?;
        let browser =
            swc_bundle_custom_view(&browser_entry, temp_dir.path(), SwcBundleTarget::Browser)?;

        let dependencies = discovery
            .dependencies
            .into_iter()
            .chain(browser.dependencies)
            .collect::<BTreeSet<_>>()
            .into_iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>();
        bundled_files.insert(
            source_key.clone(),
            SwcBundledViewFile {
                code: module_exports_from_swc_iife(&browser.code),
                dependencies,
            },
        );
        input_files.push(ViewsDiscoveryInputFile {
            source_file: source_key,
            bundle_file: discovery_bundle.display().to_string(),
        });
    }
    let input_path = temp_dir.path().join("views-discovery-input.json");
    std::fs::write(
        &input_path,
        serde_json::to_vec(&ViewsDiscoveryInput { files: input_files })?,
    )
    .with_context(|| {
        format!(
            "failed to write custom views discovery input {}",
            input_path.display()
        )
    })?;

    let mut command = Command::new("node");
    command
        .arg("--input-type=module")
        .arg("-")
        .arg(&input_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command.spawn().with_context(|| {
        format!(
            "failed to spawn custom views metadata runner: {}",
            command_display(&command)
        )
    })?;
    child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open custom views metadata runner stdin"))?
        .write_all(VIEWS_JS_RUNNER_SOURCE.as_bytes())
        .context("failed to write custom views metadata runner source")?;
    let output = child
        .wait_with_output()
        .context("failed to wait for custom views metadata runner")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let details = stderr.trim();
        let details = if details.is_empty() {
            stdout.trim()
        } else {
            details
        };
        bail!(
            "custom views metadata runner exited with status {}: {}",
            output.status,
            details
        );
    }

    let stdout = String::from_utf8(output.stdout)
        .context("custom views metadata runner output was not UTF-8")?;
    let mut manifest: ViewsManifest = serde_json::from_str(&stdout).with_context(|| {
        format!(
            "failed to parse custom views metadata runner output as JSON: {}",
            stdout.trim()
        )
    })?;
    for file in &mut manifest.files {
        if let Some(bundled) = bundled_files.get(&file.source_file) {
            file.dependencies.clone_from(&bundled.dependencies);
            for entry in &mut file.entries {
                entry.code.clone_from(&bundled.code);
            }
        }
    }
    Ok(manifest)
}

#[derive(Debug, Clone, Copy)]
enum SwcBundleTarget {
    Discovery,
    Browser,
}

#[derive(Debug)]
struct SwcBundledViewFile {
    code: String,
    dependencies: Vec<String>,
}

#[derive(Debug)]
struct SwcBundleOutput {
    code: String,
    dependencies: Vec<PathBuf>,
}

struct ViewsSwcResolver {
    node: NodeModulesResolver,
}

impl ViewsSwcResolver {
    fn new(target: SwcBundleTarget) -> Self {
        let target_env = match target {
            SwcBundleTarget::Discovery => SwcTargetEnv::Node,
            SwcBundleTarget::Browser => SwcTargetEnv::Browser,
        };
        Self {
            node: NodeModulesResolver::new(target_env, Default::default(), false),
        }
    }
}

impl Resolve for ViewsSwcResolver {
    fn resolve(&self, base: &FileName, module_specifier: &str) -> Result<Resolution> {
        if is_views_virtual_module(module_specifier) {
            return Ok(Resolution {
                filename: FileName::Custom(module_specifier.to_string()),
                slug: None,
            });
        }
        self.node.resolve(base, module_specifier)
    }
}

struct ViewsSwcLoader {
    cm: Lrc<SourceMap>,
    temp_dir: PathBuf,
    dependencies: Arc<Mutex<BTreeSet<PathBuf>>>,
}

impl Load for ViewsSwcLoader {
    fn load(&self, file: &FileName) -> Result<ModuleData> {
        let (fm, syntax) = match file {
            FileName::Real(path) => {
                let source = std::fs::read_to_string(path).with_context(|| {
                    format!("failed to read custom view module {}", path.display())
                })?;
                let resolved = std::fs::canonicalize(path).with_context(|| {
                    format!("failed to resolve custom view module {}", path.display())
                })?;
                if !resolved.starts_with(&self.temp_dir) {
                    self.dependencies
                        .lock()
                        .expect("custom view dependency lock")
                        .insert(resolved);
                }
                (
                    self.cm
                        .new_source_file(Lrc::new(FileName::Real(path.clone())), source),
                    swc_syntax_for_path(path),
                )
            }
            FileName::Custom(name) if name == "braintrust/custom-views" => (
                self.cm
                    .new_source_file(Lrc::new(file.clone()), VIEWS_JS_SDK_SOURCE.to_string()),
                Syntax::Typescript(TsSyntax::default()),
            ),
            FileName::Custom(name) if name == "@braintrust/local/custom-views" => (
                self.cm
                    .new_source_file(Lrc::new(file.clone()), VIEWS_JS_SDK_SOURCE.to_string()),
                Syntax::Typescript(TsSyntax::default()),
            ),
            FileName::Custom(name) if name == "react" => (
                self.cm
                    .new_source_file(Lrc::new(file.clone()), react_module_source()),
                Syntax::Es(EsSyntax::default()),
            ),
            FileName::Custom(name)
                if name == "react/jsx-runtime" || name == "react/jsx-dev-runtime" =>
            {
                (
                    self.cm
                        .new_source_file(Lrc::new(file.clone()), jsx_runtime_module_source()),
                    Syntax::Es(EsSyntax::default()),
                )
            }
            FileName::Custom(name) => bail!("unsupported custom view virtual module '{name}'"),
            _ => bail!("unsupported custom view module {}", file),
        };

        let module = parse_and_transform_swc_module(&self.cm, &fm, syntax)
            .with_context(|| format!("failed to compile custom view module {}", file))?;
        Ok(ModuleData {
            fm,
            module,
            helpers: Helpers::new(false),
        })
    }
}

fn parse_and_transform_swc_module(
    cm: &Lrc<SourceMap>,
    fm: &swc_common::SourceFile,
    syntax: Syntax,
) -> Result<Module> {
    let mut errors = Vec::new();
    let module = parse_file_as_module(fm, syntax, EsVersion::Es2022, None, &mut errors)
        .map_err(|err| anyhow!("{err:?}"))
        .with_context(|| format!("failed to parse module {}", fm.name))?;
    if !errors.is_empty() {
        let details = errors
            .iter()
            .map(|err| format!("{err:?}"))
            .collect::<Vec<_>>()
            .join("; ");
        bail!("failed to parse module {}: {details}", fm.name);
    }

    let unresolved_mark = Mark::new();
    let top_level_mark = Mark::new();
    let mut program = Program::Module(module);
    program.mutate(resolver(
        unresolved_mark,
        top_level_mark,
        syntax_is_typescript(syntax),
    ));
    if syntax_is_typescript(syntax) {
        program.mutate(strip_typescript(unresolved_mark, top_level_mark));
    }
    program.mutate(react(
        cm.clone(),
        None::<NoopComments>,
        ReactOptions {
            runtime: Some(ReactRuntime::Classic),
            pragma: Some("React.createElement".into()),
            pragma_frag: Some("React.Fragment".into()),
            development: Some(false),
            ..Default::default()
        },
        top_level_mark,
        unresolved_mark,
    ));
    program.mutate(fixer(None));

    let Program::Module(module) = program else {
        bail!("module {} did not parse as an ES module", fm.name);
    };
    Ok(module)
}

struct ViewsSwcHook;

impl Hook for ViewsSwcHook {
    fn get_import_meta_props(
        &self,
        _span: Span,
        _module_record: &ModuleRecord,
    ) -> Result<Vec<KeyValueProp>> {
        Ok(Vec::new())
    }
}

fn swc_bundle_custom_view(
    entry: &Path,
    temp_dir: &Path,
    target: SwcBundleTarget,
) -> Result<SwcBundleOutput> {
    let entry = std::fs::canonicalize(entry)
        .with_context(|| format!("failed to resolve custom view entry {}", entry.display()))?;
    let temp_dir = std::fs::canonicalize(temp_dir).with_context(|| {
        format!(
            "failed to resolve custom view temp directory {}",
            temp_dir.display()
        )
    })?;
    let cm = Lrc::new(SourceMap::default());
    let globals = Globals::new();
    let dependencies = Arc::new(Mutex::new(BTreeSet::new()));
    let loader = ViewsSwcLoader {
        cm: cm.clone(),
        temp_dir,
        dependencies: dependencies.clone(),
    };
    let resolver = ViewsSwcResolver::new(target);
    let mut bundler = Bundler::new(
        &globals,
        cm.clone(),
        loader,
        resolver,
        SwcBundlerConfig {
            module: ModuleType::Iife,
            ..Default::default()
        },
        Box::new(ViewsSwcHook),
    );
    let bundles = bundler
        .bundle(HashMap::from([(
            "custom-view".to_string(),
            FileName::Real(entry.clone()),
        )]))
        .with_context(|| format!("failed to bundle custom view {}", entry.display()))?;
    let bundle = single_swc_bundle(bundles, &entry)?;
    Ok(SwcBundleOutput {
        code: to_code_default(cm, None, &bundle.module),
        dependencies: Arc::try_unwrap(dependencies)
            .unwrap_or_else(|dependencies| {
                Mutex::new(
                    dependencies
                        .lock()
                        .expect("custom view dependency lock")
                        .clone(),
                )
            })
            .into_inner()
            .expect("custom view dependency lock")
            .into_iter()
            .collect(),
    })
}

fn single_swc_bundle(mut bundles: Vec<Bundle>, entry: &Path) -> Result<Bundle> {
    if bundles.len() != 1 {
        bail!(
            "expected one custom view bundle for {}, got {}",
            entry.display(),
            bundles.len()
        );
    }
    let bundle = bundles.remove(0);
    if !matches!(bundle.kind, BundleKind::Named { .. }) {
        bail!(
            "custom view bundle for {} was not an entry bundle",
            entry.display()
        );
    }
    Ok(bundle)
}

fn module_exports_from_swc_iife(code: &str) -> String {
    let expression = code.trim().trim_end_matches(';');
    format!(
        "var __BraintrustCustomView = {expression};\nmodule.exports = __BraintrustCustomView;\n"
    )
}

fn is_views_virtual_module(module_specifier: &str) -> bool {
    matches!(
        module_specifier,
        "braintrust/custom-views"
            | "@braintrust/local/custom-views"
            | "react"
            | "react/jsx-runtime"
            | "react/jsx-dev-runtime"
    )
}

fn swc_syntax_for_path(path: &Path) -> Syntax {
    match path.extension().and_then(|extension| extension.to_str()) {
        Some("ts") | Some("mts") | Some("cts") => Syntax::Typescript(TsSyntax::default()),
        Some("tsx") => Syntax::Typescript(TsSyntax {
            tsx: true,
            ..Default::default()
        }),
        Some("jsx") => Syntax::Es(EsSyntax {
            jsx: true,
            ..Default::default()
        }),
        _ => Syntax::Es(EsSyntax::default()),
    }
}

fn syntax_is_typescript(syntax: Syntax) -> bool {
    matches!(syntax, Syntax::Typescript(_))
}

fn react_module_source() -> String {
    r#"
const ReactValue = globalThis.React || React;
export default ReactValue;
export const Children = ReactValue.Children;
export const Component = ReactValue.Component;
export const Fragment = ReactValue.Fragment;
export const Profiler = ReactValue.Profiler;
export const PureComponent = ReactValue.PureComponent;
export const StrictMode = ReactValue.StrictMode;
export const Suspense = ReactValue.Suspense;
export const cloneElement = ReactValue.cloneElement;
export const createContext = ReactValue.createContext;
export const createElement = ReactValue.createElement;
export const createRef = ReactValue.createRef;
export const forwardRef = ReactValue.forwardRef;
export const isValidElement = ReactValue.isValidElement;
export const lazy = ReactValue.lazy;
export const memo = ReactValue.memo;
export const startTransition = ReactValue.startTransition;
export const useCallback = ReactValue.useCallback;
export const useContext = ReactValue.useContext;
export const useDebugValue = ReactValue.useDebugValue;
export const useDeferredValue = ReactValue.useDeferredValue;
export const useEffect = ReactValue.useEffect;
export const useId = ReactValue.useId;
export const useImperativeHandle = ReactValue.useImperativeHandle;
export const useInsertionEffect = ReactValue.useInsertionEffect;
export const useLayoutEffect = ReactValue.useLayoutEffect;
export const useMemo = ReactValue.useMemo;
export const useReducer = ReactValue.useReducer;
export const useRef = ReactValue.useRef;
export const useState = ReactValue.useState;
export const useSyncExternalStore = ReactValue.useSyncExternalStore;
export const useTransition = ReactValue.useTransition;
"#
    .to_string()
}

fn jsx_runtime_module_source() -> String {
    r#"
const ReactValue = globalThis.React || React;
export const Fragment = ReactValue.Fragment;
export const jsx = ReactValue.createElement;
export const jsxs = ReactValue.createElement;
export const jsxDEV = ReactValue.createElement;
"#
    .to_string()
}

fn command_display(command: &Command) -> String {
    let mut rendered = command.get_program().to_string_lossy().to_string();
    for arg in command.get_args() {
        rendered.push(' ');
        rendered.push_str(&arg.to_string_lossy());
    }
    rendered
}

fn validate_manifest_runtime(manifest: &ViewsManifest) -> Result<()> {
    if manifest.runtime_context.runtime != "browser" {
        bail!(
            "custom views runner returned unsupported runtime '{}'",
            manifest.runtime_context.runtime
        );
    }
    if manifest.runtime_context.version.trim().is_empty() {
        bail!("custom views runner returned an empty runtime version");
    }
    Ok(())
}

async fn prepare_views(
    client: &ApiClient,
    default_project: &Project,
    manifest: &ViewsManifest,
) -> Result<Vec<PreparedView>> {
    let mut prepared = Vec::new();
    let mut seen = BTreeSet::new();
    for file in &manifest.files {
        for entry in &file.entries {
            let key = (
                file.source_file.clone(),
                entry.view_type,
                entry.slug.clone(),
            );
            if !seen.insert(key) {
                bail!(
                    "duplicate custom view slug '{}' in {}",
                    entry.slug,
                    file.source_file
                );
            }
            let project = resolve_project_for_entry(client, default_project, entry).await?;
            let dataset = if entry.view_type == ViewType::Dataset {
                Some(resolve_dataset_for_entry(client, &project, entry).await?)
            } else {
                None
            };
            prepared.push(PreparedView {
                source_file: file.source_file.clone(),
                entry: entry.clone(),
                project,
                dataset,
            });
        }
    }
    Ok(prepared)
}

async fn resolve_project_for_entry(
    client: &ApiClient,
    default_project: &Project,
    entry: &ViewManifestEntry,
) -> Result<Project> {
    if let Some(project_id) = entry
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if project_id == default_project.id {
            return Ok(default_project.clone());
        }
        let projects = list_projects(client).await?;
        return projects
            .into_iter()
            .find(|project| project.id == project_id)
            .ok_or_else(|| anyhow!("project id '{project_id}' not found"));
    }

    if let Some(project_name) = entry
        .project_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if project_name == default_project.name {
            return Ok(default_project.clone());
        }
        return get_project_by_name(client, project_name)
            .await?
            .ok_or_else(|| anyhow!("project '{project_name}' not found"));
    }

    Ok(default_project.clone())
}

async fn resolve_dataset_for_entry(
    client: &ApiClient,
    project: &Project,
    entry: &ViewManifestEntry,
) -> Result<datasets_api::Dataset> {
    if let Some(dataset_id) = entry
        .dataset_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let datasets = datasets_api::list_datasets(client, &project.id).await?;
        return datasets
            .into_iter()
            .find(|dataset| dataset.id == dataset_id)
            .ok_or_else(|| {
                anyhow!(
                    "dataset id '{dataset_id}' not found in project '{}'",
                    project.name
                )
            });
    }

    let dataset_name = entry
        .dataset_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "dataset custom view '{}' requires dataset id or name",
                entry.slug
            )
        })?;

    datasets_api::get_dataset_by_name(client, &project.id, dataset_name)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "dataset '{dataset_name}' not found in project '{}'",
                project.name
            )
        })
}

fn build_insert_event(view: &PreparedView, if_exists: IfExistsMode) -> Value {
    let mut object = Map::new();
    object.insert(
        "project_id".to_string(),
        Value::String(view.project.id.clone()),
    );
    object.insert("name".to_string(), Value::String(view.entry.name.clone()));
    object.insert("slug".to_string(), Value::String(view.entry.slug.clone()));
    object.insert(
        "function_type".to_string(),
        Value::String("custom_view".to_string()),
    );
    object.insert(
        "if_exists".to_string(),
        Value::String(if_exists.as_str().to_string()),
    );
    object.insert(
        "function_data".to_string(),
        json!({
            "type": "code",
            "data": {
                "type": "inline",
                "runtime_context": {
                    "runtime": "browser",
                    "version": "latest",
                },
                "code": view.entry.code,
            }
        }),
    );

    if let Some(dataset) = &view.dataset {
        object.insert(
            "origin".to_string(),
            json!({
                "object_type": "dataset",
                "object_id": dataset.id,
            }),
        );
    }

    Value::Object(object)
}

fn format_views_insert_error(
    views: &[PreparedView],
    if_exists: IfExistsMode,
    err: &anyhow::Error,
) -> String {
    let view_list = views
        .iter()
        .map(|view| {
            format!(
                "{} '{}' in project '{}'",
                view.entry.view_type.label(),
                view.entry.slug,
                view.project.name
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let view_list = if view_list.is_empty() {
        "selected custom views".to_string()
    } else {
        view_list
    };
    let details = format!("{err:#}");
    let retry_hint = if if_exists == IfExistsMode::Error {
        " If you are updating an existing custom view, rerun with `bt views push --if-exists replace`."
    } else {
        ""
    };

    format!(
        "failed to push custom views ({view_list}) with --if-exists {}.{retry_hint} Server response: {details}",
        if_exists.as_str()
    )
}

async fn resolve_pushed_views(
    client: &ApiClient,
    app_url: &str,
    views: &[PreparedView],
) -> Result<Vec<PushedView>> {
    let mut pushed = Vec::new();
    for view in views {
        let function =
            functions_api::get_function_by_slug(client, &view.project.id, &view.entry.slug, None)
                .await
                .ok()
                .flatten();
        let function_id = function.as_ref().map(|function| function.id.clone());
        let url = function_id
            .as_deref()
            .map(|id| custom_view_url(app_url, client.org_name(), view, id));
        pushed.push(PushedView {
            source_file: view.source_file.clone(),
            name: view.entry.name.clone(),
            slug: view.entry.slug.clone(),
            view_type: view.entry.view_type,
            project_id: view.project.id.clone(),
            project_name: view.project.name.clone(),
            dataset_id: view.dataset.as_ref().map(|dataset| dataset.id.clone()),
            function_id,
            url,
        });
    }
    Ok(pushed)
}

fn custom_view_url(
    app_url: &str,
    org_name: &str,
    view: &PreparedView,
    function_id: &str,
) -> String {
    match (&view.entry.view_type, view.dataset.as_ref()) {
        (ViewType::Dataset, Some(dataset)) => {
            let mut url = app_project_url(
                app_url,
                org_name,
                &view.project.name,
                &["datasets", &dataset.name],
            );
            write!(url, "?dvt=custom&dv={}", encode(function_id)).ok();
            url
        }
        _ => app_project_url_with_encoded_path(
            app_url,
            org_name,
            &view.project.name,
            &format!("logs?tvt=custom&tv={}", encode(function_id)),
        ),
    }
}

async fn build_dataset_preview_data(
    client: &ApiClient,
    project: &Project,
    entry: &ViewManifestEntry,
    args: &DatasetPreviewTargetArgs,
) -> Result<Value> {
    let dataset = match args.dataset.as_deref() {
        Some(selector) => resolve_dataset_by_selector(client, project, selector).await?,
        None => resolve_dataset_for_entry(client, project, entry).await?,
    };

    let row = if let Some(row_id) = args.row_id.as_deref() {
        datasets_api::get_dataset_row_by_id(client, &dataset.id, row_id)
            .await?
            .ok_or_else(|| anyhow!("dataset row id '{row_id}' not found in '{}'", dataset.name))?
    } else {
        let index = args.row_index.unwrap_or(0);
        let limit = index + 1;
        let (rows, _) = datasets_api::list_dataset_rows_limited(
            client,
            &dataset.id,
            Some(limit),
            datasets_api::DatasetRowsPreviewLength::Full,
        )
        .await?;
        rows.into_iter().nth(index).ok_or_else(|| {
            anyhow!(
                "dataset '{}' does not have row index {}",
                dataset.name,
                index
            )
        })?
    };

    Ok(json!({
        "props": {
            "id": row.get("id").cloned().unwrap_or(Value::Null),
            "input": row.get("input").cloned().unwrap_or(Value::Null),
            "expected": row.get("expected").cloned().unwrap_or(Value::Null),
            "metadata": row.get("metadata").cloned().unwrap_or_else(|| json!({})),
            "tags": row.get("tags").cloned().unwrap_or_else(|| json!([])),
        },
        "dataset": dataset,
    }))
}

async fn resolve_dataset_by_selector(
    client: &ApiClient,
    project: &Project,
    selector: &str,
) -> Result<datasets_api::Dataset> {
    let datasets = datasets_api::list_datasets(client, &project.id).await?;
    datasets
        .into_iter()
        .find(|dataset| dataset.id == selector || dataset.name == selector)
        .ok_or_else(|| {
            anyhow!(
                "dataset '{selector}' not found in project '{}'",
                project.name
            )
        })
}

async fn build_trace_preview_data(
    client: &ApiClient,
    project: Option<&Project>,
    args: &TracePreviewTargetArgs,
) -> Result<TracePreviewData> {
    let target = resolve_trace_preview_target(client, project, args).await?;
    let rows = fetch_trace_rows(
        client,
        &target.project_id,
        &target.root_span_id,
        DEFAULT_TRACE_PREVIEW_LIMIT,
    )
    .await?;
    if rows.is_empty() {
        bail!("trace '{}' returned no spans", target.root_span_id);
    }
    let (trace, selected_span) =
        build_trace_payload(rows, &target.root_span_id, target.span_id.as_deref())?;
    Ok(TracePreviewData {
        project_id: target.project_id,
        data: json!({
            "trace": trace,
            "span": selected_span,
        }),
    })
}

#[derive(Debug)]
struct TracePreviewTarget {
    project_id: String,
    root_span_id: String,
    span_id: Option<String>,
}

async fn resolve_trace_preview_target(
    client: &ApiClient,
    default_project: Option<&Project>,
    args: &TracePreviewTargetArgs,
) -> Result<TracePreviewTarget> {
    if let Some(url) = args.url.as_deref() {
        let parsed = parse_trace_url(url)?;
        let project_id = match parsed.project.as_deref() {
            Some(project)
                if default_project
                    .map(|default_project| {
                        project == default_project.id || project == default_project.name
                    })
                    .unwrap_or(false) =>
            {
                default_project.expect("checked above").id.clone()
            }
            Some(project) if is_uuid_like(project) => project.to_string(),
            Some(project) => {
                get_project_by_name(client, project)
                    .await?
                    .ok_or_else(|| anyhow!("project '{project}' from trace URL not found"))?
                    .id
            }
            None => default_project
                .map(|project| project.id.clone())
                .ok_or_else(|| {
                    anyhow!(
                        "trace URL must include a project path like /app/<org>/p/<project>/... or a project must be supplied"
                    )
                })?,
        };
        let root_span_id = parsed
            .row_ref
            .or(parsed.span_id.clone())
            .ok_or_else(|| anyhow!("trace URL must include query parameter r or s"))?;
        return Ok(TracePreviewTarget {
            project_id,
            root_span_id,
            span_id: args.span_id.clone().or(parsed.span_id),
        });
    }

    let project_id = args
        .project_id
        .clone()
        .or_else(|| default_project.map(|project| project.id.clone()))
        .ok_or_else(|| {
            anyhow!("trace preview requires --project-id or --project when --trace-id is used")
        })?;
    let root_span_id = args
        .trace_id
        .clone()
        .ok_or_else(|| anyhow!("trace preview requires --trace-id or --url"))?;
    Ok(TracePreviewTarget {
        project_id,
        root_span_id,
        span_id: args.span_id.clone(),
    })
}

#[derive(Debug)]
struct ParsedPreviewTraceUrl {
    project: Option<String>,
    row_ref: Option<String>,
    span_id: Option<String>,
}

fn parse_trace_url(input: &str) -> Result<ParsedPreviewTraceUrl> {
    let parsed_url = Url::parse(input)
        .or_else(|_| Url::parse(&format!("https://{}", input.trim_start_matches('/'))))
        .context("invalid trace URL")?;
    let mut parsed = ParsedPreviewTraceUrl {
        project: None,
        row_ref: None,
        span_id: None,
    };
    if let Some(segments) = parsed_url.path_segments() {
        let parts = segments.filter(|part| !part.is_empty()).collect::<Vec<_>>();
        if parts.len() >= 4 && parts[0] == "app" && parts[2] == "p" {
            parsed.project = Some(parts[3].to_string());
        }
    }
    for (key, value) in parsed_url.query_pairs() {
        match key.as_ref() {
            "r" if !value.is_empty() => parsed.row_ref = Some(value.to_string()),
            "s" if !value.is_empty() => parsed.span_id = Some(value.to_string()),
            _ => {}
        }
    }
    Ok(parsed)
}

fn trace_url_supplies_project(view_type: ViewType, trace_url: Option<&str>) -> bool {
    view_type == ViewType::Trace
        && trace_url
            .and_then(|url| parse_trace_url(url).ok())
            .and_then(|parsed| parsed.project)
            .is_some()
}

fn is_uuid_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (idx, b) in bytes.iter().enumerate() {
        let is_hyphen = matches!(idx, 8 | 13 | 18 | 23);
        if is_hyphen {
            if *b != b'-' {
                return false;
            }
        } else if !(*b as char).is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

async fn fetch_trace_rows(
    client: &ApiClient,
    project_id: &str,
    root_span_id: &str,
    limit: usize,
) -> Result<Vec<Map<String, Value>>> {
    let query = format!(
        "select: * | from: project_logs({}) spans | filter: root_span_id = {} | preview_length: 125 | sort: _pagination_key ASC | limit: {}",
        sql_quote(project_id),
        sql_quote(root_span_id),
        limit,
    );
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
        .map(|response| response.data)
}

async fn fetch_full_span_row(
    client: &ApiClient,
    project_id: &str,
    row_id: &str,
) -> Result<Option<Map<String, Value>>> {
    let query = format!(
        "select: * | from: project_logs({}) spans | filter: id = {} | preview_length: -1 | limit: 1",
        sql_quote(project_id),
        sql_quote(row_id),
    );
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
        .map(|response| response.data.into_iter().next())
}

async fn execute_query(client: &ApiClient, query: &str) -> Result<BtqlResponse> {
    let body = json!({
        "query": query,
        "fmt": "json",
        "query_source": "bt_views_preview_7e8680d0f2484e2fbef8a61f2de0b9df",
    });
    let org_name = client.org_name();
    let headers = if org_name.is_empty() {
        Vec::new()
    } else {
        vec![("x-bt-org-name", org_name)]
    };
    client.post_with_headers("/btql", &body, &headers).await
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn build_trace_payload(
    rows: Vec<Map<String, Value>>,
    root_span_id: &str,
    selected_selector: Option<&str>,
) -> Result<(Value, Value)> {
    let mut spans = Map::new();
    let mut span_order = Vec::new();
    let mut parent_children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut row_id_to_span_id = BTreeMap::new();

    for row in &rows {
        let Some(span_id) = row.get("span_id").and_then(Value::as_str) else {
            continue;
        };
        span_order.push(Value::String(span_id.to_string()));
        if let Some(row_id) = row.get("id").and_then(Value::as_str) {
            row_id_to_span_id.insert(row_id.to_string(), span_id.to_string());
        }
        if let Some(parent) = parent_span_id(row) {
            parent_children
                .entry(parent)
                .or_default()
                .push(span_id.to_string());
        }
    }

    for row in rows {
        let Some(span_id) = row.get("span_id").and_then(Value::as_str) else {
            continue;
        };
        let span_id = span_id.to_string();
        let children = parent_children
            .get(&span_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();
        spans.insert(span_id.clone(), row_to_custom_span(row, children));
    }

    let selected_span_id = selected_selector
        .and_then(|selector| {
            spans
                .contains_key(selector)
                .then(|| selector.to_string())
                .or_else(|| row_id_to_span_id.get(selector).cloned())
        })
        .or_else(|| {
            spans
                .contains_key(root_span_id)
                .then(|| root_span_id.to_string())
        })
        .or_else(|| {
            span_order
                .first()
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| anyhow!("trace has no selectable spans"))?;
    let selected_span = spans
        .get(&selected_span_id)
        .cloned()
        .ok_or_else(|| anyhow!("selected span '{selected_span_id}' not found"))?;

    Ok((
        json!({
            "rootSpanId": root_span_id,
            "selectedSpanId": selected_span_id,
            "spanOrder": span_order,
            "spans": spans,
        }),
        selected_span,
    ))
}

fn row_to_custom_span(row: Map<String, Value>, children: Vec<Value>) -> Value {
    let data_fields = [
        "input",
        "output",
        "expected",
        "metadata",
        "scores",
        "metrics",
        "error",
        "tags",
        "span_attributes",
    ];
    let mut data = Map::new();
    for field in data_fields {
        if let Some(value) = row.get(field) {
            data.insert(field.to_string(), value.clone());
        }
    }

    let mut span = Map::new();
    for field in ["id", "span_id", "root_span_id", "parent_span_id"] {
        if let Some(value) = row.get(field) {
            span.insert(field.to_string(), value.clone());
        }
    }
    if let Some(value) = row.get("span_parents") {
        span.insert("span_parents".to_string(), value.clone());
    }
    span.insert("data".to_string(), Value::Object(data));
    span.insert("children".to_string(), Value::Array(children));
    Value::Object(span)
}

fn parent_span_id(row: &Map<String, Value>) -> Option<String> {
    row.get("parent_span_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            row.get("span_parents")
                .and_then(Value::as_array)
                .and_then(|parents| parents.last())
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

fn resolve_preview_row_id(spans: &Map<String, Value>, requested: &str) -> Option<String> {
    spans
        .get(requested)
        .and_then(|span| span.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            spans.values().find_map(|span| {
                let row_id = span.get("id").and_then(Value::as_str)?;
                (row_id == requested).then(|| row_id.to_string())
            })
        })
}

fn fields_from_row(row: Map<String, Value>, fields: Option<&[String]>) -> Value {
    let requested = fields
        .map(|fields| fields.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| vec!["input", "output", "expected", "metadata"]);
    let mut object = Map::new();
    for field in requested {
        if matches!(field, "input" | "output" | "expected" | "metadata") {
            if let Some(value) = row.get(field) {
                object.insert(field.to_string(), value.clone());
            }
        }
    }
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_client() -> ApiClient {
        let login = braintrust_sdk_rust::LoginState::new();
        login.set(
            "sk-test".to_string(),
            "org_test".to_string(),
            "test-org".to_string(),
            "http://127.0.0.1:1".to_string(),
            "http://127.0.0.1:1".to_string(),
        );
        ApiClient::new(&crate::auth::LoginContext {
            login,
            api_url: "http://127.0.0.1:1".to_string(),
            app_url: "http://127.0.0.1:1".to_string(),
        })
        .expect("test client")
    }

    fn test_project() -> Project {
        Project {
            id: "proj_test".to_string(),
            name: "test-project".to_string(),
            org_id: "org_test".to_string(),
            description: None,
        }
    }

    #[test]
    fn detects_view_files() {
        assert!(is_view_file(Path::new("conversation.view.tsx")));
        assert!(is_view_file(Path::new("dataset.view.js")));
        assert!(is_view_file(Path::new("conversation.trace-view.tsx")));
        assert!(is_view_file(Path::new("dataset.dataset-view.js")));
        assert!(!is_view_file(Path::new("regular.tsx")));
    }

    #[test]
    fn push_runner_bundles_tsx_with_swc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let dependency = dir.path().join("label.ts");
        let path = dir.path().join("swc.trace-view.tsx");
        std::fs::write(&dependency, "export const label = 'SWC bundled';\n")
            .expect("write dependency");
        std::fs::write(
            &path,
            r#"
import { customTraceView } from "braintrust/custom-views";
import { label } from "./label";

export default customTraceView(
  { name: "SWC Trace", slug: "swc-trace", project: { id: "proj_test" } },
  function SwcTraceView() {
    return <section>{label}</section>;
  },
);
"#,
        )
        .expect("write view");

        let manifest = run_views_runner(&[path.clone()]).expect("views manifest");

        assert_eq!(manifest.runtime_context.runtime, "browser");
        assert_eq!(manifest.files.len(), 1);
        let file = &manifest.files[0];
        assert_eq!(
            file.source_file,
            path.canonicalize()
                .expect("canonical view")
                .display()
                .to_string()
        );
        assert!(file.dependencies.iter().any(|item| item
            == &dependency
                .canonicalize()
                .expect("canonical dependency")
                .display()
                .to_string()));
        assert_eq!(file.entries.len(), 1);
        let entry = &file.entries[0];
        assert_eq!(entry.view_type, ViewType::Trace);
        assert_eq!(entry.name, "SWC Trace");
        assert_eq!(entry.slug, "swc-trace");
        assert_eq!(entry.project_id.as_deref(), Some("proj_test"));
        assert!(entry
            .code
            .contains("module.exports = __BraintrustCustomView"));
        assert!(entry.code.contains("React.createElement"));
        assert!(!entry.code.contains("esbuild"));
        assert!(!entry.code.contains(": string"));
    }

    fn test_preview_source(path: PathBuf, root: PathBuf) -> PreviewSource {
        PreviewSource {
            path,
            root,
            view: None,
            view_type: ViewType::Trace,
        }
    }

    #[test]
    fn preview_html_includes_hot_reload_polling() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        std::fs::write(&path, "export default null;\n").expect("write view");
        let source = test_preview_source(path, dir.path().to_path_buf());
        let html = render_preview_html("Preview", &source, "v1");

        assert!(html.contains("previewVersion"));
        assert!(html.contains("/preview-version"));
        assert!(html.contains("window.location.reload()"));
        assert!(html.contains("sourceModuleUrl"));
        assert!(html.contains("/preview-module/"));
        assert!(html.contains("/preview-assets/tailwindcss-browser.js"));
        assert!(html.contains("/preview-assets/react.js"));
        assert!(html.contains("/preview-assets/react-dom.js"));
        assert!(!html.contains("https://cdn.tailwindcss.com"));
        assert!(!html.contains("https://unpkg.com"));
    }

    #[test]
    fn preview_assets_are_embedded_from_npm() {
        let react = preview_asset_source("react").expect("react preview asset");
        let react_dom = preview_asset_source("react-dom").expect("react-dom preview asset");
        let tailwind = preview_asset_source("tailwindcss-browser").expect("tailwind preview asset");

        assert!(react.contains("React"));
        assert!(react_dom.contains("ReactDOM"));
        assert!(tailwind.contains("tailwind"));
        assert!(preview_asset_source("missing").is_none());
    }

    #[test]
    fn preview_source_path_allows_files_inside_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        let dependency = dir.path().join("component.tsx");
        std::fs::write(&path, "export { default } from './component';\n").expect("write view");
        std::fs::write(&dependency, "export default null;\n").expect("write dependency");
        let source = test_preview_source(path, dir.path().canonicalize().expect("root"));

        let resolved = preview_source_path_from_request(&source, &dependency.display().to_string())
            .expect("dependency should be allowed");

        assert_eq!(
            resolved,
            dependency.canonicalize().expect("canonical dependency")
        );
    }

    #[test]
    fn preview_source_path_rejects_files_outside_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let outside_dir = tempfile::tempdir().expect("outside tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        let outside = outside_dir.path().join("component.tsx");
        std::fs::write(&path, "export default null;\n").expect("write view");
        std::fs::write(&outside, "export default null;\n").expect("write outside dependency");
        let source = test_preview_source(path, dir.path().canonicalize().expect("root"));

        let error = preview_source_path_from_request(&source, &outside.display().to_string())
            .expect_err("outside dependency should be rejected");

        assert!(format!("{error:#}").contains("preview source must be inside"));
    }

    #[test]
    fn preview_relative_import_resolution_stays_inside_root() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        let dependency = dir.path().join("component.tsx");
        std::fs::write(&path, "export { default } from './component';\n").expect("write view");
        std::fs::write(&dependency, "export default null;\n").expect("write dependency");
        let source = test_preview_source(
            path.canonicalize().expect("canonical view"),
            dir.path().canonicalize().expect("root"),
        );

        let resolved = resolve_preview_source_path(
            &source,
            Some(&source.path.display().to_string()),
            "./component",
        )
        .expect("relative import should resolve");

        assert_eq!(
            resolved,
            dependency.canonicalize().expect("canonical dependency")
        );
    }

    #[test]
    fn preview_module_compiles_tsx_with_swc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        std::fs::write(
            &path,
            r#"import { customTraceView } from "braintrust/custom-views";

type Props = { span: { id: string } };

export default customTraceView({ name: "Test View", slug: "test-view" }, ({ span }: Props) => {
  return <div>{span.id}</div>;
});
"#,
        )
        .expect("write view");

        let code = compile_preview_module(&path).expect("compile TSX preview module");

        assert!(code.contains("React.createElement"));
        assert!(code.contains("customTraceView"));
        assert!(!code.contains("type Props"));
        assert!(!code.contains("<div>"));
    }

    #[test]
    fn preview_source_version_changes_when_source_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        std::fs::write(&path, "export default null;\n").expect("write view");
        let source = test_preview_source(path.clone(), dir.path().to_path_buf());

        let before = preview_source_version(&source, &[]);
        std::fs::write(&path, "export default function View() { return null; }\n")
            .expect("rewrite view");
        let after = preview_source_version(&source, &[]);

        assert_ne!(before, after);
    }

    #[test]
    fn preview_source_version_changes_when_dependency_changes() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.trace-view.tsx");
        let dependency = dir.path().join("component.tsx");
        std::fs::write(&path, "export { default } from './component';\n").expect("write view");
        std::fs::write(
            &dependency,
            "export default function View() { return null; }\n",
        )
        .expect("write dependency");
        let source = test_preview_source(path, dir.path().to_path_buf());

        let dependency_paths = vec![dependency.clone()];
        let before = preview_source_version(&source, &dependency_paths);
        std::fs::write(
            &dependency,
            "export default function View() { return 'changed'; }\n",
        )
        .expect("rewrite dependency");
        let after = preview_source_version(&source, &dependency_paths);

        assert_ne!(before, after);
    }

    #[test]
    fn parses_trace_preview_url_project_and_span_selectors() {
        let parsed = parse_trace_url(
            "https://www.braintrust.dev/app/test-org/p/test-project/logs?r=root-span&s=selected-span",
        )
        .expect("parse URL");

        assert_eq!(parsed.project.as_deref(), Some("test-project"));
        assert_eq!(parsed.row_ref.as_deref(), Some("root-span"));
        assert_eq!(parsed.span_id.as_deref(), Some("selected-span"));
    }

    #[test]
    fn trace_url_project_only_skips_project_resolution_for_trace_views() {
        let url = Some("https://www.braintrust.dev/app/test-org/p/test-project/logs?r=root-span");

        assert!(trace_url_supplies_project(ViewType::Trace, url));
        assert!(!trace_url_supplies_project(ViewType::Dataset, url));
        assert!(!trace_url_supplies_project(
            ViewType::Trace,
            Some("https://www.braintrust.dev/app/test-org/logs?r=root-span")
        ));
        assert!(!trace_url_supplies_project(ViewType::Trace, None));
    }

    #[tokio::test]
    async fn trace_preview_url_project_id_does_not_require_default_project() {
        let client = test_client();
        let project_id = "00000000-0000-4000-8000-000000000001";
        let target = resolve_trace_preview_target(
            &client,
            None,
            &TracePreviewTargetArgs {
                url: Some(format!(
                    "https://www.braintrust.dev/app/test-org/p/{project_id}/logs?r=root-span&s=selected-span"
                )),
                project_id: None,
                trace_id: None,
                span_id: None,
            },
        )
        .await
        .expect("resolve target");

        assert_eq!(target.project_id, project_id);
        assert_eq!(target.root_span_id, "root-span");
        assert_eq!(target.span_id.as_deref(), Some("selected-span"));
    }

    #[tokio::test]
    async fn trace_preview_url_without_project_uses_default_project() {
        let client = test_client();
        let project = test_project();
        let target = resolve_trace_preview_target(
            &client,
            Some(&project),
            &TracePreviewTargetArgs {
                url: Some("https://www.braintrust.dev/app/test-org/logs?r=root-span".to_string()),
                project_id: None,
                trace_id: None,
                span_id: Some("selected-span".to_string()),
            },
        )
        .await
        .expect("resolve target");

        assert_eq!(target.project_id, "proj_test");
        assert_eq!(target.root_span_id, "root-span");
        assert_eq!(target.span_id.as_deref(), Some("selected-span"));
    }

    #[tokio::test]
    async fn trace_preview_url_without_project_errors_without_default_project() {
        let client = test_client();
        let err = resolve_trace_preview_target(
            &client,
            None,
            &TracePreviewTargetArgs {
                url: Some("https://www.braintrust.dev/app/test-org/logs?r=root-span".to_string()),
                project_id: None,
                trace_id: None,
                span_id: None,
            },
        )
        .await
        .expect_err("missing project should fail");

        assert!(err
            .to_string()
            .contains("trace URL must include a project path"));
    }

    #[test]
    fn normalizes_bootstrap_name_to_slug() {
        assert_eq!(
            bootstrap_slug("Trace Review 2026").unwrap(),
            "trace-review-2026"
        );
        assert_eq!(
            bootstrap_slug("  Dataset__Review!! ").unwrap(),
            "dataset-review"
        );
        assert!(bootstrap_slug("!!!").is_err());
    }

    #[test]
    fn builds_inline_custom_view_insert_event() {
        let view = PreparedView {
            source_file: "test.view.tsx".to_string(),
            entry: ViewManifestEntry {
                view_type: ViewType::Trace,
                name: "Test View".to_string(),
                slug: "test-view".to_string(),
                code: "module.exports = function View() {}".to_string(),
                project_id: None,
                project_name: None,
                dataset_id: None,
                dataset_name: None,
            },
            project: test_project(),
            dataset: None,
        };

        let event = build_insert_event(&view, IfExistsMode::Replace);
        assert_eq!(event["function_type"], "custom_view");
        assert_eq!(event["if_exists"], "replace");
        assert_eq!(event["function_data"]["type"], "code");
        assert_eq!(event["function_data"]["data"]["type"], "inline");
        assert_eq!(
            event["function_data"]["data"]["runtime_context"]["runtime"],
            "browser"
        );
        assert!(event.get("origin").is_none());
        assert!(event.get("description").is_none());
        assert!(event.get("metadata").is_none());
        assert!(event.get("tags").is_none());
    }

    #[test]
    fn formats_actionable_custom_view_insert_error() {
        let view = PreparedView {
            source_file: "test.view.tsx".to_string(),
            entry: ViewManifestEntry {
                view_type: ViewType::Trace,
                name: "Test View".to_string(),
                slug: "test-view".to_string(),
                code: "module.exports = function View() {}".to_string(),
                project_id: None,
                project_name: None,
                dataset_id: None,
                dataset_name: None,
            },
            project: test_project(),
            dataset: None,
        };
        let err = anyhow!("request failed (400 Bad Request): slug already exists");

        let message = format_views_insert_error(&[view], IfExistsMode::Error, &err);

        assert!(message.contains("trace 'test-view' in project 'test-project'"));
        assert!(message.contains("--if-exists error"));
        assert!(message.contains("bt views push --if-exists replace"));
        assert!(message.contains("slug already exists"));
    }

    #[test]
    fn dataset_insert_event_sets_origin() {
        let view = PreparedView {
            source_file: "dataset.view.tsx".to_string(),
            entry: ViewManifestEntry {
                view_type: ViewType::Dataset,
                name: "Dataset View".to_string(),
                slug: "dataset-view".to_string(),
                code: "module.exports = function View() {}".to_string(),
                project_id: None,
                project_name: None,
                dataset_id: Some("dataset_test".to_string()),
                dataset_name: None,
            },
            project: test_project(),
            dataset: Some(datasets_api::Dataset {
                id: "dataset_test".to_string(),
                name: "test-dataset".to_string(),
                project_id: Some("proj_test".to_string()),
                description: None,
                created: None,
                created_at: None,
                metadata: None,
            }),
        };

        let event = build_insert_event(&view, IfExistsMode::Error);
        assert_eq!(event["origin"]["object_type"], "dataset");
        assert_eq!(event["origin"]["object_id"], "dataset_test");
    }
}
