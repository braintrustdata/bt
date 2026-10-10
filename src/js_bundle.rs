use std::collections::{BTreeSet, HashMap};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, bail, Context, Result};
use serde_json::Value;
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

pub(crate) fn collect_matching_files(
    paths: &[PathBuf],
    matches: fn(&Path) -> bool,
    label: &str,
) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        let path = path.as_path();
        if !path.exists() {
            bail!("{label} path not found: {}", path.display());
        }
        if path.is_file() {
            if matches(path) {
                files.push(path.to_path_buf());
            }
            continue;
        }
        collect_matching_files_in_dir(path, matches, &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_matching_files_in_dir(
    dir: &Path,
    matches: fn(&Path) -> bool,
    files: &mut Vec<PathBuf>,
) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let path = entry?.path();
        if path.is_dir() {
            if should_skip_dir(&path) {
                continue;
            }
            collect_matching_files_in_dir(&path, matches, files)?;
        } else if path.is_file() && matches(&path) {
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

pub(crate) fn run_node_metadata_runner(
    runner_source: &str,
    input_path: &Path,
    label: &str,
) -> Result<String> {
    let mut command = Command::new("node");
    command
        .arg("--input-type=module")
        .arg("-")
        .arg(input_path)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let mut child = command
        .spawn()
        .with_context(|| format!("failed to spawn {label}: {}", command_display(&command)))?;
    child
        .stdin
        .take()
        .ok_or_else(|| anyhow!("failed to open {label} stdin"))?
        .write_all(runner_source.as_bytes())
        .with_context(|| format!("failed to write {label} source"))?;
    let output = child
        .wait_with_output()
        .with_context(|| format!("failed to wait for {label}"))?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let details = stderr.trim();
        let details = if details.is_empty() {
            stdout.trim()
        } else {
            details
        };
        bail!("{label} exited with status {}: {}", output.status, details);
    }

    String::from_utf8(output.stdout).with_context(|| format!("{label} output was not UTF-8"))
}

#[derive(Debug, Clone, Copy)]
pub(crate) enum SwcBundleTarget {
    Discovery,
    Browser,
    QuickJs,
}

pub(crate) struct VirtualModule {
    pub(crate) specifier: &'static str,
    pub(crate) source: &'static str,
    pub(crate) typescript: bool,
}

struct SwcResolver {
    node: NodeModulesResolver,
    virtual_modules: &'static [VirtualModule],
}

impl SwcResolver {
    fn new(target: SwcBundleTarget, virtual_modules: &'static [VirtualModule]) -> Self {
        let target_env = match target {
            SwcBundleTarget::Discovery => SwcTargetEnv::Node,
            SwcBundleTarget::Browser | SwcBundleTarget::QuickJs => SwcTargetEnv::Browser,
        };
        Self {
            node: NodeModulesResolver::new(target_env, Default::default(), false),
            virtual_modules,
        }
    }
}

impl Resolve for SwcResolver {
    fn resolve(&self, base: &FileName, module_specifier: &str) -> Result<Resolution> {
        if self
            .virtual_modules
            .iter()
            .any(|module| module.specifier == module_specifier)
        {
            return Ok(Resolution {
                filename: FileName::Custom(module_specifier.to_string()),
                slug: None,
            });
        }
        self.node.resolve(base, module_specifier)
    }
}

pub(crate) struct BundledModule {
    pub(crate) code: String,
    pub(crate) dependency_paths: Vec<PathBuf>,
}

struct SwcLoader {
    cm: Lrc<SourceMap>,
    dependency_paths: Arc<Mutex<BTreeSet<PathBuf>>>,
    virtual_modules: &'static [VirtualModule],
    label: &'static str,
}

impl Load for SwcLoader {
    fn load(&self, file: &FileName) -> Result<ModuleData> {
        let label = self.label;
        let (fm, syntax) = match file {
            FileName::Real(path) => {
                self.dependency_paths
                    .lock()
                    .expect("bundle dependency lock poisoned")
                    .insert(path.clone());
                let source = read_js_module(path, label)?;
                (
                    self.cm
                        .new_source_file(Lrc::new(FileName::Real(path.clone())), source),
                    swc_syntax_for_path(path),
                )
            }
            FileName::Custom(name) => {
                let Some(module) = self
                    .virtual_modules
                    .iter()
                    .find(|module| module.specifier == name)
                else {
                    bail!("unsupported {label} virtual module '{name}'");
                };
                let syntax = if module.typescript {
                    Syntax::Typescript(TsSyntax::default())
                } else {
                    Syntax::Es(EsSyntax::default())
                };
                (
                    self.cm
                        .new_source_file(Lrc::new(file.clone()), module.source.to_string()),
                    syntax,
                )
            }
            _ => bail!("unsupported {label} module {}", file),
        };

        let module = parse_and_transform_swc_module(&self.cm, &fm, syntax)
            .with_context(|| format!("failed to compile {label} module {}", file))?;
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
        matches!(syntax, Syntax::Typescript(_)),
    ));
    if matches!(syntax, Syntax::Typescript(_)) {
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

struct SwcHook;

impl Hook for SwcHook {
    fn get_import_meta_props(
        &self,
        _span: Span,
        _module_record: &ModuleRecord,
    ) -> Result<Vec<KeyValueProp>> {
        Ok(Vec::new())
    }
}

pub(crate) fn swc_bundle_js_module(
    entry: &Path,
    target: SwcBundleTarget,
    virtual_modules: &'static [VirtualModule],
    label: &'static str,
) -> Result<BundledModule> {
    let entry = std::fs::canonicalize(entry)
        .with_context(|| format!("failed to resolve {label} entry {}", entry.display()))?;
    let cm = Lrc::new(SourceMap::default());
    let globals = Globals::new();
    let dependency_paths = Arc::new(Mutex::new(BTreeSet::new()));
    let loader = SwcLoader {
        cm: cm.clone(),
        dependency_paths: dependency_paths.clone(),
        virtual_modules,
        label,
    };
    let resolver = SwcResolver::new(target, virtual_modules);
    let mut bundler = Bundler::new(
        &globals,
        cm.clone(),
        loader,
        resolver,
        SwcBundlerConfig {
            module: ModuleType::Iife,
            ..Default::default()
        },
        Box::new(SwcHook),
    );
    let bundles = bundler
        .bundle(HashMap::from([(
            "entry".to_string(),
            FileName::Real(entry.clone()),
        )]))
        .with_context(|| format!("failed to bundle {label} {}", entry.display()))?;
    let bundle = single_swc_bundle(bundles, &entry, label)?;
    let dependency_paths = dependency_paths
        .lock()
        .expect("bundle dependency lock poisoned")
        .iter()
        .cloned()
        .collect();
    Ok(BundledModule {
        code: to_code_default(cm, None, &bundle.module),
        dependency_paths,
    })
}

fn single_swc_bundle(mut bundles: Vec<Bundle>, entry: &Path, label: &str) -> Result<Bundle> {
    if bundles.len() != 1 {
        bail!(
            "expected one {label} bundle for {}, got {}",
            entry.display(),
            bundles.len()
        );
    }
    let bundle = bundles.remove(0);
    if !matches!(bundle.kind, BundleKind::Named { .. }) {
        bail!(
            "{label} bundle for {} was not an entry bundle",
            entry.display()
        );
    }
    Ok(bundle)
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

fn read_js_module(path: &Path, label: &str) -> Result<String> {
    let source = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read {label} module {}", path.display()))?;
    if path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| extension.eq_ignore_ascii_case("json"))
    {
        let json: Value = serde_json::from_str(&source)
            .with_context(|| format!("failed to parse {label} JSON module {}", path.display()))?;
        let json = serde_json::to_string(&json)?.replace("</", "<\\/");
        return Ok(format!("export default {json};\n"));
    }
    Ok(source)
}

fn command_display(command: &Command) -> String {
    let mut rendered = command.get_program().to_string_lossy().to_string();
    for arg in command.get_args() {
        rendered.push(' ');
        rendered.push_str(&arg.to_string_lossy());
    }
    rendered
}
