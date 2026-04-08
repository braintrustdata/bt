use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand};

use crate::{
    args::BaseArgs,
    http::ApiClient,
    project_context::resolve_project_command_context,
    ui::{self, with_spinner},
};

pub(crate) mod api;
mod create;
mod delete;
mod list;
mod records;
mod refresh;
mod upload;
mod view;

use api::{self as datasets_api, Dataset};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

#[derive(Debug, Clone, Args)]
struct DatasetNameArgs {
    /// Dataset name (positional)
    #[arg(value_name = "NAME", conflicts_with = "name_flag")]
    name_positional: Option<String>,

    /// Dataset name (flag)
    #[arg(long = "name", short = 'n', conflicts_with = "name_positional")]
    name_flag: Option<String>,
}

impl DatasetNameArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct DatasetInputArgs {
    /// JSON/JSONL input file. If omitted, bt reads dataset rows from --rows or stdin.
    #[arg(
        long,
        env = "BT_DATASETS_FILE",
        value_name = "PATH",
        conflicts_with = "rows"
    )]
    file: Option<PathBuf>,

    /// Inline dataset rows as JSON, such as an array of row objects.
    #[arg(
        long,
        env = "BT_DATASETS_ROWS",
        value_name = "JSON",
        conflicts_with = "file"
    )]
    rows: Option<String>,

    /// Dot-separated field path used to read stable record ids.
    #[arg(
        long,
        env = "BT_DATASETS_ID_FIELD",
        value_name = "PATH",
        default_value = "id"
    )]
    id_field: String,
}

#[derive(Debug, Clone, Args)]
#[command(after_help = r#"Examples:
  bt datasets list
  bt datasets create my-dataset
  bt datasets create my-dataset --file records.jsonl
  cat records.jsonl | bt datasets create my-dataset
  bt datasets create my-dataset --rows '[{"id":"case-1","input":{"text":"hi"},"expected":"hello"}]'
  bt datasets add my-dataset --file more-records.jsonl
  bt datasets append my-dataset --rows '[{"id":"case-2","input":{"text":"bye"},"expected":"goodbye"}]'
  bt datasets refresh my-dataset --file records.jsonl --id-field metadata.case_id --prune
  bt datasets view my-dataset
  bt datasets delete my-dataset
"#)]
pub struct DatasetsArgs {
    #[command(subcommand)]
    command: Option<DatasetsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum DatasetsCommands {
    /// List all datasets
    List,
    /// Create a new dataset, optionally seeding rows from a file, --rows, or stdin
    Create(CreateArgs),
    /// Add rows to a remote Braintrust dataset
    #[command(visible_aliases = ["add", "append", "update"])]
    Upload(UploadArgs),
    /// Deterministically refresh a remote Braintrust dataset by record id
    Refresh(RefreshArgs),
    /// View a dataset
    View(ViewArgs),
    /// Delete a dataset
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct CreateArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    #[command(flatten)]
    input: DatasetInputArgs,
}

impl CreateArgs {
    fn name(&self) -> Option<&str> {
        self.name.name()
    }
}

#[derive(Debug, Clone, Args)]
struct UploadArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    #[command(flatten)]
    input: DatasetInputArgs,
}

#[derive(Debug, Clone, Args)]
struct RefreshArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    #[command(flatten)]
    input: DatasetInputArgs,

    /// Delete remote rows whose ids are not present in the input.
    #[arg(
        long,
        env = "BT_DATASETS_REFRESH_PRUNE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    prune: bool,
}

#[derive(Debug, Clone, Args)]
struct ViewArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    /// Open in browser
    #[arg(
        long,
        env = "BT_DATASETS_WEB",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    web: bool,

    /// Show full dataset row payloads
    #[arg(
        long,
        env = "BT_DATASETS_VERBOSE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    verbose: bool,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name.name()
    }
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    /// Skip confirmation
    #[arg(
        long,
        short = 'f',
        env = "BT_DATASETS_FORCE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    force: bool,
}

impl DeleteArgs {
    fn name(&self) -> Option<&str> {
        self.name.name()
    }
}

pub(crate) async fn select_dataset_interactive(
    client: &ApiClient,
    project_id: &str,
) -> Result<Dataset> {
    let mut datasets = with_spinner(
        "Loading datasets...",
        datasets_api::list_datasets(client, project_id),
    )
    .await?;

    if datasets.is_empty() {
        bail!("no datasets found");
    }

    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = datasets
        .iter()
        .map(|dataset| dataset.name.as_str())
        .collect();
    let selection = ui::fuzzy_select("Select dataset", &names, 0)?;
    Ok(datasets[selection].clone())
}

pub async fn run(base: BaseArgs, args: DatasetsArgs) -> Result<()> {
    let ctx = resolve_project_command_context(&base).await?;

    match args.command {
        None | Some(DatasetsCommands::List) => list::run(&ctx, base.json).await,
        Some(DatasetsCommands::Create(create_args)) => {
            create::run(
                &ctx,
                create_args.name(),
                create_args.input.file.as_deref(),
                create_args.input.rows.as_deref(),
                &create_args.input.id_field,
                base.json,
            )
            .await
        }
        Some(DatasetsCommands::Upload(upload_args)) => {
            upload::run(
                &ctx,
                upload_args.name.name(),
                upload_args.input.file.as_deref(),
                upload_args.input.rows.as_deref(),
                &upload_args.input.id_field,
                base.json,
            )
            .await
        }
        Some(DatasetsCommands::Refresh(refresh_args)) => {
            refresh::run(
                &ctx,
                refresh_args.name.name(),
                refresh_args.input.file.as_deref(),
                refresh_args.input.rows.as_deref(),
                &refresh_args.input.id_field,
                refresh_args.prune,
                base.json,
            )
            .await
        }
        Some(DatasetsCommands::View(view_args)) => {
            view::run(
                &ctx,
                view_args.name(),
                base.json,
                view_args.web,
                view_args.verbose,
            )
            .await
        }
        Some(DatasetsCommands::Delete(delete_args)) => {
            delete::run(&ctx, delete_args.name(), delete_args.force).await
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use clap::{Parser, Subcommand};

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(subcommand)]
        command: Commands,
    }

    #[derive(Debug, Subcommand)]
    enum Commands {
        Datasets(DatasetsArgs),
    }

    fn parse(args: &[&str]) -> anyhow::Result<DatasetsArgs> {
        let mut argv = vec!["bt"];
        argv.extend_from_slice(args);
        let parsed = CliHarness::try_parse_from(argv)?;
        match parsed.command {
            Commands::Datasets(args) => Ok(args),
        }
    }

    #[test]
    fn datasets_without_subcommand_defaults_to_list() {
        let parsed = parse(&["datasets"]).expect("parse datasets");
        assert!(parsed.command.is_none());
    }

    #[test]
    fn create_parses_positional_name() {
        let parsed = parse(&["datasets", "create", "my-dataset"]).expect("parse create");
        let DatasetsCommands::Create(create) = parsed.command.expect("subcommand") else {
            panic!("expected create command");
        };
        assert_eq!(create.name(), Some("my-dataset"));
    }

    #[test]
    fn create_parses_file_rows_and_id_field() {
        let parsed = parse(&[
            "datasets",
            "create",
            "my-dataset",
            "--file",
            "records.jsonl",
            "--id-field",
            "metadata.case_id",
        ])
        .expect("parse create with file");
        let DatasetsCommands::Create(create) = parsed.command.expect("subcommand") else {
            panic!("expected create command");
        };
        assert_eq!(create.name(), Some("my-dataset"));
        assert_eq!(create.input.file, Some(PathBuf::from("records.jsonl")));
        assert_eq!(create.input.id_field, "metadata.case_id");
        assert!(create.input.rows.is_none());

        let parsed = parse(&[
            "datasets",
            "create",
            "my-dataset",
            "--rows",
            r#"[{"id":"case-1"}]"#,
        ])
        .expect("parse create with rows");
        let DatasetsCommands::Create(create) = parsed.command.expect("subcommand") else {
            panic!("expected create command");
        };
        assert_eq!(create.input.rows.as_deref(), Some(r#"[{"id":"case-1"}]"#));
        assert!(create.input.file.is_none());
    }

    #[test]
    fn upload_parses_file_and_id_field() {
        let parsed = parse(&[
            "datasets",
            "upload",
            "my-dataset",
            "--file",
            "records.jsonl",
            "--id-field",
            "metadata.case_id",
        ])
        .expect("parse upload");
        let DatasetsCommands::Upload(upload) = parsed.command.expect("subcommand") else {
            panic!("expected upload command");
        };
        assert_eq!(upload.name.name(), Some("my-dataset"));
        assert_eq!(upload.input.file, Some(PathBuf::from("records.jsonl")));
        assert_eq!(upload.input.id_field, "metadata.case_id");
    }

    #[test]
    fn upload_visible_aliases_parse() {
        for alias in ["add", "append", "update"] {
            let parsed = parse(&[
                "datasets",
                alias,
                "my-dataset",
                "--rows",
                r#"[{"id":"case-1"}]"#,
            ])
            .unwrap_or_else(|err| panic!("parse {alias} alias: {err}"));
            let DatasetsCommands::Upload(upload) = parsed.command.expect("subcommand") else {
                panic!("expected upload command");
            };
            assert_eq!(upload.name.name(), Some("my-dataset"));
            assert_eq!(upload.input.rows.as_deref(), Some(r#"[{"id":"case-1"}]"#));
        }
    }

    #[test]
    fn refresh_parses_prune() {
        let parsed = parse(&[
            "datasets",
            "refresh",
            "my-dataset",
            "--file",
            "records.jsonl",
            "--prune",
        ])
        .expect("parse refresh");
        let DatasetsCommands::Refresh(refresh) = parsed.command.expect("subcommand") else {
            panic!("expected refresh command");
        };
        assert_eq!(refresh.name.name(), Some("my-dataset"));
        assert_eq!(refresh.input.file, Some(PathBuf::from("records.jsonl")));
        assert!(refresh.prune);
        assert_eq!(refresh.input.id_field, "id");
    }

    #[test]
    fn view_parses_name_flag_web_and_verbose() {
        let parsed = parse(&[
            "datasets",
            "view",
            "--name",
            "my-dataset",
            "--web",
            "--verbose",
        ])
        .expect("parse view");
        let DatasetsCommands::View(view) = parsed.command.expect("subcommand") else {
            panic!("expected view command");
        };
        assert_eq!(view.name(), Some("my-dataset"));
        assert!(view.web);
        assert!(view.verbose);
    }

    #[test]
    fn delete_parses_name_and_force() {
        let parsed = parse(&["datasets", "delete", "my-dataset", "--force"]).expect("parse delete");
        let DatasetsCommands::Delete(delete) = parsed.command.expect("subcommand") else {
            panic!("expected delete command");
        };
        assert_eq!(delete.name(), Some("my-dataset"));
        assert!(delete.force);
    }

    #[test]
    fn dataset_name_rejects_positional_and_name_flag_together() {
        let err = parse(&[
            "datasets",
            "delete",
            "positional-name",
            "--name",
            "flag-name",
            "--force",
        ])
        .expect_err("name should be ambiguous when both positional and --name are set");
        let rendered = err.to_string();
        assert!(rendered.contains("cannot be used with"));
        assert!(rendered.contains("--name"));
    }
}
