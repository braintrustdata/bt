use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand};
use dialoguer::Input;

use crate::{
    args::BaseArgs,
    http::ApiClient,
    project_context::resolve_project_command_context_with_auth_mode,
    ui::{self, with_spinner},
};

pub(crate) mod api;
mod create;
mod delete;
mod list;
mod records;
mod update;
mod utils;
mod view;

use api::{self as datasets_api, Dataset};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;
const DEFAULT_DATASETS_VIEW_ROW_LIMIT: usize = 200;

#[derive(Debug, Clone, Args)]
struct DatasetNameArgs {
    /// Dataset name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Dataset name (flag)
    #[arg(long = "name", short = 'n')]
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
  bt datasets create my-dataset --description "Dataset for smoke tests"
  bt datasets create my-dataset --file records.jsonl
  cat records.jsonl | bt datasets create my-dataset
  bt datasets create my-dataset --rows '[{"id":"case-1","input":{"text":"hi"},"expected":"hello"}]'
  bt datasets update my-dataset --file records.jsonl
  bt datasets add my-dataset --rows '[{"id":"case-2","input":{"text":"bye"},"expected":"goodbye"}]'
  bt datasets refresh my-dataset --file records.jsonl --id-field metadata.case_id
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
    /// Upsert remote dataset rows by record id
    #[command(visible_aliases = ["add", "refresh"])]
    Update(UpdateArgs),
    /// View a dataset
    View(ViewArgs),
    /// Delete a dataset
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct CreateArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    /// Optional dataset description.
    #[arg(
        long,
        short = 'd',
        env = "BT_DATASETS_DESCRIPTION",
        value_name = "TEXT"
    )]
    description: Option<String>,

    #[command(flatten)]
    input: DatasetInputArgs,
}

impl CreateArgs {
    fn name(&self) -> Option<&str> {
        self.name.name()
    }
}

#[derive(Debug, Clone, Args)]
struct UpdateArgs {
    #[command(flatten)]
    name: DatasetNameArgs,

    #[command(flatten)]
    input: DatasetInputArgs,
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

    /// Maximum number of rows to load. Defaults to 200 unless --all-rows is passed.
    #[arg(long, env = "BT_DATASETS_VIEW_LIMIT", value_name = "N")]
    limit: Option<usize>,

    /// Load all rows (can be expensive for large datasets).
    #[arg(
        long = "all-rows",
        env = "BT_DATASETS_VIEW_ALL",
        value_parser = BoolishValueParser::new(),
        default_value_t = false,
        conflicts_with = "limit"
    )]
    all_rows: bool,
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

pub(crate) fn resolve_dataset_name(name: Option<&str>, command: &str) -> Result<String> {
    match name {
        Some(name) if !name.trim().is_empty() => Ok(name.trim().to_string()),
        _ => {
            if !ui::is_interactive() {
                bail!("dataset name required. Use: bt datasets {command} <name>");
            }
            Ok(Input::new().with_prompt("Dataset name").interact_text()?)
        }
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
    let read_only = datasets_command_is_read_only(args.command.as_ref());
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None | Some(DatasetsCommands::List) => list::run(&ctx, base.json).await,
        Some(DatasetsCommands::Create(create_args)) => {
            create::run(
                &ctx,
                create_args.name(),
                create_args.description.as_deref(),
                create_args.input.file.as_deref(),
                create_args.input.rows.as_deref(),
                &create_args.input.id_field,
                base.json,
            )
            .await
        }
        Some(DatasetsCommands::Update(update_args)) => {
            update::run(
                &ctx,
                update_args.name.name(),
                update_args.input.file.as_deref(),
                update_args.input.rows.as_deref(),
                &update_args.input.id_field,
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
                resolve_view_row_limit(&view_args),
            )
            .await
        }
        Some(DatasetsCommands::Delete(delete_args)) => {
            delete::run(&ctx, delete_args.name(), delete_args.force).await
        }
    }
}

fn datasets_command_is_read_only(command: Option<&DatasetsCommands>) -> bool {
    matches!(
        command,
        None | Some(DatasetsCommands::List) | Some(DatasetsCommands::View(_))
    )
}

fn resolve_view_row_limit(args: &ViewArgs) -> Option<usize> {
    if args.all_rows {
        None
    } else {
        Some(args.limit.unwrap_or(DEFAULT_DATASETS_VIEW_ROW_LIMIT))
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
            "--description",
            "Dataset for smoke tests",
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
        assert_eq!(
            create.description.as_deref(),
            Some("Dataset for smoke tests")
        );
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
    fn update_parses_file_and_id_field() {
        let parsed = parse(&[
            "datasets",
            "update",
            "my-dataset",
            "--file",
            "records.jsonl",
            "--id-field",
            "metadata.case_id",
        ])
        .expect("parse update");
        let DatasetsCommands::Update(update) = parsed.command.expect("subcommand") else {
            panic!("expected update command");
        };
        assert_eq!(update.name.name(), Some("my-dataset"));
        assert_eq!(update.input.file, Some(PathBuf::from("records.jsonl")));
        assert_eq!(update.input.id_field, "metadata.case_id");
    }

    #[test]
    fn update_visible_aliases_parse() {
        for alias in ["add", "refresh"] {
            let parsed = parse(&[
                "datasets",
                alias,
                "my-dataset",
                "--rows",
                r#"[{"id":"case-1"}]"#,
            ])
            .unwrap_or_else(|err| panic!("parse {alias} alias: {err}"));
            let DatasetsCommands::Update(update) = parsed.command.expect("subcommand") else {
                panic!("expected update command");
            };
            assert_eq!(update.name.name(), Some("my-dataset"));
            assert_eq!(update.input.rows.as_deref(), Some(r#"[{"id":"case-1"}]"#));
        }
    }

    #[test]
    fn refresh_alias_parses_file_and_default_id_field() {
        let parsed = parse(&[
            "datasets",
            "refresh",
            "my-dataset",
            "--file",
            "records.jsonl",
        ])
        .expect("parse refresh");
        let DatasetsCommands::Update(update) = parsed.command.expect("subcommand") else {
            panic!("expected update command");
        };
        assert_eq!(update.name.name(), Some("my-dataset"));
        assert_eq!(update.input.file, Some(PathBuf::from("records.jsonl")));
        assert_eq!(update.input.id_field, "id");
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
            "--limit",
            "25",
        ])
        .expect("parse view");
        let DatasetsCommands::View(view) = parsed.command.expect("subcommand") else {
            panic!("expected view command");
        };
        assert_eq!(view.name(), Some("my-dataset"));
        assert!(view.web);
        assert!(view.verbose);
        assert_eq!(view.limit, Some(25));
        assert!(!view.all_rows);
    }

    #[test]
    fn view_parses_all_rows() {
        let parsed = parse(&["datasets", "view", "my-dataset", "--all-rows"]).expect("parse view");
        let DatasetsCommands::View(view) = parsed.command.expect("subcommand") else {
            panic!("expected view command");
        };
        assert_eq!(view.name(), Some("my-dataset"));
        assert!(view.all_rows);
        assert!(view.limit.is_none());
    }

    #[test]
    fn view_limit_defaults_and_all_rows_override() {
        let default_args = ViewArgs {
            name: DatasetNameArgs {
                name_positional: Some("dataset".to_string()),
                name_flag: None,
            },
            web: false,
            verbose: false,
            limit: None,
            all_rows: false,
        };
        assert_eq!(
            resolve_view_row_limit(&default_args),
            Some(DEFAULT_DATASETS_VIEW_ROW_LIMIT)
        );

        let all_rows_args = ViewArgs {
            all_rows: true,
            ..default_args
        };
        assert_eq!(resolve_view_row_limit(&all_rows_args), None);
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
    fn dataset_name_positional_takes_precedence_over_flag() {
        let parsed = parse(&[
            "datasets",
            "delete",
            "positional-name",
            "--name",
            "flag-name",
            "--force",
        ])
        .expect("both positional and --name should parse");
        let DatasetsCommands::Delete(delete) = parsed.command.expect("subcommand") else {
            panic!("expected delete command");
        };
        assert_eq!(delete.name(), Some("positional-name"));
    }

    #[test]
    fn datasets_routes_list_and_view_to_read_only_auth() {
        assert!(datasets_command_is_read_only(None));
        assert!(datasets_command_is_read_only(Some(&DatasetsCommands::List)));
        assert!(datasets_command_is_read_only(Some(
            &DatasetsCommands::View(ViewArgs {
                name: DatasetNameArgs {
                    name_positional: Some("dataset".to_string()),
                    name_flag: None,
                },
                web: false,
                verbose: false,
                limit: None,
                all_rows: false,
            })
        )));
    }

    #[test]
    fn datasets_routes_write_commands_to_validated_auth() {
        assert!(!datasets_command_is_read_only(Some(
            &DatasetsCommands::Create(CreateArgs {
                name: DatasetNameArgs {
                    name_positional: Some("dataset".to_string()),
                    name_flag: None,
                },
                description: None,
                input: DatasetInputArgs {
                    file: None,
                    rows: Some("[]".to_string()),
                    id_field: "id".to_string(),
                },
            })
        )));
        assert!(!datasets_command_is_read_only(Some(
            &DatasetsCommands::Update(UpdateArgs {
                name: DatasetNameArgs {
                    name_positional: Some("dataset".to_string()),
                    name_flag: None,
                },
                input: DatasetInputArgs {
                    file: None,
                    rows: Some("[]".to_string()),
                    id_field: "id".to_string(),
                },
            })
        )));
        assert!(!datasets_command_is_read_only(Some(
            &DatasetsCommands::Delete(DeleteArgs {
                name: DatasetNameArgs {
                    name_positional: Some("dataset".to_string()),
                    name_flag: None,
                },
                force: true,
            })
        )));
    }
}
