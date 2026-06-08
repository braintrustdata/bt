use anyhow::{bail, Result};
use clap::{Args, Subcommand};
use std::path::PathBuf;

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

pub(crate) mod api;
mod btmap;
mod config;
mod formatting;
mod open;
mod poke;
mod report;
mod rewind;
mod status;

pub(crate) type ResolvedContext = crate::project_context::ProjectContext;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt topics
  bt topics status
  bt topics status --full
  bt topics status --watch
  bt topics config
  bt topics config <automation-or-topic-map-id>
  bt topics config enable
  bt topics config delete
  bt topics config set --topic-window 1h --generation-cadence 1d
  bt topics config topic-map <topic-map-id>
  bt topics config topic-map set Task --embedding-model brain-embedding-1
  bt topics report fn_123
  bt topics report fn_123 --version 0000000000000001
  bt topics btmap fn_123
  bt topics btmap fn_123 --output topic-map.btmap
  bt topics poke
  bt topics rewind 7d
  bt topics open
")]
pub struct TopicsArgs {
    #[command(subcommand)]
    command: Option<TopicsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum TopicsCommands {
    /// Show Topics automation status for the active project
    Status(StatusArgs),
    /// View or edit Topics automation config
    Config(Box<ConfigArgs>),
    /// Queue Topics to run on the next executor pass
    Poke,
    /// Rewind recent Topics history and queue it to reprocess
    Rewind(RewindArgs),
    /// Download a saved topic map report JSON file
    Report(ReportArgs),
    /// Download the raw topic map (.btmap) artifact
    Btmap(BtmapArgs),
    /// Open the Topics page in the browser
    Open,
}

#[derive(Debug, Clone, Args)]
struct StatusArgs {
    /// Show expanded diagnostics and progress counts
    #[arg(long)]
    full: bool,

    /// Window for status progress counts, for example 1h or 7d
    #[arg(
        long = "progress-window",
        env = "BT_TOPICS_STATUS_PROGRESS_WINDOW",
        value_name = "WINDOW"
    )]
    progress_window: Option<String>,

    /// Refresh every 2 seconds until interrupted
    #[arg(long)]
    watch: bool,
}

#[derive(Debug, Clone, Args)]
struct ConfigArgs {
    /// Specific automation ID to show
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Automation ID/name or topic map name/function ID to show
    #[arg(value_name = "TARGET")]
    target: Option<String>,

    #[command(subcommand)]
    command: Option<ConfigCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ConfigCommands {
    /// Enable Topics for this project with the provided config
    Enable(ConfigEnableArgs),
    /// Delete the Topics automation from this project
    Delete(ConfigDeleteArgs),
    /// Update editable Topics config fields
    Set(ConfigSetArgs),
    /// View or edit per-topic-map settings
    #[command(name = "topic-map")]
    TopicMap(TopicMapArgs),
}

#[derive(Debug, Clone, Args)]
struct TopicsConfigFieldsArgs {
    /// Human-friendly automation name
    #[arg(long)]
    name: Option<String>,

    /// Human-friendly automation description
    #[arg(long)]
    description: Option<String>,

    /// Topic window duration, for example 1h or 1d
    #[arg(long = "topic-window", alias = "window")]
    window: Option<String>,

    /// How often Topics should try to generate fresh topic maps, for example 1h or 1d
    #[arg(long = "generation-cadence", alias = "cadence")]
    cadence: Option<String>,

    /// Relabel overlap duration, for example 1h
    #[arg(long = "relabel-overlap")]
    relabel_overlap: Option<String>,

    /// Trace idle wait duration, for example 30s
    #[arg(long = "idle-time", alias = "idle")]
    idle: Option<String>,

    /// Percent of matching traces to sample, for example 25 or 25%
    #[arg(long = "sampling-rate")]
    sampling_rate: Option<String>,

    /// BTQL filter used to select which traces get facets and topics
    #[arg(long, conflicts_with = "clear_filter")]
    filter: Option<String>,

    /// Clear the top-level BTQL filter
    #[arg(long, conflicts_with = "filter")]
    clear_filter: bool,
}

#[derive(Debug, Clone, Args)]
struct ConfigEnableArgs {
    #[command(flatten)]
    fields: TopicsConfigFieldsArgs,

    /// Facet labels to enable. Reuse the built-in defaults by omitting this flag.
    #[arg(long = "facet")]
    facets: Vec<String>,

    /// Embedding model used for new topic maps
    #[arg(long = "embedding-model")]
    embedding_model: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct ConfigSetArgs {
    /// Specific automation ID to update
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    #[command(flatten)]
    fields: TopicsConfigFieldsArgs,
}

#[derive(Debug, Clone, Args)]
struct ConfigDeleteArgs {
    /// Specific automation ID to delete
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct TopicMapArgs {
    /// Topic map name or function ID to show
    #[arg(value_name = "TOPIC_MAP")]
    topic_map: Option<String>,

    /// Specific automation ID to search within
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    #[command(subcommand)]
    command: Option<TopicMapCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum TopicMapCommands {
    /// Show a configured Topics topic map by name or function ID
    #[command(alias = "view")]
    Show(TopicMapViewArgs),
    /// Update a configured Topics topic map by name or function ID
    Set(Box<TopicMapSetArgs>),
}

#[derive(Debug, Clone, Args)]
struct TopicMapViewArgs {
    /// Specific automation ID to search within
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Topic map name or function ID
    topic_map: String,
}

#[derive(Debug, Clone, Args)]
struct TopicMapSetArgs {
    /// Specific automation ID to search within
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Topic map name or function ID
    topic_map: String,

    /// Human-friendly topic map name
    #[arg(long)]
    name: Option<String>,

    /// Human-friendly topic map description
    #[arg(long)]
    description: Option<String>,

    /// Facet field this topic map clusters
    #[arg(long = "source-facet")]
    source_facet: Option<String>,

    /// Embedding model used for clustering
    #[arg(long = "embedding-model")]
    embedding_model: Option<String>,

    /// Maximum centroid distance before returning no_match
    #[arg(long = "distance-threshold")]
    distance_threshold: Option<f64>,

    /// Whether to disable reconciliation against the previously saved report
    #[arg(long = "disable-reconciliation")]
    disable_reconciliation: Option<bool>,

    /// Clustering algorithm to use when generating topics
    #[arg(long, value_parser = ["hdbscan", "kmeans"])]
    algorithm: Option<String>,

    /// Dimension reduction step to use before clustering
    #[arg(long = "dimension-reduction", value_parser = ["umap", "pca", "none"])]
    dimension_reduction: Option<String>,

    /// Maximum number of rows sampled during topic-map generation
    #[arg(long = "sample-size")]
    sample_size: Option<u32>,

    /// Number of clusters when using kmeans
    #[arg(long = "n-clusters")]
    n_clusters: Option<u32>,

    /// Minimum cluster size when using hdbscan
    #[arg(long = "min-cluster-size")]
    min_cluster_size: Option<usize>,

    /// Minimum samples when using hdbscan
    #[arg(long = "min-samples")]
    min_samples: Option<usize>,

    /// Hierarchy threshold used when naming hierarchical clusters
    #[arg(long = "hierarchy-threshold")]
    hierarchy_threshold: Option<usize>,

    /// LLM model used to name generated topics
    #[arg(long = "naming-model")]
    naming_model: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct RewindArgs {
    /// Specific automation ID to rewind
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Topic window to reprocess, for example 1h or 7d
    topic_window: String,
}

#[derive(Debug, Clone, Args)]
struct ReportArgs {
    /// Topic map function ID
    #[arg(value_name = "FUNCTION_ID")]
    function_id_positional: Option<String>,

    /// Topic map function ID
    #[arg(long = "id", env = "BT_TOPICS_REPORT_FUNCTION_ID")]
    id: Option<String>,

    /// Specific topic map version/xact ID
    #[arg(long, env = "BT_TOPICS_REPORT_VERSION")]
    version: Option<String>,

    /// Output file path. Omit to write the report JSON to stdout.
    #[arg(long, env = "BT_TOPICS_REPORT_OUTPUT")]
    output: Option<PathBuf>,
}

impl ReportArgs {
    fn function_id(&self) -> Result<&str> {
        match (self.function_id_positional.as_deref(), self.id.as_deref()) {
            (Some(_), Some(_)) => {
                anyhow::bail!("use either --id or a positional function id, not both")
            }
            (Some(id), None) | (None, Some(id)) => Ok(id),
            (None, None) => {
                anyhow::bail!("topic map function id required. Use: bt topics report <function-id>")
            }
        }
    }
}

#[derive(Debug, Clone, Args)]
struct BtmapArgs {
    /// Topic map function ID
    #[arg(value_name = "FUNCTION_ID")]
    function_id_positional: Option<String>,

    /// Topic map function ID
    #[arg(long = "id", env = "BT_TOPICS_BTMAP_FUNCTION_ID")]
    id: Option<String>,

    /// Specific topic map version/xact ID
    #[arg(long, env = "BT_TOPICS_BTMAP_VERSION")]
    version: Option<String>,

    /// Output file path. Omit to write the .btmap bytes to stdout.
    #[arg(long, env = "BT_TOPICS_BTMAP_OUTPUT")]
    output: Option<PathBuf>,
}

impl BtmapArgs {
    fn function_id(&self) -> Result<&str> {
        match (self.function_id_positional.as_deref(), self.id.as_deref()) {
            (Some(_), Some(_)) => {
                anyhow::bail!("use either --id or a positional function id, not both")
            }
            (Some(id), None) | (None, Some(id)) => Ok(id),
            (None, None) => {
                anyhow::bail!("topic map function id required. Use: bt topics btmap <function-id>")
            }
        }
    }
}

pub async fn run(base: BaseArgs, args: TopicsArgs) -> Result<()> {
    if let Some(TopicsCommands::Report(report_args)) = args.command.as_ref() {
        return report::run(&base, report_args, base.json).await;
    }

    if let Some(TopicsCommands::Btmap(btmap_args)) = args.command.as_ref() {
        return btmap::run(&base, btmap_args, base.json).await;
    }

    let read_only = match args.command.as_ref() {
        None | Some(TopicsCommands::Status(_)) | Some(TopicsCommands::Open) => true,
        Some(TopicsCommands::Config(config_args)) => match config_args.command.as_ref() {
            None => true,
            Some(ConfigCommands::TopicMap(topic_map_args)) => {
                matches!(
                    topic_map_args.command,
                    None | Some(TopicMapCommands::Show(_))
                )
            }
            Some(ConfigCommands::Enable(_))
            | Some(ConfigCommands::Delete(_))
            | Some(ConfigCommands::Set(_)) => false,
        },
        Some(TopicsCommands::Poke) | Some(TopicsCommands::Rewind(_)) => false,
        Some(TopicsCommands::Report(_)) | Some(TopicsCommands::Btmap(_)) => {
            unreachable!("handled before project resolution")
        }
    };
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None => {
            status::run(
                &ctx,
                StatusArgs {
                    full: false,
                    progress_window: None,
                    watch: false,
                },
                base.json,
            )
            .await
        }
        Some(TopicsCommands::Status(status_args)) => {
            status::run(&ctx, status_args, base.json).await
        }
        Some(TopicsCommands::Config(config_args)) => {
            let parent_automation_id = config_args.automation_id;
            let target = config_args.target;
            match config_args.command {
                None => match target {
                    Some(target) => {
                        config::run_view_target(
                            &ctx,
                            parent_automation_id.as_deref(),
                            &target,
                            base.json,
                        )
                        .await
                    }
                    None => {
                        config::run_view(&ctx, parent_automation_id.as_deref(), base.json).await
                    }
                },
                Some(ConfigCommands::Enable(enable_args)) => {
                    config::run_enable(&ctx, &enable_args, base.json).await
                }
                Some(ConfigCommands::Delete(delete_args)) => {
                    config::run_delete(
                        &ctx,
                        delete_args
                            .automation_id
                            .or(parent_automation_id)
                            .as_deref(),
                        delete_args.force,
                        base.json,
                    )
                    .await
                }
                Some(ConfigCommands::Set(mut set_args)) => {
                    set_args.automation_id = set_args.automation_id.or(parent_automation_id);
                    config::run_set(&ctx, &set_args, base.json).await
                }
                Some(ConfigCommands::TopicMap(topic_map_args)) => {
                    let topic_map_automation_id =
                        topic_map_args.automation_id.or(parent_automation_id);
                    match topic_map_args.command {
                        None => {
                            let Some(topic_map) = topic_map_args.topic_map else {
                                bail!(
                                    "topic map name or function ID is required; try `bt topics config topic-map <topic-map-id>`"
                                );
                            };
                            config::run_topic_map_view(
                                &ctx,
                                topic_map_automation_id.as_deref(),
                                &topic_map,
                                base.json,
                            )
                            .await
                        }
                        Some(TopicMapCommands::Show(view_args)) => {
                            config::run_topic_map_view(
                                &ctx,
                                view_args
                                    .automation_id
                                    .or(topic_map_automation_id)
                                    .as_deref(),
                                &view_args.topic_map,
                                base.json,
                            )
                            .await
                        }
                        Some(TopicMapCommands::Set(mut set_args)) => {
                            set_args.automation_id =
                                set_args.automation_id.or(topic_map_automation_id);
                            config::run_topic_map_set(&ctx, &set_args, base.json).await
                        }
                    }
                }
            }
        }
        Some(TopicsCommands::Poke) => poke::run(&ctx, base.json).await,
        Some(TopicsCommands::Rewind(rewind_args)) => {
            rewind::run(&ctx, &rewind_args, base.json).await
        }
        Some(TopicsCommands::Report(_)) | Some(TopicsCommands::Btmap(_)) => {
            unreachable!("handled before project resolution")
        }
        Some(TopicsCommands::Open) => open::run(&ctx).await,
    }
}

#[cfg(test)]
mod tests {
    use clap::{Parser, Subcommand};

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(subcommand)]
        command: Commands,
    }

    #[derive(Debug, Subcommand)]
    enum Commands {
        Topics(TopicsArgs),
    }

    fn parse(args: &[&str]) -> anyhow::Result<TopicsArgs> {
        let mut argv = vec!["bt"];
        argv.extend_from_slice(args);
        let parsed = CliHarness::try_parse_from(argv)?;
        match parsed.command {
            Commands::Topics(args) => Ok(args),
        }
    }

    fn topics_command_is_read_only(command: Option<&TopicsCommands>) -> bool {
        match command {
            None | Some(TopicsCommands::Status(_)) | Some(TopicsCommands::Open) => true,
            Some(TopicsCommands::Config(config_args)) => match config_args.command.as_ref() {
                None => true,
                Some(ConfigCommands::TopicMap(topic_map_args)) => {
                    matches!(
                        topic_map_args.command,
                        None | Some(TopicMapCommands::Show(_))
                    )
                }
                Some(ConfigCommands::Enable(_))
                | Some(ConfigCommands::Delete(_))
                | Some(ConfigCommands::Set(_)) => false,
            },
            Some(TopicsCommands::Poke) | Some(TopicsCommands::Rewind(_)) => false,
            Some(TopicsCommands::Report(_)) | Some(TopicsCommands::Btmap(_)) => true,
        }
    }

    #[test]
    fn topics_commands_use_read_only_auth() {
        let parsed = parse(&["topics"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "status"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "status", "--full", "--watch"]).expect("parse");
        let Some(TopicsCommands::Status(status)) = parsed.command.as_ref() else {
            panic!("expected status command");
        };
        assert!(status.full);
        assert!(status.watch);
        assert_eq!(status.progress_window, None);

        let parsed = parse(&["topics", "status", "--progress-window", "7d"]).expect("parse");
        let Some(TopicsCommands::Status(status)) = parsed.command.as_ref() else {
            panic!("expected status command");
        };
        assert_eq!(status.progress_window.as_deref(), Some("7d"));
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "open"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "report", "fn_123"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_poke_uses_validated_auth() {
        let parsed = parse(&["topics", "poke"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_rewind_uses_validated_auth() {
        let parsed = parse(&["topics", "rewind", "7d"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_report_parses_function_id_version_and_output() {
        let parsed = parse(&[
            "topics",
            "report",
            "--id",
            "fn_123",
            "--version",
            "0000000000000001",
            "--output",
            "report.json",
        ])
        .expect("parse");

        let Some(TopicsCommands::Report(args)) = parsed.command.as_ref() else {
            panic!("expected report command");
        };
        assert_eq!(args.function_id().expect("function id"), "fn_123");
        assert_eq!(args.version.as_deref(), Some("0000000000000001"));
        assert_eq!(
            args.output.as_deref(),
            Some(std::path::Path::new("report.json"))
        );
        assert!(topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_report_accepts_id_flag_without_output() {
        let parsed = parse(&["topics", "report", "--id", "fn_test_topic_map"]).expect("parse");

        let Some(TopicsCommands::Report(args)) = parsed.command.as_ref() else {
            panic!("expected report command");
        };
        assert_eq!(
            args.function_id().expect("function id"),
            "fn_test_topic_map"
        );
        assert_eq!(args.output, None);
    }

    #[test]
    fn topics_config_view_uses_read_only_auth() {
        let parsed = parse(&["topics", "config"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "config", "func_1"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected config command");
        };
        assert_eq!(config_args.target.as_deref(), Some("func_1"));
    }

    #[test]
    fn topics_config_topic_map_view_uses_read_only_auth() {
        let parsed = parse(&["topics", "config", "topic-map", "func_1"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected config command");
        };
        let Some(ConfigCommands::TopicMap(topic_map_args)) = config_args.command.as_ref() else {
            panic!("expected topic-map command");
        };
        assert_eq!(topic_map_args.topic_map.as_deref(), Some("func_1"));
        assert!(topic_map_args.command.is_none());

        let parsed = parse(&["topics", "config", "topic-map", "show", "func_1"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_set_uses_validated_auth() {
        let parsed = parse(&["topics", "config", "set", "--topic-window", "1h"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_enable_uses_validated_auth() {
        let parsed = parse(&["topics", "config", "enable"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_delete_uses_validated_auth() {
        let parsed = parse(&["topics", "config", "delete"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_topic_map_set_uses_validated_auth() {
        let parsed = parse(&[
            "topics",
            "config",
            "topic-map",
            "set",
            "Task",
            "--embedding-model",
            "brain-embedding-1",
        ])
        .expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_set_accepts_legacy_flag_aliases() {
        let parsed = parse(&[
            "topics",
            "config",
            "set",
            "--window",
            "1h",
            "--cadence",
            "1d",
            "--idle",
            "30s",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected config set command");
        };
        let Some(ConfigCommands::Set(set_args)) = config_args.command.as_ref() else {
            panic!("expected config set command");
        };

        assert_eq!(set_args.fields.window.as_deref(), Some("1h"));
        assert_eq!(set_args.fields.cadence.as_deref(), Some("1d"));
        assert_eq!(set_args.fields.idle.as_deref(), Some("30s"));
    }

    #[test]
    fn topics_config_enable_accepts_shared_flags() {
        let parsed = parse(&[
            "topics",
            "config",
            "enable",
            "--topic-window",
            "6h",
            "--generation-cadence",
            "1d",
            "--sampling-rate",
            "25%",
            "--facet",
            "Task",
            "--facet",
            "Issues",
            "--embedding-model",
            "brain-embedding-1",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected config enable command");
        };
        let Some(ConfigCommands::Enable(enable_args)) = config_args.command.as_ref() else {
            panic!("expected config enable command");
        };

        assert_eq!(enable_args.fields.window.as_deref(), Some("6h"));
        assert_eq!(enable_args.fields.cadence.as_deref(), Some("1d"));
        assert_eq!(enable_args.fields.sampling_rate.as_deref(), Some("25%"));
        assert_eq!(enable_args.facets, vec!["Task", "Issues"]);
        assert_eq!(
            enable_args.embedding_model.as_deref(),
            Some("brain-embedding-1")
        );
    }

    #[test]
    fn topics_config_delete_accepts_automation_id_and_force() {
        let parsed = parse(&[
            "topics",
            "config",
            "delete",
            "--automation-id",
            "auto_123",
            "--force",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected config delete command");
        };
        let Some(ConfigCommands::Delete(delete_args)) = config_args.command.as_ref() else {
            panic!("expected config delete command");
        };

        assert_eq!(delete_args.automation_id.as_deref(), Some("auto_123"));
        assert!(delete_args.force);
    }

    #[test]
    fn topics_rewind_uses_positional_window() {
        let parsed = parse(&["topics", "rewind", "7d"]).expect("parse");
        let Some(TopicsCommands::Rewind(rewind_args)) = parsed.command.as_ref() else {
            panic!("expected rewind command");
        };
        assert_eq!(rewind_args.topic_window.as_str(), "7d");
    }

    #[test]
    fn topics_config_topic_map_set_parses_generation_settings() {
        let parsed = parse(&[
            "topics",
            "config",
            "topic-map",
            "set",
            "Task",
            "--embedding-model",
            "brain-embedding-1",
            "--disable-reconciliation",
            "true",
            "--naming-model",
            "brain-agent-1",
            "--algorithm",
            "hdbscan",
            "--dimension-reduction",
            "umap",
            "--min-cluster-size",
            "25",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(config_args)) = parsed.command.as_ref() else {
            panic!("expected topic-map set command");
        };
        let Some(ConfigCommands::TopicMap(topic_map_args)) = config_args.command.as_ref() else {
            panic!("expected topic-map set command");
        };
        let Some(TopicMapCommands::Set(set_args)) = &topic_map_args.command else {
            panic!("expected topic-map set command");
        };

        assert_eq!(set_args.topic_map, "Task");
        assert_eq!(
            set_args.embedding_model.as_deref(),
            Some("brain-embedding-1")
        );
        assert_eq!(set_args.disable_reconciliation, Some(true));
        assert_eq!(set_args.naming_model.as_deref(), Some("brain-agent-1"));
        assert_eq!(set_args.algorithm.as_deref(), Some("hdbscan"));
        assert_eq!(set_args.dimension_reduction.as_deref(), Some("umap"));
        assert_eq!(set_args.min_cluster_size, Some(25));
    }

    #[test]
    fn parent_automation_id_parsed_separately_from_delete_child() {
        // `config --automation-id X delete` puts X on the parent, not the child.
        // The dispatch merges them with child.or(parent).
        let parsed =
            parse(&["topics", "config", "--automation-id", "parent_id", "delete"]).expect("parse");

        let Some(TopicsCommands::Config(config_args)) = &parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(config_args.automation_id.as_deref(), Some("parent_id"));

        let Some(ConfigCommands::Delete(delete_args)) = &config_args.command else {
            panic!("expected delete subcommand");
        };
        assert_eq!(delete_args.automation_id, None);
    }

    #[test]
    fn parent_automation_id_parsed_separately_from_set_child() {
        let parsed = parse(&[
            "topics",
            "config",
            "--automation-id",
            "parent_id",
            "set",
            "--topic-window",
            "1h",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(config_args)) = &parsed.command else {
            panic!("expected config command");
        };
        assert_eq!(config_args.automation_id.as_deref(), Some("parent_id"));

        let Some(ConfigCommands::Set(set_args)) = &config_args.command else {
            panic!("expected set subcommand");
        };
        assert_eq!(set_args.automation_id, None);
    }
}
