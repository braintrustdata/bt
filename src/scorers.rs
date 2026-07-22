use anyhow::Result;
use clap::{Args, Subcommand};

use crate::args::BaseArgs;
use crate::functions::{self, FunctionCommands, FunctionTypeFilter};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt scorers list
  bt scorers view my-scorer
  bt scorers create \"Helpfulness\" --model gpt-4o-mini --prompt-file judge.md \\
    --choice-scores '{\"A\":1,\"B\":0}' --use-cot
  bt scorers update my-scorer --prompt-file judge.md
  bt scorers delete my-scorer
")]
pub struct ScorersArgs {
    #[command(subcommand)]
    command: Option<ScorersCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ScorersCommands {
    /// Create an LLM scorer
    Create(functions::create::CreateArgs),
    #[command(flatten)]
    Function(FunctionCommands),
}

pub async fn run(base: BaseArgs, args: ScorersArgs) -> Result<()> {
    match args.command {
        Some(ScorersCommands::Create(create)) => functions::run_scorer_create(base, create).await,
        Some(ScorersCommands::Function(command)) => {
            functions::run_typed_command(base, Some(command), FunctionTypeFilter::Scorer).await
        }
        None => functions::run_typed_command(base, None, FunctionTypeFilter::Scorer).await,
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct ScorersArgsHarness {
        #[command(flatten)]
        args: ScorersArgs,
    }

    #[test]
    fn parses_create_scorer() {
        let parsed = ScorersArgsHarness::try_parse_from([
            "bt-scorers",
            "create",
            "Test scorer",
            "--model",
            "gpt-test",
            "--prompt",
            "Judge {{output}}",
            "--choice-scores",
            r#"{"yes":1,"no":0}"#,
            "--use-cot=false",
            "--if-exists",
            "replace",
        ])
        .expect("parse create");

        assert!(matches!(
            parsed.args.command,
            Some(ScorersCommands::Create(_))
        ));
    }

    #[test]
    fn still_parses_shared_scorer_commands() {
        let parsed = ScorersArgsHarness::try_parse_from([
            "bt-scorers",
            "update",
            "test-scorer",
            "--model",
            "gpt-test",
            "--yes",
        ])
        .expect("parse update");

        assert!(matches!(
            parsed.args.command,
            Some(ScorersCommands::Function(FunctionCommands::Update(_)))
        ));
    }
}
