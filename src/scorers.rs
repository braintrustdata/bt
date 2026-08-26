use anyhow::Result;
use clap::{Args, Subcommand};

use crate::args::BaseArgs;
use crate::functions::{self, FunctionCommands, FunctionTypeFilter};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt scorers list
  bt scorers view my-scorer
  bt scorers create \"Helpfulness\" --model gpt-5.4-nano --messages @messages.json \\
    --choice-scores '{\"A\":1,\"B\":0}'
  bt scorers delete my-scorer

TypeScript and Python code scorers:
  TypeScript: projects.create({ name: \"test-project\" }).scorers.create({...})
              bt functions push scorer.ts
  Python:     projects.create(\"test-project\").scorers.create(...)
              bt functions push scorer.py
")]
pub struct ScorersArgs {
    #[command(subcommand)]
    command: Option<ScorersCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ScorersCommands {
    /// Create an LLM scorer or classifier
    Create(Box<functions::create::CreateArgs>),
    #[command(flatten)]
    Function(FunctionCommands),
}

pub async fn run(base: BaseArgs, args: ScorersArgs) -> Result<()> {
    match args.command {
        Some(ScorersCommands::Create(create)) => functions::run_scorer_create(base, *create).await,
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
    use crate::args::CLIArgs;

    #[derive(Debug, Parser)]
    struct ScorersArgsHarness {
        #[command(flatten)]
        args: ScorersArgs,
    }

    #[test]
    fn invoke_accepts_global_json_flag() {
        #[derive(Debug, Parser)]
        struct Harness {
            #[command(flatten)]
            command: CLIArgs<ScorersArgs>,
        }

        let parsed = Harness::try_parse_from(["bt-scorers", "invoke", "test-scorer", "--json"])
            .expect("parse scorer invoke with global JSON output");

        assert!(parsed.command.base.json);
        assert!(matches!(
            parsed.command.args.command,
            Some(ScorersCommands::Function(FunctionCommands::Invoke(_)))
        ));
    }

    #[test]
    fn parses_create_scorer() {
        let parsed = ScorersArgsHarness::try_parse_from([
            "bt-scorers",
            "create",
            "Test scorer",
            "--model",
            "gpt-test",
            "--messages",
            r#"[{"role":"user","content":"Judge {{output}}"}]"#,
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
}
