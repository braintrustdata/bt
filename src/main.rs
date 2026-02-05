use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "bt", about = "Braintrust CLI", version)]
struct Cli {
    /// Output as JSON
    #[arg(short = 'j', long)]
    json: bool,

    /// Override active project
    #[arg(short = 'p', long)]
    project: Option<String>,

    /// Override stored API key (or via BRAINTRUST_API_KEY)
    #[arg(long, env = "BRAINTRUST_API_KEY")]
    api_key: Option<String>,
}

fn main() {
    let _cli = Cli::parse();
}
