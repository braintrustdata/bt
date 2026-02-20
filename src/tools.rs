use anyhow::Result;

use crate::args::BaseArgs;
use crate::functions::{self, FunctionArgs, TOOL};

pub type ToolsArgs = FunctionArgs;

pub async fn run(base: BaseArgs, args: ToolsArgs) -> Result<()> {
    functions::run(base, args, &TOOL).await
}
