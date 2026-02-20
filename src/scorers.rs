use anyhow::Result;

use crate::args::BaseArgs;
use crate::functions::{self, FunctionArgs, SCORER};

pub type ScorersArgs = FunctionArgs;

pub async fn run(base: BaseArgs, args: ScorersArgs) -> Result<()> {
    functions::run(base, args, &SCORER).await
}
