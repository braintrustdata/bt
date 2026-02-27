use anyhow::Result;

use crate::args::BaseArgs;
use crate::functions::{self, FunctionArgs, FunctionTypeFilter};

pub type ScorersArgs = FunctionArgs;

pub async fn run(base: BaseArgs, args: ScorersArgs) -> Result<()> {
    functions::run_typed(base, args, FunctionTypeFilter::Scorer).await
}
