use anyhow::Result;
use std::process;

use crate::args::BaseArgs;

pub fn run(base: BaseArgs, key: &str, global: bool, local: bool) -> Result<()> {
    let cfg = if global {
        super::load_global()?
    } else if local {
        match super::local_path() {
            Some(p) => super::load_file(&p),
            None => super::Config::default(),
        }
    } else {
        super::load()?
    };

    match cfg.get_field(key) {
        Some(value) => {
            if base.json {
                println!("{}", serde_json::to_string(value)?);
            } else {
                println!("{value}");
            }
            Ok(())
        }
        None => {
            process::exit(1);
        }
    }
}
