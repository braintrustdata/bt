use anyhow::Result;
use dialoguer::console;
use reqwest::Client;

use crate::login::LoginContext;
use crate::ui::with_spinner;

use super::api;

pub async fn run(http: &Client, ctx: &LoginContext, json: bool) -> Result<()> {
    let projects = with_spinner("Loading projects...", api::list_projects(http, ctx)).await?;

    if json {
        println!("{}", serde_json::to_string(&projects)?);
    } else {
        println!("{}", console::style("Projects").bold());
        for project in projects {
            println!("{}", project.name);
        }
    }

    Ok(())
}
