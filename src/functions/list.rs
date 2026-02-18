use anyhow::Result;

use crate::{http::ApiClient, resource_cmd::print_named_resource_list, ui::with_spinner};

use super::api;

pub async fn run(client: &ApiClient, project: &str, org: &str, json: bool) -> Result<()> {
    let functions =
        with_spinner("Loading functions...", api::list_functions(client, project)).await?;

    if json {
        println!("{}", serde_json::to_string(&functions)?);
    } else {
        print_named_resource_list(&functions, "function", org, project, true)?;
    }

    Ok(())
}
