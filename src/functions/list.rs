use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    http::ApiClient,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::{api, FunctionKind};

pub async fn run(
    client: &ApiClient,
    project_id: &str,
    org: &str,
    json: bool,
    kind: &FunctionKind,
) -> Result<()> {
    let functions = with_spinner(
        &format!("Loading {}...", kind.plural),
        api::list_functions(client, project_id, Some(kind.function_type)),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&functions)?);
    } else {
        let mut output = String::new();
        let count = format!(
            "{} {}",
            functions.len(),
            pluralize(functions.len(), kind.type_name, Some(kind.plural))
        );
        writeln!(
            output,
            "{} found in {} {} {}\n",
            console::style(count),
            console::style(org).bold(),
            console::style("/").dim().bold(),
            console::style(project_id).bold()
        )?;

        let mut table = styled_table();
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);
        apply_column_padding(&mut table, (0, 6));

        for func in &functions {
            let desc = func
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| truncate(s, 60))
                .unwrap_or_else(|| "-".to_string());
            table.add_row(vec![&func.name, &desc, &func.slug]);
        }

        write!(output, "{table}")?;
        print_with_pager(&output)?;
    }
    Ok(())
}
