use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::ui::{
    apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner,
};
use crate::utils::pluralize;

use super::{api, label, label_plural, FunctionTypeFilter, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, json: bool, ft: Option<FunctionTypeFilter>) -> Result<()> {
    let function_type = ft.map(|f| f.as_str());
    let functions = with_spinner(
        &format!("Loading {}...", label_plural(ft)),
        api::list_functions(&ctx.client, &ctx.project.id, function_type),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&functions)?);
        return Ok(());
    }

    let mut output = String::new();
    let count = format!(
        "{} {}",
        functions.len(),
        pluralize(functions.len(), label(ft), Some(label_plural(ft)))
    );
    writeln!(
        output,
        "{} found in {} {} {}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(&ctx.project.name).bold()
    )?;

    let mut table = styled_table();
    if ft.is_none() {
        table.set_header(vec![
            header("Name"),
            header("Type"),
            header("Description"),
            header("Slug"),
        ]);
    } else {
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);
    }
    apply_column_padding(&mut table, (0, 6));

    for func in &functions {
        let desc = func
            .description
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, 60))
            .unwrap_or_else(|| "-".to_string());
        if ft.is_none() {
            let t = func.function_type.as_deref().unwrap_or("-");
            table.add_row(vec![&func.name, t, &desc, &func.slug]);
        } else {
            table.add_row(vec![&func.name, &desc, &func.slug]);
        }
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
