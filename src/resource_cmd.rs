use std::fmt::Write as _;

use anyhow::{bail, Result};
use dialoguer::console;

use crate::{
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate},
    utils::pluralize,
};

pub trait NamedResource: Clone {
    fn name(&self) -> &str;
    fn description(&self) -> Option<&str>;
    fn slug(&self) -> &str;
    fn resource_type(&self) -> Option<String> {
        None
    }
}

pub fn print_named_resource_list<T: NamedResource>(
    resources: &[T],
    singular: &str,
    org: &str,
    project: &str,
    include_type_column: bool,
) -> Result<()> {
    let mut output = String::new();

    let count = format!(
        "{} {}",
        resources.len(),
        pluralize(resources.len(), singular, None)
    );
    writeln!(
        output,
        "{} found in {} {} {}\n",
        console::style(count),
        console::style(org).bold(),
        console::style("/").dim().bold(),
        console::style(project).bold()
    )?;

    let mut table = styled_table();
    if include_type_column {
        table.set_header(vec![
            header("Name"),
            header("Description"),
            header("Type"),
            header("Slug"),
        ]);
    } else {
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);
    }
    apply_column_padding(&mut table, (0, 6));

    for resource in resources {
        let desc = resource
            .description()
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, 60))
            .unwrap_or_else(|| "-".to_string());

        if include_type_column {
            let resource_type = resource.resource_type().unwrap_or_else(|| "-".to_string());
            table.add_row(vec![
                resource.name(),
                &desc,
                &resource_type,
                resource.slug(),
            ]);
        } else {
            table.add_row(vec![resource.name(), &desc, resource.slug()]);
        }
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;

    Ok(())
}

pub fn select_named_resource_interactive<T: NamedResource>(
    mut resources: Vec<T>,
    empty_message: &str,
    prompt: &str,
) -> Result<T> {
    if resources.is_empty() {
        bail!("{empty_message}");
    }

    resources.sort_by(|a, b| a.name().cmp(b.name()));
    let names: Vec<&str> = resources.iter().map(|item| item.name()).collect();

    let selection = crate::ui::fuzzy_select(prompt, &names)?;
    Ok(resources[selection].clone())
}
