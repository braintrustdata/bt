use comfy_table::{presets::NOTHING, Attribute, Cell, ContentArrangement, Table};

/// Create a table with the standard CLI styling (no borders, no wrapping)
pub fn styled_table() -> Table {
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table.set_content_arrangement(ContentArrangement::Disabled);
    table
}

/// Truncate text to max length with ellipsis
pub fn truncate(text: &str, max_len: usize) -> String {
    if text.len() <= max_len {
        text.to_string()
    } else {
        format!("{}…", &text[..max_len.saturating_sub(1)])
    }
}

/// Apply padding to all columns (call after setting headers)
pub fn apply_column_padding(table: &mut Table, padding: (u16, u16)) {
    for i in 0..table.column_count() {
        if let Some(col) = table.column_mut(i) {
            col.set_padding(padding);
        }
    }
}

/// Create a header cell with dim + bold styling
pub fn header(text: &str) -> Cell {
    Cell::new(text)
        .add_attribute(Attribute::Bold)
        .add_attribute(Attribute::Dim)
}
