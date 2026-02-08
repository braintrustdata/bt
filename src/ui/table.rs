use comfy_table::{presets::NOTHING, Attribute, Cell, ContentArrangement, Table};

pub fn styled_table() -> Table {
    let mut table = Table::new();
    table.load_preset(NOTHING);
    if let Ok((width, _)) = crossterm::terminal::size() {
        table.set_width(width);
    }
    table.set_content_arrangement(ContentArrangement::Dynamic);
    table
}

pub fn truncate(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        return text.to_string();
    }

    let keep = max_len.saturating_sub(1);
    let truncated: String = text.chars().take(keep).collect();
    format!("{truncated}…")
}

pub fn apply_column_padding(table: &mut Table, padding: (u16, u16)) {
    for i in 0..table.column_count() {
        if let Some(col) = table.column_mut(i) {
            col.set_padding(padding);
        }
    }
}

pub fn header(text: &str) -> Cell {
    Cell::new(text)
        .add_attribute(Attribute::Bold)
        .add_attribute(Attribute::Dim)
}
