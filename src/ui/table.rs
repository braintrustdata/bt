use comfy_table::{presets::NOTHING, Attribute, Cell, Table};

/// Create a table with the standard CLI styling (no borders)
pub fn styled_table() -> Table {
    let mut table = Table::new();
    table.load_preset(NOTHING);
    table
}

/// Create a header cell with dim + bold styling
pub fn header(text: &str) -> Cell {
    Cell::new(text)
        .add_attribute(Attribute::Bold)
        .add_attribute(Attribute::Dim)
}
