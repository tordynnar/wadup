use std::io::Read;
use anyhow::Error;
use wadup_bindings::{WadupInput, WadupSchema, wadup_start};

wadup_start!(main);

fn main() -> Result<(), Error> {
    let mut input = WadupInput::new();

    let mut buf = Vec::<u8>::new();
    input.read_to_end(&mut buf)?;

    let schema = WadupSchema::new("schema1");
    let col_data = schema.column_str("data");
    let col_length = schema.column_i64("length");
    col_data.value(std::str::from_utf8(&buf)?);
    col_length.value(buf.len() as i64);
    schema.flush_row();

    Ok(())
}
