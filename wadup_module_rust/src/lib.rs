use std::io::Read;
use serde::Serialize;
use anyhow::Error;
use wadup_bindings::{WadupInput, WadupOutput, WadupSchema, wadup_start};

#[derive(Serialize)]
struct TestData {
    value1: String,
    value2: Vec<String>,
    value3: u64,
}

wadup_start!(main);

fn main() -> Result<(), Error> {
    let mut input = WadupInput::new();

    let mut buf = Vec::<u8>::new();
    input.read_to_end(&mut buf)?;

    let mut result = 0u64;
    for i in 0..buf.len() {
        result += buf[i] as u64;
    }

    let data = TestData {
        value1: "hello world".to_owned(),
        value2: vec!["this".to_owned(), "is a test".to_owned()],
        value3: result,
    };

    let output = WadupOutput::new();
    serde_json::to_writer(output, &data)?;

    let schema = WadupSchema::new("schema1");
    let column1 = schema.column_str("column1");
    let column2 = schema.column_i64("column2");
    let column3 = schema.column_f64("column3");

    column1.value("hello");
    column2.value(199);
    column3.value(76.6);
    schema.flush_row();

    column1.value("bye");
    column2.value(777);
    column3.value(88.8);
    schema.flush_row();

    input.carve(3, 2);

    Ok(())
    //Err(anyhow::anyhow!("an error!!!"))
}
