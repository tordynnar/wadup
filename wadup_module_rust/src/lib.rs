use std::io::Read;
use serde::Serialize;
use anyhow::Error;
use wadup_bindings::{WadupInput, WadupOutput, wadup_start};

#[derive(Serialize)]
struct TestData {
    value1: String,
    value2: u64,
}

pub fn read_u32_le<R: Read>(input: &mut R) -> Result<u32, Error> {
    let mut buf = [0u8; 4];
    input.read(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

wadup_start!(main);

fn main() -> Result<(), Error> {
    let mut input = WadupInput::new();

    // Magic bytes "DD"
    let mut buf = [0u8; 2];
    input.read(&mut buf)?;
    if buf[0] != b'D' || buf[1] != b'D' {
        return Ok(());
    }

    let count = read_u32_le(&mut input)?;
    for _ in 0..count {
        let length = read_u32_le(&mut input)?;
        input.carve_from(length as u64);
    }

    let data = TestData {
        value1: "hello json".to_owned(),
        value2: 888,
    };

    let output = WadupOutput::new();
    serde_json::to_writer(output, &data)?;

    Ok(())
}
