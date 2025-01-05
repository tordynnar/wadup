use std::sync::Arc;
use wasmtime::{Caller, Linker};
use anyhow::{Result, anyhow};
use uuid::Uuid;

use crate::{carve::Carve, types::DataValue};
use crate::context::Context;
use crate::job::{JobOrDie, Job, JobInfo};

pub fn wadup_read(data: &[u8], mut caller: Caller<'_, Context>, buffer: u32, offset: u64, length: u32) -> Result<u32> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_read memory not exported"))?;
    let start = usize::try_from(offset).map_err(|_| anyhow!("wadup_read offset u64 to usize conversion failed"))?;
    let length = usize::try_from(length).map_err(|_| anyhow!("wadup_read length u64 to usize conversion failed"))?;
    let end = std::cmp::min(data.len(), start + length);
    let input = data.get(start..end).unwrap_or_default();
    let buffer = usize::try_from(buffer).map_err(|_| anyhow!("wadup_read buffer u32 to usize conversion failed"))?;
    memory.write(&mut caller, buffer, input).map_err(|_| anyhow!("wadup_read failed to write memory"))?;
    let result = u32::try_from(input.len()).map_err(|_| anyhow!("wadup_read result usize to u32 conversion failed"))?;
    Ok(result)
}

pub fn wadup_input_read(caller: Caller<'_, Context>, buffer: u32, offset: u64, length: u32) -> Result<u32> {
    let input = caller.data().input.clone();
    wadup_read(input.as_ref().as_ref(), caller, buffer, offset, length).map_err(|e| e.context("wadup_input_read"))
}

pub fn wadup_input_len(caller: Caller<'_, Context>) -> u64 {
    caller.data().input.as_ref().as_ref().len() as u64
}

pub fn wadup_input_carve(caller: Caller<'_, Context>, offset: u64, length: u64) -> Result<()> {
    let offset = usize::try_from(offset).map_err(|_| anyhow!("wadup_input_carve offset u64 to usize conversion failed"))?;
    let length = usize::try_from(length).map_err(|_| anyhow!("wadup_input_carve length u64 to usize conversion failed"))?;
    let carve = Arc::new(Carve::new(caller.data().input.clone(), offset, length)?);
    for (module_name, module) in &*caller.data().job.environment.modules {
        caller.data().job.job_sender.send(JobOrDie::Job(Job {
            info: JobInfo {
                id: Uuid::new_v4(),
                module_name: module_name.clone(),
                file_path: None,
            },
            job_sender: caller.data().job.job_sender.clone(),
            tracking_sender: caller.data().job.tracking_sender.clone(),
            environment: caller.data().job.environment.clone(),
            module: module.clone(),
            blob: carve.clone(),
        }))?;
    }
    Ok(())
}

pub fn wadup_output_create(caller: Caller<'_, Context>) -> Result<i32> {
    let mut output = caller.data().output.lock().map_err(|_| anyhow!("wadup_output_create unable to lock mutex"))?;
    output.push(Vec::new());
    let result = i32::try_from(output.len() - 1).map_err(|_| anyhow!("wadup_output_create result usize to i32 conversion failed"))?;
    Ok(result)
}

pub fn wadup_output_read(caller: Caller<'_, Context>, fd: i32, buffer: u32, offset: u64, length: u32) -> Result<u32> {
    let fd = usize::try_from(fd).map_err(|_| anyhow!("wadup_output_read fd i32 to usize conversion failed"))?;
    let output = caller.data().output.clone();
    let output = output.lock().map_err(|_| anyhow!("wadup_output_read unable to lock mutex"))?;
    let output = output.get(fd).ok_or_else(|| anyhow!("wadup_output_read fd does not exist"))?;
    wadup_read(&output, caller, buffer, offset, length).map_err(|e| e.context("wadup_output_read"))
}

pub fn wadup_output_write(mut caller: Caller<'_, Context>, fd: i32, buffer: u32, offset: u64, length: u32) -> Result<()> {
    let buffer = usize::try_from(buffer).map_err(|_| anyhow!("wadup_output_write buffer u32 to usize conversion failed"))?;
    let length = usize::try_from(length).map_err(|_| anyhow!("wadup_output_write length u32 to usize conversion failed"))?;
    let offset = usize::try_from(offset).map_err(|_| anyhow!("wadup_output_write offset u64 to usize conversion falied"))?;
    
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_output_write memory not exported"))?;
    let memory = memory.data(&caller);

    let fd = usize::try_from(fd).map_err(|_| anyhow!("wadup_output_write fd i32 to usize conversion failed"))?;
    let mut output = caller.data().output.lock().map_err(|_| anyhow!("wadup_output_write unable to lock mutex"))?;
    let output = output.get_mut(fd).ok_or_else(|| anyhow!("wadup_output_write fd does not exist"))?;
    
    output.resize(std::cmp::max(offset + length, output.len()), 0);
    let output = &mut output[offset..offset+length];
    let memory = memory.get(buffer..buffer+length).ok_or_else(|| anyhow!("wadup_output_write cannot get memory buffer"))?;
    output.copy_from_slice(memory);

    Ok(())
}

pub fn wadup_output_len(caller: Caller<'_, Context>, fd: i32) -> Result<u64> {
    let fd = usize::try_from(fd).map_err(|_| anyhow!("wadup_output_len fd i32 to usize conversion failed"))?;
    let output = caller.data().output.lock().map_err(|_| anyhow!("wadup_output_len unable to lock mutex"))?;
    let output = output.get(fd).ok_or_else(|| anyhow!("wadup_output_len fd does not exist"))?;
    let result = u64::try_from(output.len()).map_err(|_| anyhow!("wadup_output_len result usize to u64 conversion failed"))?;
    Ok(result)
}

pub fn wadup_string_from_buffer(memory: &[u8], buffer: u32, length: u32) -> Result<String> {
    let buffer = usize::try_from(buffer).map_err(|_| anyhow!("wadup_string_from_buffer buffer u32 to usize conversion failed"))?;
    let length = usize::try_from(length).map_err(|_| anyhow!("wadup_string_from_buffer length u32 to usize conversion failed"))?;

    let mut value = Vec::<u8>::new();
    value.resize(length, 0);
    let value_buffer = value.as_mut_slice();
    let memory = memory.get(buffer..buffer+length).ok_or_else(|| anyhow!("wadup_string_from_buffer cannot get memory buffer"))?;
    value_buffer.copy_from_slice(memory);

    let value = std::str::from_utf8(&value).map_err(|_| anyhow!("wadup_string_from_buffer cannot convert bytes to UTF8"))?;
    Ok(value.to_owned())
}

pub fn wadup_error(mut caller: Caller<'_, Context>, error: u32, error_length: u32) -> Result<()> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);

    let error = wadup_string_from_buffer(memory, error, error_length).map_err(|e| e.context("wadup_error"))?;
    Err(anyhow!("wasm module: {}", error))
}

pub fn wadup_metadata_schema(mut caller: Caller<'_, Context>, schema_name: u32, schema_length: u32) -> Result<u32> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);
    
    let schema_name = wadup_string_from_buffer(memory, schema_name, schema_length).map_err(|e| e.context("wadup_metadata_schema"))?;
    let mut schema = caller.data().schema.lock().map_err(|_| anyhow!("wadup_metadata_schema failed to get schema lock"))?;
    let schema_index = schema.get_by_left(&schema_name).map(|v| v.to_owned()).unwrap_or_else(|| {
        let next_schema_index = schema.iter().map(|(_, v)| v.to_owned()).max().unwrap_or(0u32) + 1;
        schema.insert(schema_name, next_schema_index);
        next_schema_index
    });
    Ok(schema_index)
}

fn wadup_metadata_column(mut caller: Caller<'_, Context>, schema_index: u32, column_name: u32, column_length: u32, _column_type: u32) -> Result<u32> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);

    let column_name = wadup_string_from_buffer(memory, column_name, column_length).map_err(|e| e.context("wadup_metadata_column"))?;
    let mut column = caller.data().column.lock().map_err(|_| anyhow!("wadup_metadata_column failed to get column lock"))?;
    let column = column.entry(schema_index).or_default();
    let column_index = column.get(&column_name).map(|v| v.to_owned()).unwrap_or_else(|| {
        let next_column_index = column.iter().map(|(_, v)| v.to_owned()).max().unwrap_or(0u32) + 1;
        column.insert(column_name, next_column_index);
        next_column_index
    });
    Ok(column_index.to_owned())
}

pub fn wadup_metadata_value_str(mut caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: u32, value_length: u32) -> Result<()> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);

    let value = wadup_string_from_buffer(memory, value, value_length).map_err(|e| e.context("wadup_metadata_value_str"))?;
    
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_str failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::StringValue(value));
    Ok(())
}

pub fn wadup_metadata_value_i64(caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: i64) -> Result<()> {
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_i64 failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::Int64Value(value));
    Ok(())
}

pub fn wadup_metadata_value_f64(caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: f64) -> Result<()> {
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_f64 failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::Float64Value(value));
    Ok(())
}

pub fn wadup_metadata_flush_row(caller: Caller<'_, Context>, schema_index: u32) -> Result<()> {
    let schema = caller.data().schema.lock().map_err(|_| anyhow!("wadup_metadata_flush_row failed to get metadata lock"))?;
    let column = caller.data().column.lock().map_err(|_| anyhow!("wadup_metadata_flush_row failed to get metadata lock"))?;
    let metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_flush_row failed to get metadata lock"))?;
    let schema_name = schema.get_by_right(&schema_index).ok_or_else(|| anyhow!("wadup_metadata_flush_row schema index not found"))?;
    let column = column.get(&schema_index).ok_or_else(|| anyhow!("wadup_metadata_flush_row schema index not found"))?;
    for (column_name, column_index) in column {
        let value = metadata.get(&(schema_index, column_index.to_owned())).unwrap_or(&DataValue::NoneValue);
        println!("{} {} {:?}", schema_name, column_name, value);
    }
    Ok(())
}

pub fn add_to_linker(linker : &mut Linker<Context>) -> Result<()> {
    linker.func_wrap("host", "wadup_input_read", wadup_input_read)?;
    linker.func_wrap("host", "wadup_input_len", wadup_input_len)?;
    linker.func_wrap("host", "wadup_input_carve", wadup_input_carve)?;
    linker.func_wrap("host", "wadup_output_create", wadup_output_create)?;
    linker.func_wrap("host", "wadup_output_read", wadup_output_read)?;
    linker.func_wrap("host", "wadup_output_write", wadup_output_write)?;
    linker.func_wrap("host", "wadup_output_len", wadup_output_len)?;
    linker.func_wrap("host", "wadup_error", wadup_error)?;
    linker.func_wrap("host", "wadup_metadata_schema", wadup_metadata_schema)?;
    linker.func_wrap("host", "wadup_metadata_column", wadup_metadata_column)?;
    linker.func_wrap("host", "wadup_metadata_value_str", wadup_metadata_value_str)?;
    linker.func_wrap("host", "wadup_metadata_value_i64", wadup_metadata_value_i64)?;
    linker.func_wrap("host", "wadup_metadata_value_f64", wadup_metadata_value_f64)?;
    linker.func_wrap("host", "wadup_metadata_flush_row", wadup_metadata_flush_row)?;
    Ok(())
}
