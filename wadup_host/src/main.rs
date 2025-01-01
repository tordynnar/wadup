/*
TODO:
  - Not yet processing new data output from modules, only carved data
  - Track lineage what metadata was derived from what file; what file was derived from what file; etc.
  - Limit recursion
  - Split this code into multiple file
 */

#![feature(get_many_mut)]
#![allow(dead_code)]

use crossbeam::channel::Sender;
use wasmtime::{Caller, Config, Engine, Linker, Module, Store, ResourceLimiter, Trap};
use anyhow::{Result, anyhow};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;
use bimap::BiMap;
use std::time::UNIX_EPOCH;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::fs::{self, File};
use clap::Parser;
use memmap2::Mmap;
use std::thread;
use std::sync::atomic::{AtomicU64, AtomicBool};
use std::sync::atomic::Ordering::Relaxed;

type Blob = Arc<dyn AsRef<[u8]> + Sync + Send>;

#[derive(Debug)]
pub enum DataValue {
    StringValue(String),
    Int64Value(i64),
    Float64Value(f64),
    NoneValue,
}

pub struct Carve {
    pub data: Blob,
    pub offset: usize,
    pub length: usize,
}

impl Carve {
    pub fn new(data: Blob, offset: usize, length: usize) -> Result<Carve> {
        if offset + length > data.as_ref().as_ref().len() {
            Err(anyhow!("carve out of bounds"))
        } else {
            Ok(Carve { data, offset, length })
        }
    }
}

const EMPTY_CARVE: &[u8] = &[];

impl AsRef<[u8]> for Carve {
    fn as_ref(&self) -> &[u8] {
        let data = self.data.as_ref().as_ref();
        data.get(self.offset..(self.offset+self.length)).unwrap_or(EMPTY_CARVE)
    }
}

// TODO: Do all of these need to be Arc<Mutex<<>> ??

pub struct Context {
    pub job: Job,
    pub input: Blob,
    pub output: Arc<Mutex<Vec<Vec<u8>>>>,
    pub schema: Arc<Mutex<BiMap<String,u32>>>,
    pub column: Arc<Mutex<HashMap<u32,HashMap<String,u32>>>>,
    pub metadata: Arc<Mutex<HashMap<(u32,u32),DataValue>>>,
    pub memory_limit: usize,
    pub memory_used: usize,
    pub table_limit: usize,
    pub table_used: usize,
}

impl ResourceLimiter for Context {
    fn memory_growing(&mut self, _: usize, desired: usize, _: Option<usize>) -> Result<bool> {
        if desired > self.memory_limit {
            Err(anyhow!("memory limit exceeded"))
        } else {
            self.memory_used = desired;
            Ok(true)
        }
    }

    fn table_growing(&mut self, _: usize, desired: usize, _: Option<usize>) -> Result<bool> {
        if desired > self.table_limit {
            Err(anyhow!("table limit exceeded"))
        } else {
            self.table_used = desired;
            Ok(true)
        }
    }
}

fn wadup_read(data: &[u8], mut caller: Caller<'_, Context>, buffer: u32, offset: u64, length: u32) -> Result<u32> {
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

fn wadup_input_read(caller: Caller<'_, Context>, buffer: u32, offset: u64, length: u32) -> Result<u32> {
    let input = caller.data().input.clone();
    wadup_read(input.as_ref().as_ref(), caller, buffer, offset, length).map_err(|e| e.context("wadup_input_read"))
}

fn wadup_input_len(caller: Caller<'_, Context>) -> u64 {
    caller.data().input.as_ref().as_ref().len() as u64
}

fn wadup_input_carve(caller: Caller<'_, Context>, offset: u64, length: u64) -> Result<()> {
    let offset = usize::try_from(offset).map_err(|_| anyhow!("wadup_input_carve offset u64 to usize conversion failed"))?;
    let length = usize::try_from(length).map_err(|_| anyhow!("wadup_input_carve length u64 to usize conversion failed"))?;
    let carve = Arc::new(Carve::new(caller.data().input.clone(), offset, length)?);
    for (module_name, module) in &*caller.data().job.modules {
        caller.data().job.sender.send(JobOrDie::Job(Job {
            sender: caller.data().job.sender.clone(),
            engine: caller.data().job.engine.clone(),
            linker: caller.data().job.linker.clone(),
            modules: caller.data().job.modules.clone(),
            module: module.clone(),
            module_name: module_name.clone(),
            file_name: "[derived]".to_owned(),
            blob: carve.clone(),

            // TODO: Send the args into the job
            fuel: 10_000_000,
            memory: 10_000_000,
            table: 10_000,
        }))?;
    }
    Ok(())
}

fn wadup_output_create(caller: Caller<'_, Context>) -> Result<i32> {
    let mut output = caller.data().output.lock().map_err(|_| anyhow!("wadup_output_create unable to lock mutex"))?;
    output.push(Vec::new());
    let result = i32::try_from(output.len() - 1).map_err(|_| anyhow!("wadup_output_create result usize to i32 conversion failed"))?;
    Ok(result)
}

fn wadup_output_read(caller: Caller<'_, Context>, fd: i32, buffer: u32, offset: u64, length: u32) -> Result<u32> {
    let fd = usize::try_from(fd).map_err(|_| anyhow!("wadup_output_read fd i32 to usize conversion failed"))?;
    let output = caller.data().output.clone();
    let output = output.lock().map_err(|_| anyhow!("wadup_output_read unable to lock mutex"))?;
    let output = output.get(fd).ok_or_else(|| anyhow!("wadup_output_read fd does not exist"))?;
    wadup_read(&output, caller, buffer, offset, length).map_err(|e| e.context("wadup_output_read"))
}

fn wadup_output_write(mut caller: Caller<'_, Context>, fd: i32, buffer: u32, offset: u64, length: u32) -> Result<()> {
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

fn wadup_output_len(caller: Caller<'_, Context>, fd: i32) -> Result<u64> {
    let fd = usize::try_from(fd).map_err(|_| anyhow!("wadup_output_len fd i32 to usize conversion failed"))?;
    let output = caller.data().output.lock().map_err(|_| anyhow!("wadup_output_len unable to lock mutex"))?;
    let output = output.get(fd).ok_or_else(|| anyhow!("wadup_output_len fd does not exist"))?;
    let result = u64::try_from(output.len()).map_err(|_| anyhow!("wadup_output_len result usize to u64 conversion failed"))?;
    Ok(result)
}

fn wadup_string_from_buffer(memory: &[u8], buffer: u32, length: u32) -> Result<String> {
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

fn wadup_error(mut caller: Caller<'_, Context>, error: u32, error_length: u32) -> Result<()> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);

    let error = wadup_string_from_buffer(memory, error, error_length).map_err(|e| e.context("wadup_error"))?;
    Err(anyhow!("wasm module: {}", error))
}

fn wadup_metadata_schema(mut caller: Caller<'_, Context>, schema_name: u32, schema_length: u32) -> Result<u32> {
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

fn wadup_metadata_value_str(mut caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: u32, value_length: u32) -> Result<()> {
    let memory = caller.get_export("memory").and_then(|v| v.into_memory()).ok_or(anyhow!("wadup_error memory not exported"))?;
    let memory = memory.data(&caller);

    let value = wadup_string_from_buffer(memory, value, value_length).map_err(|e| e.context("wadup_metadata_value_str"))?;
    
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_str failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::StringValue(value));
    Ok(())
}

fn wadup_metadata_value_i64(caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: i64) -> Result<()> {
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_i64 failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::Int64Value(value));
    Ok(())
}

fn wadup_metadata_value_f64(caller: Caller<'_, Context>, schema_index: u32, column_index: u32, value: f64) -> Result<()> {
    let mut metadata = caller.data().metadata.lock().map_err(|_| anyhow!("wadup_metadata_value_f64 failed to get metadata lock"))?;
    metadata.insert((schema_index, column_index), DataValue::Float64Value(value));
    Ok(())
}

fn wadup_metadata_flush_row(caller: Caller<'_, Context>, schema_index: u32) -> Result<()> {
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

fn add_to_linker(linker : &mut Linker<Context>) -> Result<()> {
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

pub fn read_u64_le<R: Read>(input: &mut R) -> Result<u64> {
    let mut buf = [0u8; 8];
    input.read(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

pub fn write_u64_le<W: Write>(output: &mut W, value: u64) -> Result<()> {
    output.write_all(value.to_le_bytes().as_ref())?;
    Ok(())
}

fn read_compiled(module_compiled_path: &Path, engine_hash: u64, module_modified: u64) -> Result<Vec<u8>> {
    if !module_compiled_path.exists() {
        return Err(anyhow!("Compiled module path doesn't exist"))
    }

    let mut file = File::open(module_compiled_path)?;

    if read_u64_le(&mut file)? != engine_hash {
        return Err(anyhow!("Engine hash doesn't match"))
    };

    if read_u64_le(&mut file)? != module_modified {
        return Err(anyhow!("Module modified doesn't match"))
    };

    let mut result = Vec::new();
    file.read_to_end(&mut result)?;

    Ok(result)
}

fn load_module(engine: &Engine, module_path: &PathBuf) -> Result<(String, Module)> {
    let mut hasher = DefaultHasher::new();
    engine.precompile_compatibility_hash().hash(&mut hasher);
    let engine_hash = hasher.finish();

    let module_modified = fs::metadata(module_path)?.modified()?.duration_since(UNIX_EPOCH)?.as_secs();

    let module_compiled_path = format!("{}_precompiled", module_path.display());
    let module_compiled_path = Path::new(&module_compiled_path);

    let module = if let Some(module_compiled) = read_compiled(module_compiled_path, engine_hash, module_modified).ok() {
        unsafe { Module::deserialize(&engine, &module_compiled) }?
    } else {
        let module = Module::from_file(&engine, module_path)?;
        let mut file = File::create(module_compiled_path)?;
        write_u64_le(&mut file, engine_hash)?;
        write_u64_le(&mut file, module_modified)?;
        let module_serialized = module.serialize()?;
        file.write_all(module_serialized.as_slice())?;
        module
    };

    let name = module_path
        .file_name().ok_or_else(|| anyhow!("unable to get module file name"))?
        .to_str().ok_or_else(|| anyhow!("unable to convert module file name to string"))?
        .to_owned();

    Ok((name, module))
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Cli {
    #[arg(long)]
    modules: PathBuf,

    #[arg(long)]
    input: PathBuf,

    #[arg(long)]
    fuel: u64,

    #[arg(long)]
    memory: usize,

    #[arg(long)]
    table: usize,
}

fn process(job: Job) -> Result<()> {
    let mut store = Store::new(&job.engine, Context {
        job: job.clone(),
        input: job.blob,
        output: Default::default(),
        schema: Default::default(),
        column: Default::default(),
        metadata: Default::default(),
        memory_limit: job.memory,
        memory_used: Default::default(),
        table_limit: job.table,
        table_used: Default::default(),
    });

    store.set_fuel(job.fuel)?;
    store.limiter(|s| s);

    let instance = job.linker.instantiate(&mut store, &job.module)?;
    
    let func = instance.get_typed_func::<(), ()>(&mut store, "wadup_run")?;

    if let Some(e) = func.call(&mut store, ()).err() {
        let e = if let Some(e) = e.downcast_ref::<Trap>() {
            e.to_string()
        } else if let Some(e) = e.downcast_ref::<String>() {
            e.to_string()
        } else {
            e.to_string()
        };
        println!("ERROR: {}", e);
    }

    let fuel_end = store.get_fuel()?;
    let fuel_used = job.fuel - fuel_end;

    println!("{} {} memory used: {}, table used: {}, fuel used: {}", job.module_name, job.file_name, store.data().memory_used, store.data().table_used, fuel_used);

    Ok(())
}

enum JobOrDie {
    Job(Job),
    Die,
}

#[derive(Clone)]
pub struct Job {
    sender: Sender<JobOrDie>,
    engine: Arc<Engine>,
    linker: Arc<Linker<Context>>,
    modules: Arc<Vec<(String, Arc<Module>)>>,
    module: Arc<Module>,
    module_name: String,
    file_name: String,
    blob: Blob,
    fuel: u64,
    memory: usize,
    table: usize,
}

fn main() -> Result<()> {
    let args = Cli::parse();

    let mut config = Config::new();
    config.consume_fuel(true);

    let engine = Engine::new(&config)?;

    let mut linker: Linker<Context> = Linker::new(&engine);
    add_to_linker(&mut linker)?;

    let module_paths = fs::read_dir(args.modules)?
        .filter_map(|p| p.ok() )
        .map(|p| p.path())
        .filter(|p| p.extension().map(|s| s == "wasm").unwrap_or(false))
        .collect::<Vec<_>>();

    let modules = module_paths.iter()
        .map(|p| load_module(&engine, p))
        .collect::<Result<Vec<_>,_>>()?;

    let modules = modules.into_iter().map(|(n, m)| (n, Arc::new(m))).collect::<Vec<_>>();

    let input_paths = fs::read_dir(args.input)?
        .filter_map(|p| p.ok() )
        .map(|p| p.path());

    let engine = Arc::new(engine);
    let linker = Arc::new(linker);
    let modules = Arc::new(modules);

    let (sender, receiver) = crossbeam::channel::unbounded::<JobOrDie>();

    // TODO:
    //  - Tracking state like this is clever, but probably want a locking state object for display purposes
    //  - Alternatively, could use have a list of submitted jobs and channel to signal which jobs are complete (removing jobs as they complete)

    let waiting = &AtomicU64::new(0);
    let started = &AtomicBool::new(false);
    let thread_count = 5usize; // TODO: test for <= 64 (allow an extra bit to allow calculation of the next value)
    let all_waiting = if thread_count == 64 {
        0xffffffffffffffff
    } else {
        (1u64 << thread_count) - 1
    };

    thread::scope(|s| {
        for thread_index in 0..thread_count {
            let thread_mask = 1u64 << thread_index;
            let sender = sender.clone();
            let receiver = receiver.clone();
            s.spawn(move || {
                loop {
                    if receiver.is_empty() {
                        println!("thread {}: no job in queue", thread_index);
                        if waiting.fetch_or(thread_mask, Relaxed) | thread_mask == all_waiting {
                            if started.load(Relaxed) {
                                println!("thread {}: all receivers waiting, sending die!!", thread_index);
                                for _ in 0..thread_count {
                                    sender.send(JobOrDie::Die).unwrap(); // TODO: Remove unwrap
                                }
                                return;
                            } else {
                                println!("thread {}: all receivers waiting, but haven't received first job yet", thread_index)
                            }
                        }
                    } else {
                        println!("thread {}: job in queue", thread_index);
                    }
                    match receiver.recv() {
                        Ok(JobOrDie::Job(job)) => {
                            waiting.fetch_and(all_waiting - thread_mask, Relaxed);
                            started.store(true, Relaxed);
                            println!("thread {}: processing job {} {}", thread_index, job.module_name, job.file_name);
                            process(job).unwrap(); // TODO: Remove unwrap
                        },
                        _ => {
                            println!("thread: {} terminating", thread_index);
                            return;
                        }
                    }
                }
            });
        }

        for input_path in input_paths {
            let input_file = File::open(&input_path).unwrap(); // TODO: remove unwrap
            // TODO: Check size and only start the queue if we won't exceed maximum mmap'd files
            let input_blob : Blob = Arc::new(unsafe { Mmap::map(&input_file).unwrap() }); // TODO: remove unwrap
            for (module_name, module) in &*modules {
                sender.send(JobOrDie::Job(Job {
                    // These could be all in one Arc<> ... Arc<Environment>??
                    sender: sender.clone(),
                    engine: engine.clone(),
                    linker: linker.clone(),
                    modules: modules.clone(),

                    module: module.clone(),
                    module_name: module_name.clone(),
                    file_name: input_path.as_os_str().to_str().unwrap().to_owned(), // TODO: remove unwrap
                    blob: input_blob.clone(),
                    fuel: args.fuel,
                    memory: args.memory,
                    table: args.table,
                })).unwrap(); // TODO: remove unwrap
            }
        }
    });

    Ok(())
}
