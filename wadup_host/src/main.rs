/*
TODO:
  - Not yet processing new data output from modules, only carved data
  - Track lineage what metadata was derived from what file; what file was derived from what file; etc.
  - Limit recursion
  - Have a list of submitted jobs and channel to signal which jobs are complete (removing jobs as they complete)
 */

#![feature(try_blocks)]

use std::sync::Arc;
use std::path::PathBuf;
use std::fs::{self, File};
use std::thread;
use std::sync::atomic::{AtomicU64, AtomicBool};
use std::sync::atomic::Ordering::Relaxed;
use clap::Parser;
use wasmtime::{Config, Engine, Linker};
use anyhow::Result;

mod bindings;
mod carve;
mod context;
mod job;
mod load;
mod types;
mod mmap;

use bindings::add_to_linker;
use context::Context;
use job::{process, Job, JobOrDie};
use load::load_module;
use types::Blob;
use mmap::Mmap;

#[derive(Debug)]
pub enum DataValue {
    StringValue(String),
    Int64Value(i64),
    Float64Value(f64),
    NoneValue,
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

    #[arg(long)]
    mapped: u64,
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
                                    let _ = sender.send(JobOrDie::Die);
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
                            println!("thread {}: processing job {} {}", thread_index, &job.module_name, &job.file_name);
                            if let Err(err) = process(job) {
                                println!("thread error {}: {}", thread_index, err);
                            }
                        },
                        _ => {
                            println!("thread: {} terminating", thread_index);
                            return;
                        }
                    }
                }
            });
        }

        let (free_sender, free_receiver) = crossbeam::channel::unbounded::<u64>();
        let mut mapped = 0u64;

        for input_path in input_paths {
            let result : Result<()> = try {
                let input_file = File::open(&input_path)?;
                
                let input_len = input_file.metadata()?.len();
                while mapped + input_len > args.mapped {
                    mapped -= free_receiver.recv()?;
                }

                mapped += input_len;
                let input_blob : Blob = Arc::new(Mmap::new(&input_file, input_len, free_sender.clone())?);

                for (module_name, module) in &*modules {
                    sender.send(JobOrDie::Job(Job {
                        sender: sender.clone(),
                        engine: engine.clone(),
                        linker: linker.clone(),
                        modules: modules.clone(),
                        module: module.clone(),
                        module_name: module_name.clone(),
                        file_name: input_path.as_os_str().to_str().unwrap_or("").to_owned(),
                        blob: input_blob.clone(),
                        fuel: args.fuel,
                        memory: args.memory,
                        table: args.table,
                    }))?;
                }
            };
            if let Err(err) = result {
                println!("Failed to create job from {}: {}", input_path.as_os_str().to_str().unwrap_or(""), err);
            }
        }
    });

    Ok(())
}
