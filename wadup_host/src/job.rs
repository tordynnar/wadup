#![allow(dead_code)]

use std::path::PathBuf;
use std::sync::Arc;

use crossbeam::channel::Sender;
use wasmtime::{Module, Store, Trap};
use anyhow::Result;
use uuid::Uuid;

use crate::context::Context;
use crate::types::Blob;
use crate::environment::Environment;

pub enum JobOrDie {
    Job(Job),
    Die,
}

#[derive(Clone, Debug)]
pub struct JobInfo {
    pub id: Uuid,
    pub module_name: String,
    pub file_path: Option<PathBuf>,
}

#[derive(Clone)]
pub struct JobResult {
    pub id: Uuid,
    pub message: Option<String>,
    pub error: Option<String>,
}

pub enum JobTracking {
    JobInfo(JobInfo),
    JobResult(JobResult),
}

#[derive(Clone)]
pub struct Job {
    pub info: JobInfo, 
    pub job_sender: Sender<JobOrDie>,
    pub tracking_sender: Sender<JobTracking>,
    pub environment: Arc<Environment>,
    pub module: Arc<Module>,
    pub blob: Blob,
}

pub fn process(job: Job) -> Result<JobResult> {
    let mut store = Store::new(&job.environment.engine, Context {
        job: job.clone(),
        input: job.blob,
        output: Default::default(),
        schema: Default::default(),
        column: Default::default(),
        metadata: Default::default(),
        memory_limit: job.environment.args.memory,
        memory_used: Default::default(),
        table_limit: job.environment.args.table,
        table_used: Default::default(),
    });

    store.set_fuel(job.environment.args.fuel)?;
    store.limiter(|s| s);

    let instance = job.environment.linker.instantiate(&mut store, &job.module)?;
    
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
    let fuel_used = job.environment.args.fuel - fuel_end;

    let message = format!("{} {:?} memory used: {}, table used: {}, fuel used: {}", job.info.module_name, job.info.file_path, store.data().memory_used, store.data().table_used, fuel_used);
    Ok(JobResult {
        id: job.info.id,
        message: Some(message),
        error: None,
    })
}