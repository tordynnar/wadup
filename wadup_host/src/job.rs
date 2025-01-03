use std::sync::Arc;

use crossbeam::channel::Sender;
use wasmtime::{Module, Store, Trap};
use anyhow::Result;

use crate::{context::Context, types::Blob, environment::Environment};

pub enum JobOrDie {
    Job(Job),
    Die,
}

#[derive(Clone)]
pub struct Job {
    pub sender: Sender<JobOrDie>,
    pub environment: Arc<Environment>,
    pub module: Arc<Module>,
    pub module_name: String,
    pub file_name: String,
    pub blob: Blob,
}

pub fn process(job: Job) -> Result<()> {
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

    println!("{} {} memory used: {}, table used: {}, fuel used: {}", job.module_name, job.file_name, store.data().memory_used, store.data().table_used, fuel_used);

    Ok(())
}