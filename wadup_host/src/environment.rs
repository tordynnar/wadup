use clap::Parser;
use wasmtime::{Config, Engine, Linker, Module};
use std::{fs, path::PathBuf, sync::Arc};
use anyhow::Result;
use crate::{bindings::add_to_linker, context::Context, load::load_module};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
pub struct Cli {
    #[arg(long)]
    pub modules: PathBuf,

    #[arg(long)]
    pub input: PathBuf,

    #[arg(long)]
    pub fuel: u64,

    #[arg(long)]
    pub memory: usize,

    #[arg(long)]
    pub table: usize,

    #[arg(long)]
    pub mapped: u64,
}

pub struct Environment {
    pub engine: Engine,
    pub linker: Linker<Context>,
    pub modules: Vec<(String, Arc<Module>)>,
    pub args: Cli,
}

impl Environment {
    pub fn create() -> Result<Environment> {
        let args = Cli::parse();

        let mut config = Config::new();
        config.consume_fuel(true);

        let engine = Engine::new(&config)?;

        let mut linker: Linker<Context> = Linker::new(&engine);
        add_to_linker(&mut linker)?;

        let module_paths = fs::read_dir(&args.modules)?
            .filter_map(|p| p.ok() )
            .map(|p| p.path())
            .filter(|p| p.extension().map(|s| s == "wasm").unwrap_or(false))
            .collect::<Vec<_>>();

        let modules = module_paths.iter()
            .map(|p| load_module(&engine, p))
            .collect::<Result<Vec<_>,_>>()?;

        let modules = modules.into_iter().map(|(n, m)| (n, Arc::new(m))).collect::<Vec<_>>();

        Ok(Environment {
            engine,
            linker,
            modules,
            args,
        })
    }
}