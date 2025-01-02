use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bimap::BiMap;
use wasmtime::ResourceLimiter;
use anyhow::{Result, anyhow};

use crate::DataValue;
use crate::types::Blob;
use crate::job::Job;

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
