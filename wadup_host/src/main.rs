/*
TODO:
  - Not yet processing new data output from modules, only carved data
  - Track lineage what metadata was derived from what file; what file was derived from what file; etc.
  - Limit recursion
  - Have a list of submitted jobs and channel to signal which jobs are complete (removing jobs as they complete)
 */

#![feature(try_blocks)]

use std::sync::Arc;
use std::fs::{self, File};
use std::thread;
use std::sync::atomic::{AtomicU64, AtomicBool};
use std::sync::atomic::Ordering::Relaxed;
use anyhow::Result;

mod bindings;
mod carve;
mod context;
mod environment;
mod job;
mod load;
mod types;
mod mmap;

use environment::Environment;
use job::{process, Job, JobOrDie};
use types::Blob;
use mmap::Mmap;

fn main() -> Result<()> {
    let environment = Arc::new(Environment::create()?);
    
    let input_paths = fs::read_dir(&environment.args.input)?
        .filter_map(|p| p.ok() )
        .map(|p| p.path());

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
                while mapped + input_len > environment.args.mapped {
                    mapped -= free_receiver.recv()?;
                }

                mapped += input_len;
                let input_blob : Blob = Arc::new(Mmap::new(&input_file, input_len, free_sender.clone())?);

                for (module_name, module) in &*environment.modules {
                    sender.send(JobOrDie::Job(Job {
                        sender: sender.clone(),
                        environment: environment.clone(),
                        module: module.clone(),
                        module_name: module_name.clone(),
                        file_name: input_path.as_os_str().to_str().unwrap_or("").to_owned(),
                        blob: input_blob.clone(),
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
