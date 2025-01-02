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
use anyhow::{Result, anyhow};

mod bindings;
mod carve;
mod context;
mod environment;
mod job;
mod load;
mod types;
mod mmap;

use crossbeam::channel::TryRecvError;
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
    let started = &AtomicBool::new(false); // TODO: Can started be a thread local, non-atomic value?? Answer: No, if a thread starts, processes a job and finishes before any other thread starts, then it locks
    // BUG: What if a thread starts, processes a job and finishes before any other thread has started; and the started.store(true) hasn't registered yet - then it locks right? Unlikely, but possible
    let thread_count = environment.args.threads;
    if thread_count > 64 {
        return Err(anyhow!("Up to 64 threads are supported"));
    }
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
                    let job_or_die = match receiver.try_recv() {
                        Ok(job_or_die) => {
                            job_or_die
                        },
                        Err(TryRecvError::Disconnected) => {
                            JobOrDie::Die
                        }
                        Err(TryRecvError::Empty) => {
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

                            match receiver.recv() {
                                Ok(job_or_die) => {
                                    job_or_die
                                },
                                Err(_) => {
                                    JobOrDie::Die
                                }
                            }
                        }
                    };

                    match job_or_die {
                        JobOrDie::Job(job) => {
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
                if input_len > environment.args.mapped {
                    Err(anyhow!("File {} larger than maximum mapped memory", input_path.as_os_str().to_str().unwrap_or("")))?;
                }
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
