/*
TODO:
  - Not yet processing new data output from modules, only carved data
  - Track lineage what metadata was derived from what file; what file was derived from what file; etc.
  - Limit recursion
 */

#![feature(try_blocks)]

use std::collections::HashSet;
use std::sync::Arc;
use std::fs::{self, File};
use std::thread;
use anyhow::{Result, anyhow};
use uuid::Uuid;

mod bindings;
mod carve;
mod context;
mod environment;
mod job;
mod load;
mod types;
mod mmap;

use environment::Environment;
use job::{process, Job, JobInfo, JobOrDie, JobResult, JobTracking};
use types::Blob;
use mmap::Mmap;

fn main() -> Result<()> {
    let (job_sender, job_receiver) = crossbeam::channel::unbounded::<JobOrDie>();
    let (tracking_sender, tracking_receiver) = crossbeam::channel::unbounded::<JobTracking>();
    let (free_sender, free_receiver) = crossbeam::channel::unbounded::<u64>();

    let environment = Arc::new(Environment::create()?);

    let file_paths = fs::read_dir(&environment.args.input)?
        .filter_map(|p| p.ok() )
        .map(|p| p.path());


    let jobs = file_paths.map(|file_path| {
        (file_path.clone(), environment.modules.iter().map(move |(module_name, module)| {
            (JobInfo {
                id: Uuid::new_v4(),
                module_name: module_name.clone(),
                file_path: Some(file_path.clone()),
            }, module.clone())
        }).collect::<Vec<_>>())
    }).collect::<Vec<_>>();

    for (_file_path, file_jobs) in &jobs {
        for (info, _module) in file_jobs {
            let _ = tracking_sender.send(JobTracking::JobInfo(info.clone()));
        }
    }

    let thread_count = environment.args.threads;

    thread::scope(|s| {
        let job_sender_2 = job_sender.clone();
        s.spawn(move || {
            let mut job_ids = HashSet::<Uuid>::new();
            loop {
                match tracking_receiver.recv() {
                    Ok(JobTracking::JobInfo(info)) => {
                        job_ids.insert(info.id);
                    },
                    Ok(JobTracking::JobResult(result)) => {
                        println!("RESULT: {:?}", result);
                        job_ids.remove(&result.id);
                        if job_ids.len() == 0 {
                            for _ in 0..thread_count {
                                let _ = job_sender_2.send(JobOrDie::Die);
                            }
                            return;
                        }
                    },
                    Err(_) => {
                        return;
                    },
                }
            }
        });

        for thread_index in 0..thread_count {
            let job_receiver = job_receiver.clone();
            let tracking_sender = tracking_sender.clone();
            s.spawn(move || {
                loop {
                    match job_receiver.recv() {
                        Ok(JobOrDie::Job(job)) => {
                            let job_id = job.info.id;
                            //println!("thread {}: processing job {} {:?}", thread_index, &job.info.module_name, &job.info.file_path);
                            let _ = tracking_sender.send(JobTracking::JobResult(match process(job) {
                                Ok(result) => result,
                                Err(err) => {
                                    let error = format!("thread error {}: {}", thread_index, err);
                                    JobResult {
                                        id: job_id,
                                        message: None,
                                        error: Some(error),
                                    }
                                },
                            }));
                        },
                        _ => {
                            //println!("thread: {} terminating", thread_index);
                            return;
                        }
                    }
                }
            });
        }

        let mut mapped = 0u64;
        for (file_path, file_jobs) in &jobs {
            let result : Result<Blob> = try {
                let file_handle = File::open(&file_path)?;
                
                let file_len = file_handle.metadata()?.len();
                if file_len > environment.args.mapped {
                    Err(anyhow!("File {:?} larger than maximum mapped memory", file_path))?;
                }
                while mapped + file_len > environment.args.mapped {
                    mapped -= free_receiver.recv()?;
                }

                mapped += file_len;
                let input_blob : Blob = Arc::new(Mmap::new(&file_handle, file_len, free_sender.clone())?);
                input_blob
            };
            match result {
                Ok(input_blob) => {
                    for (info, module) in file_jobs {
                        if let Err(err) = job_sender.send(JobOrDie::Job(Job {
                            info: info.clone(),
                            job_sender: job_sender.clone(),
                            tracking_sender: tracking_sender.clone(),
                            environment: environment.clone(),
                            module: module.clone(),
                            blob: input_blob.clone(),
                        })) {
                            let error = format!("Failed to send job {:?}: {}", info, err);
                            let _ = tracking_sender.send(JobTracking::JobResult(JobResult {
                                id: info.id,
                                message: None,
                                error: Some(error.clone()),
                            }));
                        }
                    }
                },
                Err(err) => {
                    let error = format!("Failed to create jobs from {:?}: {}", file_path, err);
                    for (info, _module) in file_jobs {
                        let _ = tracking_sender.send(JobTracking::JobResult(JobResult {
                            id: info.id,
                            message: None,
                            error: Some(error.clone()),
                        }));
                    }
                }
            }
        }
        
    });

    Ok(())
}
