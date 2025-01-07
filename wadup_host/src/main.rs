/*
TODO:
  - Not yet processing new data output from modules, only carved data
  - Track lineage what metadata was derived from what file; what file was derived from what file; etc.
  - Limit recursion
 */

#![feature(try_blocks)]
#![feature(mpmc_channel)]

use std::collections::HashSet;
use std::sync::Arc;
use std::fs::{self, File};
use std::path::PathBuf;
use std::thread;
use std::sync::mpmc::{Sender, Receiver, channel};
use anyhow::{Result, anyhow};
use uuid::Uuid;
use wasmtime::Module;

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

fn tracker_thread(thread_count: usize, tracking_receiver: Receiver<JobTracking>, job_sender: Sender<JobOrDie>) {
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
                        job_sender.send(JobOrDie::Die).unwrap();
                    }
                    return;
                }
            },
            Err(_) => {
                return;
            },
        }
    }
}

fn process_thread(job_receiver: Receiver<JobOrDie>, tracking_sender: Sender<JobTracking>) {
    loop {
        match job_receiver.recv() {
            Ok(JobOrDie::Job(job)) => {
                let job_id = job.info.id;
                tracking_sender.send(JobTracking::JobResult(match process(job) {
                    Ok(result) => result,
                    Err(err) => {
                        JobResult {
                            id: job_id,
                            message: None,
                            error: Some(err.to_string()),
                        }
                    },
                })).unwrap();
            },
            _ => {
                return;
            }
        }
    }
}

fn input_thread(
    jobs: Vec<(PathBuf, Vec<(JobInfo, Arc<Module>)>)>, 
    environment: Arc<Environment>,
    job_sender: Sender<JobOrDie>,
    tracking_sender: Sender<JobTracking>,
) {
    let (free_sender, free_receiver) = channel::<u64>();
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
                    job_sender.send(JobOrDie::Job(Job {
                        info: info.clone(),
                        job_sender: job_sender.clone(),
                        tracking_sender: tracking_sender.clone(),
                        environment: environment.clone(),
                        module: module.clone(),
                        blob: input_blob.clone(),
                    })).unwrap();
                }
            },
            Err(err) => {
                let error = format!("Failed to create jobs from {:?}: {}", file_path, err);
                for (info, _module) in file_jobs {
                    tracking_sender.send(JobTracking::JobResult(JobResult {
                        id: info.id,
                        message: None,
                        error: Some(error.clone()),
                    })).unwrap();
                }
            }
        }
    }
}

fn create_jobs_one_shot(environment: Arc<Environment>, tracking_sender: Sender<JobTracking>) -> Result<Vec<(PathBuf, Vec<(JobInfo, Arc<Module>)>)>> {
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
            tracking_sender.send(JobTracking::JobInfo(info.clone())).unwrap();
        }
    }

    Ok(jobs)
}

fn main() -> Result<()> {
    let (job_sender, job_receiver) = channel::<JobOrDie>();
    let (tracking_sender, tracking_receiver) = channel::<JobTracking>();

    let environment = Arc::new(Environment::create()?);
    let thread_count = environment.args.threads;

    let jobs = create_jobs_one_shot(environment.clone(), tracking_sender.clone())?;

    thread::scope(|s| {
        s.spawn(|| {
            tracker_thread(thread_count, tracking_receiver, job_sender.clone());
        });

        for _ in 0..thread_count {
            s.spawn(|| {
                process_thread(job_receiver.clone(), tracking_sender.clone());
            });
        }

        s.spawn(|| {
            input_thread(jobs, environment, job_sender.clone(), tracking_sender.clone());
        });
    });

    Ok(())
}
