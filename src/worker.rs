/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::env;
use std::sync::{Arc, Mutex, mpsc::{Sender, Receiver, TryRecvError}};
use std::{thread, thread::ThreadId};
use std::time;
use std::error::Error;
use rand::prelude::*;

use curl::easy::Easy;
use rusoto_s3::S3Client;
use rusoto_core::Region;
use rusoto_credential::EnvironmentProvider;

use crate::writer::Writer;
use crate::reader::Reader;
use crate::deleter::Deleter;
use crate::queue::Queue;
use crate::utils::ChumError;

pub const DIR: &str = "chum";

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: ThreadId,
    pub op: String, /* e.g. 'read' or 'write' */
    pub size: u64, /* in bytes */
    pub ttfb: u128, /* millis */
    pub rtt: u128, /* millis */
}

/*
 * WorkerInfos can be aggregated into WorkerStats.
 */
pub struct WorkerStat {
    pub objs: u64,
    pub data: u64,
    pub ttfb: u128,
    pub rtt: u128,
}

pub enum WorkerClient {
    WebDav(Easy),
    S3(S3Client),
}

fn bytes_to_human(bytes: u64) -> String {
    /* Need to decide if we really care about decimal precision. */
    format!("{:.3}MB", bytes / 1024 / 1024)
}

impl WorkerStat {
    pub fn new() -> Self {
        WorkerStat {
            objs: 0,
            data: 0,
            ttfb: 0,
            rtt: 0,
        }
    }
    pub fn add_result(&mut self, res: &WorkerInfo) {
        self.objs += 1;
        self.data += res.size;
        self.ttfb += res.ttfb;
        self.rtt += res.rtt;
    }

    pub fn clear(&mut self) {
        self.objs = 0;
        self.data = 0;
        self.ttfb = 0;
        self.rtt = 0;
    }

    /* For easy printing when the caller doesn't care about time. */
    pub fn serialize_relative(&mut self) -> String {
        format!("{} objects, {}, avg ttfb {}ms, avg rtt {}ms", self.objs,
            bytes_to_human(self.data), self.ttfb / u128::from(self.objs),
            self.rtt / u128::from(self.objs))
    }

    /*
     * For easy printing when the user cares about run time (e.g. computing
     * average throughput).
     */
    pub fn serialize_absolute(&mut self, d: u64) -> String {
        format!("{} objects, {}, {}s, avg {} objs/s, avg {}/s",
            self.objs, bytes_to_human(self.data),
            d, self.objs / d, bytes_to_human(self.data / d))
    }

}

pub trait WorkerTask {
    fn setup(&self, client: &mut WorkerClient)
        -> Result<(), Box<dyn Error>>;
    fn work(&mut self, client: &mut WorkerClient)
        -> Result<Option<WorkerInfo>, Box<dyn Error>>;
    fn get_type(&self) -> String;
}

pub struct Worker {
    writer: Writer,
    reader: Reader,
    deleter: Deleter,
    client: WorkerClient,
    tx: Sender<Result<WorkerInfo, ChumError>>,
    signal: Receiver<()>,
    pause: u64,
    ops: Vec<String>,
}

/*
 * A Worker is something that interacts with a target. It should emit events
 * in the form of a WorkerInfo for every operation performed.
 *
 * A Worker calls out to WorkerTask implementors and throws their WorkerInfo
 * into the tx mpsc to get picked up by a statistics listener.
 */
impl Worker {
    pub fn new(signal: Receiver<()>, tx: Sender<Result<WorkerInfo, ChumError>>,
        target: String, distr: Vec<u64>, pause: u64, queue: Arc<Mutex<Queue>>,
        ops: Vec<String>) -> Worker {

        let tok: Vec<&str> = target.split(':').collect();
        let protocol = tok[0].to_ascii_lowercase(); /* e.g. 's3' or 'webdav'. */
        let target = tok[1].to_string();

        let writer = Writer::new(target.clone(), distr, Arc::clone(&queue));
        let reader = Reader::new(target.clone(), Arc::clone(&queue));
        let deleter = Deleter::new(target.clone(), Arc::clone(&queue));

        /*
         * Construct a client of the given type.
         *
         * The S3 client needs a lot more up-front setup vs libcurl. libcurl
         * keeps around a bunch of global state that we overwrite each time
         * we use it.
         */
        let client = match protocol.as_ref() {
            "webdav" => WorkerClient::WebDav(Easy::new()),
            "s3" => {

                /*
                 * Users may supply access keys in environment variables. We use
                 * the minio defaults if keys are not provided.
                 */
                if env::var("AWS_ACCESS_KEY_ID").is_err() {
                    env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
                }
                if env::var("AWS_SECRET_ACCESS_KEY").is_err() {
                    env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
                }

                let region = Region::Custom {
                    name: "chum-s3".to_owned(),
                    endpoint: format!("http://{}:9000", target),
                };

                let s3client: S3Client = S3Client::new_with(
                    rusoto_core::request::HttpClient::new()
                        .expect("failed to create S3 HTTP client"),
                    EnvironmentProvider::default(),
                    region);

                WorkerClient::S3(s3client)
            },
            _ => panic!("unknown client protocol"),
        };

        Worker {
            writer,
            reader,
            deleter,
            client,
            tx,
            signal,
            pause,
            ops,
        }
    }

    pub fn work(&mut self) {
        let mut rng = thread_rng();

        /*
         * Perform setup routines.
         *
         * We added this because S3 writer workers need to make sure a bucket
         * is created before they start uploading files. The alternative is to
         * attempt to create the bucket before every upload which is wasteful.
         */
        for operator in &self.ops {
            let op: Box<dyn WorkerTask> = match operator.as_ref() {
                "r" => Box::new(&self.reader),
                "w" => Box::new(&self.writer),
                "d" => Box::new(&self.deleter),
                _ => panic!("unrecognized operator"),
            };

            if let Err(e) = op.setup(&mut self.client) {
                panic!("setup failed for worker ({}): {:?}",
                    op.get_type(), e);
            }
        }

        loop {
            /* Thread exits when it receives a signal over its channel. */

            { /* Scope so 'operator' doesn't hold an immutable borrow. */
                let mut operator: Box<dyn WorkerTask> =
                    match self.ops.choose(&mut rng)
                    .expect("choosing operation failed").as_ref() {

                    /* XXX we should use something more elegant here. */
                    "r" => Box::new(&self.reader),
                    "w" => Box::new(&self.writer),
                    "d" => Box::new(&self.deleter),
                    _ => panic!("unrecognized operator"),
                };

                /*
                 * Kick off an operation, collect the worker result, and send
                 * it off through the channel to the stat collector.
                 */
                match operator.work(&mut self.client) {
                    Ok(val) => if let Some(wr) = val {
                        /*
                         * The other end of this channel is likely no longer
                         * listening. Even though this worker performed work
                         * that will not be accounted for, stop the worker.
                         */
                        if self.should_stop() {
                            return;
                        }
                        self.send_info(Ok(wr));
                    },
                    Err(e) => {
                        if self.should_stop() {
                            println!("worker error: {}", e.to_string());
                            return;
                        }
                        self.send_info(Err(ChumError::new(&e.to_string())));
                    }
                }
            }
            self.sleep();
        }
    }

    fn sleep(&mut self) {
        if self.pause > 0 {
            thread::sleep(time::Duration::from_millis(self.pause));
        }
    }

    fn send_info(&self, res: Result<WorkerInfo, ChumError>) {
        match self.tx.send(res) {
            Ok(_) => (),
            Err(e) => {
                panic!(
                    "failed to send result: {}", e.to_string());
            }
        };
    }

    fn should_stop(&self) -> bool {
        match self.signal.try_recv() {
            Ok(_) | Err(TryRecvError::Disconnected) => true,
            Err(TryRecvError::Empty) => false,
        }
    }
}
