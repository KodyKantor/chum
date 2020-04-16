/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::sync::{Arc, Mutex, mpsc::{Sender, SendError}};
use std::{thread, thread::ThreadId};
use std::time;
use rand::prelude::*;

use crate::queue::Queue;
use crate::utils::ChumError;
use crate::s3::S3;
use crate::fs::Fs;
use crate::webdav::WebDav;
use crate::state::State;

pub const DIR: &str = "chum";

#[derive(Debug)]
pub struct WorkerInfo {
    pub id: ThreadId,
    pub op: Operation, /* e.g. 'read' or 'write' */
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

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum Operation {
    Read,
    Write,
    Delete,
    Error,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let str = match self {
            Operation::Read => "read",
            Operation::Write => "write",
            Operation::Delete => "delete",
            Operation::Error => "error",
        };
        write!(f, "{}", str)
    }
}

pub trait Backend {
    fn write(&self) -> Result<Option<WorkerInfo>, ChumError>;
    fn read(&self) -> Result<Option<WorkerInfo>, ChumError>;
    fn delete(&self) -> Result<Option<WorkerInfo>, ChumError>;
}

pub struct Worker {
    backend: Box<dyn Backend>,
    tx: Sender<Result<WorkerInfo, ChumError>>,
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
    pub fn new(
        tx: Sender<Result<WorkerInfo, ChumError>>,
        target: String,
        distr: Vec<u64>,
        pause: u64,
        queue: Arc<Mutex<Queue>>,
        ops: Vec<String>,
        dtx: Option<Sender<State>>)
    -> Worker {

        let tok: Vec<&str> = target.split(':').collect();
        let protocol = tok[0].to_ascii_lowercase(); /* e.g. 's3' or 'webdav'. */
        let target = tok[1].to_string();

        /*
         * Construct a client of the given type.
         *
         * The S3 client needs a lot more up-front setup vs libcurl. libcurl
         * keeps around a bunch of global state that we overwrite each time
         * we use it.
         */
        let backend: Box<dyn Backend> = match protocol.as_ref() {
            "webdav" => {
                Box::new(WebDav::new(target, distr,
                    Arc::clone(&queue), dtx))
            },
            "s3" => {
                Box::new(S3::new(target, distr,
                    Arc::clone(&queue), dtx))
            },
            "fs" => {
                Box::new(Fs::new(target, distr,
                    Arc::clone(&queue), dtx))
            }
            _ => panic!("unknown client protocol"),
        };

        Worker {
            backend,
            tx,
            pause,
            ops,
        }
    }

    pub fn process_result(&self, res: Result<Option<WorkerInfo>, ChumError>)
        -> Result<(), SendError<Result<WorkerInfo, ChumError>>> {

        match res {
            Ok(val) => if let Some(wr) = val {
                self.tx.send(Ok(wr))
            } else {
                Ok(()) /* no-op, like a read operation with an empty queue */
            },
            Err(e) => {
                self.tx.send(Err(e))
            }
        }

    }

    pub fn work(&mut self) {
        let mut rng = thread_rng();

        loop {
            /* Thread exits when it receives a signal over its channel. */

            let res = match self.ops.choose(&mut rng)
                .expect("choosing operation failed").as_ref() {

                "r" => self.backend.read(),
                "w" => self.backend.write(),
                "d" => self.backend.delete(),
                _ => panic!("unrecognized operator"),
            };

            match self.process_result(res) {
                Ok(_) => (),
                Err(_) => {
                    /*
                     * Stat thread exited, which we take to mean the workers
                     * should exit.
                     */
                    return
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
}
