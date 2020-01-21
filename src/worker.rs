/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::sync::{Arc, Mutex, mpsc::{Sender, Receiver, TryRecvError}};
use std::{thread, thread::ThreadId};
use std::time;
use std::error::Error;
use rand::prelude::*;

use curl::easy::Easy;

use crate::writer::Writer;
use crate::reader::Reader;
use crate::queue::Queue;

pub const DIR: &str = "chum";

#[derive(Debug)]
pub struct WorkerResult {
    pub id: ThreadId,
    pub op: String, /* e.g. 'read' or 'write' */
    pub size: u64, /* in bytes */
    pub ttfb: u128, /* millis */
    pub rtt: u128, /* millis */
}

/*
 * WorkerResults can be aggregated into WorkerStats.
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
    pub fn add_result(&mut self, res: &WorkerResult) {
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
    fn work(&mut self, client: &mut Easy)
        -> Result<Option<WorkerResult>, Box<dyn Error>>;
    fn get_type(&self) -> String;
}

pub struct Worker {
    writer: Writer,
    reader: Reader,
    client: Easy,
    tx: Sender<WorkerResult>,
    signal: Receiver<()>,
    pause: u64,
    ops: Vec<String>,
}

/*
 * A Worker is something that interacts with a target. It should emit events
 * in the form of a WorkerResult for every operation performed.
 *
 * A Worker calls out to WorkerTask implementors and throws their WorkerResult
 * into the tx mpsc to get picked up by a statistics listener.
 */
impl Worker {
    pub fn new(signal: Receiver<()>, tx: Sender<WorkerResult>, target: String,
        distr: Vec<u64>, pause: u64,
        queue: Arc<Mutex<Queue>>, ops: Vec<String>) -> Worker {

        let writer = Writer::new(target.clone(), distr,
            Arc::clone(&queue));
        let reader = Reader::new(target.clone(),
            Arc::clone(&queue));
        let mut client = Easy::new();
        client.forbid_reuse(true).unwrap();

        Worker {
            writer,
            reader,
            client,
            tx,
            signal,
            pause,
            ops,
        }
    }

    pub fn work(&mut self) {
        let mut rng = thread_rng();

        loop {
            /* Exit the thread when we receive a signal. */
            match self.signal.try_recv() {
                Ok(_) | Err(TryRecvError::Disconnected) => {
                    return
                },
                Err(TryRecvError::Empty) => (),
            }
            { /* Scope so 'operator' doesn't hold an immutable borrow. */
                let mut operator: Box<dyn WorkerTask> =
                    match self.ops.choose(&mut rng)
                    .expect("choosing operation failed").as_ref() {

                    /* XXX we should use something more elegant here. */
                    "r" => Box::new(&self.reader),
                    "w" => Box::new(&self.writer),
                    _ => panic!("unrecognized operator"),
                };

                match operator.work(&mut self.client) {
                    Ok(val) => if let Some(wr) = val {
                        match self.tx.send(wr) {
                            Ok(_) => (),
                            Err(e) => {
                                println!(
                                    "failed to send result: {}", e.to_string())
                            }
                        };
                    },
                    Err(e) => panic!("worker error: {}", e.to_string()),
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
