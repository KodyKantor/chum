/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

use std::sync::{Arc, Mutex, mpsc::Sender};
use std::{thread, thread::ThreadId};
use std::time;
use std::error::Error;

use curl::easy::Easy;

use crate::writer::Writer;
use crate::reader::Reader;
use crate::queue::Queue;

pub const DIR: &str = "chum";

pub struct WorkerResult {
    pub id: ThreadId,
    pub op: String, /* e.g. 'read' or 'write' */
    pub size: u64, /* in bytes */
    pub ttfb: u128, /* millis */
    pub e2e: u128, /* millis */
}

/*
 * WorkerResults can be aggregated into WorkerStats.
 */
pub struct WorkerStat {
    pub objs: u64,
    pub data: u64,
    pub ttfb: u128,
    pub e2e: u128,
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
            e2e: 0,
        }
    }
    pub fn add_result(&mut self, res: &WorkerResult) {
        self.objs += 1;
        self.data += res.size;
        self.ttfb += res.ttfb;
        self.e2e += res.e2e;
    }

    pub fn clear(&mut self) {
        self.objs = 0;
        self.data = 0;
        self.ttfb = 0;
        self.e2e = 0;
    }

    /* For easy printing when the caller doesn't care about time. */
    pub fn serialize_relative(&mut self) -> String {
        format!("{} objects, {}, avg ttfb {}ms, avg e2e {}ms", self.objs,
            bytes_to_human(self.data), self.ttfb / u128::from(self.objs),
            self.e2e / u128::from(self.objs))
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
}

pub struct Worker {
    writer: Writer,
    reader: Reader,
    client: Easy,
    tx: Sender<WorkerResult>,
    pause: u64,
}

/*
 * A Worker is something that interacts with a target. It should emit events
 * in the form of a WorkerResult for every operation performed.
 *
 * A Worker calls out to WorkerTask implementors and throws their WorkerResult
 * into the tx mpsc to get picked up by a statistics listener.
 */
impl Worker {
    pub fn new(tx: Sender<WorkerResult>, target: String,
        distr: Vec<u64>, unit: String, pause: u64,
        queue: Arc<Mutex<Queue>>) -> Worker {

        let writer = Writer::new(target.clone(), distr, unit,
            Arc::clone(&queue));
        let reader = Reader::new(target.clone(),
            Arc::clone(&queue));

        Worker {
            writer,
            reader,
            client: Easy::new(),
            tx,
            pause,
        }
    }

    pub fn work(&mut self) {
        loop {
            /* For now just unwrap the errors */
            let res = self.writer.work(&mut self.client).unwrap();
            if res.is_some() {
                self.tx.send(res.unwrap()).unwrap();
            }
            self.sleep();

            let res = self.reader.work(&mut self.client).unwrap();
            if res.is_some() {
                self.tx.send(res.unwrap()).unwrap();
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
