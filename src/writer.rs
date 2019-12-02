/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate curl;
extern crate uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::io::{Read, BufReader};
use std::sync::{Arc, mpsc::Sender};
use std::thread;
use std::time;
use std::vec::Vec;

use crate::worker::WorkerResult;

use curl::easy::Easy;
use uuid::Uuid;

#[derive(Clone)]
pub struct Writer {
    concurrency: u32,
    tx: Sender<WorkerResult>,
    target: String,
    distr: Arc<Vec<u64>>,
    unit: String,
    pause: u64,
}

impl Writer {
    pub fn new(concurrency: u32, tx: Sender<WorkerResult>, target: String,
        distr: Vec<u64>, unit: String, pause: u64) -> Writer {

        Writer {
            concurrency,
            tx,
            target,
            distr: Arc::new(distr),
            unit,
            pause,
        }
    }

    /*
     * Kicks off a bunch of worker threads to do the work. This is sync, so the
     * caller will block.
     */
    pub fn run(&self) {
        let mut worker_threads = Vec::new();

        for i in 0..self.concurrency {
            /*
             * The heaviest things to clone here shouldn't be too bad to clone:
             * - distr is an Arc, so this will not clone the entire vec.
             * - client is an Arc under the covers, so a new threadpool is not
             *   spun up.
             */
            let worker = self.clone();
            worker_threads.push(thread::spawn(move || worker.work(i)));
        }

        for thread in worker_threads {
            thread.join().expect("failed to join worker thread");
        }
    }

    /*
     * Upload files in a loop!
     */
    fn work(&self, id: u32) {
        let mut rng = thread_rng();
        let dir = "chum"; /* destination directory for uploads */
        let mut client = Easy::new();

        loop {
            /* This should be similar to how muskie generates objecids. */
            let fname = Uuid::new_v4();

            /* Randomly choose a file size from the list. */
            let osize = *self.distr.choose(&mut rng).unwrap();

            let fsize = match self.unit.as_ref() {
                "k" => osize * 1024,
                "m" => osize * 1024 * 1024,
                _ => osize * 1024, /* unknown data type - assume 'k.' */
            };

            /*
             * Obviously sub-optimal having this in the loop. Borrow-checker
             * struggle in the flesh.
             */
            let f = File::open("/dev/urandom").unwrap();
            let mut reader = BufReader::new(f);

            client.url(&format!(
                "http://{}:80/{}/{}", self.target, dir, fname)).unwrap();
            client.put(true).unwrap();
            client.upload(true).unwrap();
            client.in_filesize(fsize).unwrap();
            client.read_function(move |into| {
                /*
                 * Fill the buffer that curl hands us. In my tests this is
                 * always 64k. curl will make sure that the data it sends over
                 * the connection is limited by 'fsize,' since we set
                 * in_filesize previously.
                 */
                reader.read_exact(into).unwrap();
                Ok(into.len())
            }).unwrap();
            client.perform().unwrap();

            /*
             * We get a 201 when the file is new, and a 204 when a file
             * is overwritten.
             */
            if client.response_code().unwrap() == 201
                || client.response_code().unwrap() == 204 {
                self.tx.send(WorkerResult { id, size: osize }).unwrap();
            } else {
                println!("request failed: {}", client.response_code().unwrap());
            }

            if self.pause > 0 {
                thread::sleep(time::Duration::from_millis(self.pause));
            }
        }
    }
}
