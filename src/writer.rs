/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate reqwest;
extern crate uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::fs::File;
use std::sync::{Arc, mpsc::Sender};
use std::thread;
use std::time;
use std::vec::Vec;

use crate::worker::WorkerResult;

use reqwest::{Client, Body, header::USER_AGENT};
use uuid::Uuid;

#[derive(Clone)]
pub struct Writer {
    concurrency: u32,
    tx: Sender<WorkerResult>,
    target: String,
    distr: Arc<Vec<u64>>,
    unit: String,
    pause: u64,
    client: Client,
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
            client: Client::new(),
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
            let body = Body::sized(f, fsize);

            let req = self.client.put(
                &format!("http://{}:80/{}/{}", self.target, dir, fname))
                .header(USER_AGENT, "chum/1.0")
                .body(body);

            /*
             * Ideally we could do something like req.send().expect(...), but
             * there appears to be a problem with the reqwest library and
             * various underlying channels closing unexpectedly. This is easily
             * reproduced by configuring chum to send 10k files.
             *
             * This may be a rust-on-illumos issue, or it may be a problem with
             * reqwest. Until we can root-cause, let's just swallow the errors
             * instead of panicking the thread.
             */
            if let Ok(resp) = req.send() {
                if !resp.status().is_success() {
                    println!("upload not successful: {}", resp.status());
                }
                self.tx.send(WorkerResult { id, size: osize }).unwrap();
            } else {
                println!("request builder failed, swallowing error.");
            }

            if self.pause > 0 {
                thread::sleep(time::Duration::from_millis(self.pause));
            }
        }
    }
}
