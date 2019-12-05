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
use std::sync::{Arc, mpsc::Sender};
use std::thread;
use std::time;
use std::vec::Vec;

use rand::Rng;
use rand::AsByteSliceMut;

use crate::worker::WorkerResult;

use curl::easy::Easy;
use uuid::Uuid;

#[derive(Clone)]
pub struct Writer {
    concurrency: u32,           /* number of threads to spin up */
    tx: Sender<WorkerResult>,   /* channel for progress reporting */
    target: String,             /* target ip address */
    distr: Arc<Vec<u64>>,       /* object size distribution */
    unit: String,               /* unit of measurement (kb/mb) */
    pause: u64,                 /* time to sleep between uploads */
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
             * The heaviest things to clone here shouldn't be too heavy since
             * we're using reference counters.
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

        /*
         * Create a random buffer. This is the data that will be sent
         * to the target server. We share this buffer among all the worker
         * threads using an Arc.
         */
        let mut buf = [0u8; 65536];
        rng.fill(&mut buf[..]);
        let arr = buf.as_byte_slice_mut();
        let mut vec: Vec<u8> = Vec::new();
        vec.extend_from_slice(arr);

        loop {
            /* This should be similar to how muskie generates objectids. */
            let fname = Uuid::new_v4();

            /* Randomly choose a file size from the list. */
            let osize = *self.distr.choose(&mut rng).unwrap();

            let fsize = match self.unit.as_ref() {
                "k" => osize * 1024,
                "m" => osize * 1024 * 1024,
                _ => osize * 1024, /* unknown data type - assume 'k.' */
            };

            client.url(&format!(
                "http://{}:80/{}/{}", self.target, dir, fname)).unwrap();
            client.put(true).unwrap();
            client.upload(true).unwrap();
            client.in_filesize(fsize).unwrap();

            /*
             * Make another scope here to make sure that 'transfer' won't be
             * able to use anything it borrows once the HTTP request ends.
             *
             * This also makes it such that we can re-use 'client' as mutable
             * after this scope ends, like to get the response status code.
             *
             * We don't currently borrow anything and use it again later, but
             * this might make future-me less frustrated.
             */
            {
                let mut transfer = client.transfer();
                transfer.read_function(|into| {
                    /* This should be memcpy, thus pretty fast. */
                    into.copy_from_slice(&vec);
                    Ok(into.len())
                }).unwrap();
                transfer.perform().unwrap();
            }

            /*
             * We get a 201 when the file is new, and a 204 when a file
             * is overwritten. Everything else is unexpected.
             */
            let code = client.response_code().unwrap();
            if code == 201 || code == 204 {
                /*
                 * XXX want to use .as_secs_f64() or similar once we can move
                 * to rust 1.38+
                 */
                let ttfb = client.starttransfer_time().unwrap().as_millis();
                let e2e = client.total_time().unwrap().as_millis();
                self.tx.send(WorkerResult {
                    id,
                    size: osize,
                    ttfb,
                    e2e,
                }).unwrap();
            } else {
                println!("request failed: {}", code);
            }

            if self.pause > 0 {
                thread::sleep(time::Duration::from_millis(self.pause));
            }
        }
    }
}
