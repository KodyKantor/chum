/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate curl;

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use curl::easy::Easy;
use crate::queue::{Queue};
use crate::worker::{WorkerResult, WorkerTask, DIR};

const OP: &str = "read";

pub struct Reader {
    target: String,
    queue: Arc<Mutex<Queue>>,
}

impl Reader {
    pub fn new(target: String, queue: Arc<Mutex<Queue>>) -> Reader {
        Reader { target, queue }
    }
}

impl WorkerTask for Reader {
    fn work(&mut self, client: &mut Easy)
        -> Result<Option<WorkerResult>, Box<dyn Error>> {

        /*
         * Create a scope here to ensure that we don't keep the queue locked
         * for longer than necessary.
         */
        {
            let qi = self.queue.lock().unwrap().get();
            if qi.is_none() {
                return Ok(None)
            }
            let qi = qi.unwrap();

            client.url(&format!("http://{}:80/{}/{}", self.target,
                DIR, qi.uuid))?;
        }
        client.get(true)?;

        let mut size = 0;
        {
            let mut transfer = client.transfer();
            transfer.write_function(|data| {
                size += data.len();
                Ok(data.len())
            })?;
            transfer.perform()?;
        }

        let code = client.response_code()?;
        if code == 200 {
            let ttfb = client.starttransfer_time()?.as_millis();
            let e2e = client.total_time()?.as_millis();
            return Ok(Some(WorkerResult {
                id: thread::current().id(),
                op: String::from(OP),
                size: size as u64,
                ttfb,
                e2e,
            }))
        } else {
            println!("request failed: {}", code);
        }
        Ok(None)
    }
}
