/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Instant;

use crate::queue::{Queue, QueueItem};
use crate::worker::{WorkerInfo, WorkerTask, WorkerClient, DIR};
use crate::utils::ChumError;

use rusoto_s3::{S3Client, S3, DeleteObjectRequest};

pub const OP: &str = "delete";

pub struct Deleter {
    _target: String, /* unused until we implement WebDAV deletion */
    queue: Arc<Mutex<Queue>>
}

impl Deleter {
    pub fn new(target: String, queue: Arc<Mutex<Queue>>) -> Deleter {
        Deleter { _target: target, queue }
    }

    pub fn s3_delete(&self, client: &mut S3Client)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        let fname: String;
        {
            let mut q = self.queue.lock().unwrap();
            let qi = q.remove();
            if qi.is_none() {
                return Ok(None)
            }
            let qi = qi.unwrap();

            fname = qi.obj;
        }

        let dr = DeleteObjectRequest {
            bucket: DIR.to_string(),
            key: fname.clone(),
            ..Default::default()
        };

        let rtt_start = Instant::now();

        let res = client.delete_object(dr).sync();

        /*
         * Re-insert the object to make it available for future read or delete
         * operations if there was an error during the delete.
         */
        if let Err(e) = res {
            self.queue.lock().unwrap().insert(QueueItem{ obj: fname.clone() });

            return Err(Box::new(ChumError::new(&format!("Deleting {} \
                failed: {}", fname, e))))
        }

        let rtt = rtt_start.elapsed().as_millis();

        Ok(Some(WorkerInfo {
            id: thread::current().id(),
            op: self.get_type(),
            size: 0,
            ttfb: 0,
            rtt
        }))
    }
}

impl WorkerTask for &Deleter {
    fn work(&mut self, client: &mut WorkerClient)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        match client {
            WorkerClient::S3(s3) => self.s3_delete(s3),
            _ => Err(Box::new(ChumError::new("not implemented")))
        }
    }

    fn setup(&self, _client: &mut WorkerClient) -> Result<(), Box<dyn Error>> {
        Ok(())
    }

    fn get_type(&self) -> String { String::from(OP) }
}
