/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

extern crate curl;

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::thread;

use crate::queue::{Queue};
use crate::worker::{WorkerInfo, WorkerTask, WorkerClient, DIR};
use crate::utils::ChumError;

use curl::easy::Easy;
use rusoto_s3::{S3Client, GetObjectRequest, S3};
use std::io::Read;

pub const OP: &str = "read";

pub struct Reader {
    target: String,
    queue: Arc<Mutex<Queue>>,
}

impl Reader {
    pub fn new(target: String, queue: Arc<Mutex<Queue>>) -> Reader {
        Reader { target, queue }
    }

    fn web_dav_download(&self, client: &mut Easy)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        let path: String;

        /*
         * Create a scope here to ensure that we don't keep the queue locked
         * for longer than necessary.
         */
        {
            let mut q = self.queue.lock().unwrap();
            let qi = q.get();
            if qi.is_none() {
                return Ok(None)
            }
            let qi = qi.unwrap();

            path = qi.obj.clone();
            client.url(&format!("http://{}:80/{}", self.target, path))?;
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
            let rtt = client.total_time()?.as_millis();
            Ok(Some(WorkerInfo {
                id: thread::current().id(),
                op: String::from(OP),
                size: size as u64,
                ttfb,
                rtt,
            }))
        } else {
            Err(Box::new(ChumError::new(
                &format!("Reading {} failed: {}", path, code))))
        }
    }

    fn s3_download(&self, client: &mut S3Client)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        /*
         * Create a scope here to ensure that we don't keep the queue locked
         * for longer than necessary.
         */
        let fname: String;
        {
            let mut q = self.queue.lock().unwrap();
            let qi = q.get();
            if qi.is_none() {
                return Ok(None)
            }
            let qi = qi.unwrap();

            fname = qi.obj.clone();
        }

        let gr = GetObjectRequest {
            bucket: DIR.to_string(),
            key: fname,
            ..Default::default()
        };

        let res = client.get_object(gr).sync()?;

        /*
         * Read the response buffer and throw it away. We don't care about the
         * data.
         */
        if res.body.is_some() {
            let mut stream = res.body.unwrap().into_blocking_read();
            let mut body = Vec::new();
            stream.read_to_end(&mut body).expect("failed to read response \
                body");
        }

        let size = res.content_length.expect("failed to get content-length");

        Ok(Some(WorkerInfo {
                id: thread::current().id(),
                op: String::from(OP),
                size: size as u64,
                ttfb: 0,
                rtt: 0,
        }))
    }
}

impl WorkerTask for &Reader {
    fn work(&mut self, client: &mut WorkerClient)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        match client {
            WorkerClient::WebDav(easy) => self.web_dav_download(easy),
            WorkerClient::S3(s3) => self.s3_download(s3),
        }
    }

    fn setup(&self, client: &mut WorkerClient)
        -> Result<(), Box<dyn Error>> {
 
        /* No setup required */
        match client {
            WorkerClient::WebDav(_) => Ok(()),
            WorkerClient::S3(_) => Ok(()),
        }
    }

    fn get_type(&self) -> String { String::from(OP) }
}
