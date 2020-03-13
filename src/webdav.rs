/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use crate::worker::{Backend, DIR, WorkerInfo, Operation};
use crate::utils::ChumError;
use crate::queue::{Queue, QueueItem};

use curl::easy::Easy;
use uuid::Uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::Rng;
use rand::AsByteSliceMut;

use std::vec::Vec;
use std::thread;
use std::sync::{Arc, Mutex};

pub struct WebDav {
    target: String,             /* target ip address */
    distr: Arc<Vec<u64>>,       /* object size distribution */
    queue: Arc<Mutex<Queue>>,
    buf: Vec<u8>,
}

impl WebDav {
    pub fn new(target: String, distr: Vec<u64>, queue: Arc<Mutex<Queue>>)
        -> WebDav {

        let mut rng = thread_rng();

        /*
         * Create a random buffer. This is the data that will be sent
         * to the target server.
         */
        let mut buf = [0u8; 65536];
        rng.fill(&mut buf[..]);
        let arr = buf.as_byte_slice_mut();
        let mut vec: Vec<u8> = Vec::new();
        vec.extend_from_slice(arr);

        WebDav {
            target,
            distr: Arc::new(distr),
            queue: Arc::clone(&queue),
            buf: vec,
        }
    }
}

impl Backend for WebDav {

    fn write(&self) -> Result<Option<WorkerInfo>, ChumError> {
        /*
         * XXX it would be great if we could re-use our client, but libcurl
         * has some weird internal mutability.
         */
        let mut client = Easy::new();

        let mut rng = thread_rng();

        /* This should be similar to how muskie generates objectids. */
        let fname = Uuid::new_v4();

        /* Randomly choose a file size from the list. */
        let size = *self.distr.choose(&mut rng)
            .expect("choosing file size failed");

        let path = format!("{}/{}", DIR, fname);

        client.url(&format!(
            "http://{}:80/{}/{}", self.target, DIR, fname))?;
        client.put(true)?;
        client.upload(true)?;
        client.in_filesize(size)?;

        /*
         * Make another scope here to make sure that 'transfer' won't be
         * able to use anything it borrows once the HTTP request ends.
         *
         * This also allows us to re-use 'client' as mutable
         * after this scope ends, like to get the response status code.
         *
         * We don't currently borrow anything and use it again later, but
         * this might make future-me less frustrated.
         */
        {
            let mut transfer = client.transfer();
            transfer.read_function(|into| {
                /* This should be memcpy, thus pretty fast. */
                into.copy_from_slice(&self.buf);
                Ok(into.len())
            })?;
            transfer.perform()?;
        }

        /*
         * We get a 201 when the file is new, and a 204 when a file
         * is overwritten. Everything else is unexpected.
         */
        let code = client.response_code()?;
        if code == 201 || code == 204 {
            /*
             * XXX want to use .as_secs_f64() or similar once we can move
             * to rust 1.38+
             */
            let ttfb = client.starttransfer_time().unwrap().as_millis();
            let rtt = client.total_time().unwrap().as_millis();

            self.queue.lock().unwrap().insert(QueueItem{ obj: path });
            Ok(Some(WorkerInfo {
                id: thread::current().id(),
                op: Operation::Write,
                size,
                ttfb,
                rtt,
            }))

        } else {
            Err(ChumError::new(
                &format!("Writing {} failed: {}", path, code)))
        }
    }
    fn read(&self) -> Result<Option<WorkerInfo>, ChumError> {

        let mut client = Easy::new();
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
                op: Operation::Read,
                size: size as u64,
                ttfb,
                rtt,
            }))
        } else {
            Err(ChumError::new(
                &format!("Reading {} failed: {}", path, code)))
        }
    }
    fn delete(&self) -> Result<Option<WorkerInfo>, ChumError> {
        Err(ChumError::new("'delete' not implemented for WebDAV backends."))
    }
}
