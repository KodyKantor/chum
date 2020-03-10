/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

extern crate curl;
extern crate uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::sync::{Arc, Mutex};
use std::vec::Vec;
use std::error::Error;
use std::thread;
use std::time::Instant;
use std::fs::File;
use std::io::{Write, BufWriter};

use rand::Rng;
use rand::AsByteSliceMut;

use crate::worker::{WorkerInfo, WorkerTask, WorkerClient, FsClient, DIR};
use crate::queue::{Queue, QueueItem};
use crate::utils::ChumError;

use curl::easy::Easy;
use rusoto_core::RusotoError;
use rusoto_s3::{S3Client, PutObjectRequest, CreateBucketRequest, S3};

use uuid::Uuid;

pub const OP: &str = "write";

pub struct Writer {
    target: String,             /* target ip address */
    distr: Arc<Vec<u64>>,       /* object size distribution */
    queue: Arc<Mutex<Queue>>,
    buf: Vec<u8>,
}

impl Writer {
    pub fn new(target: String, distr: Vec<u64>,
        queue: Arc<Mutex<Queue>>) -> Writer {

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

        Writer {
            target,
            distr: Arc::new(distr),
            queue: Arc::clone(&queue),
            buf: vec,
        }
    }

    fn web_dav_upload(&self, client: &mut Easy)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

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
                op: String::from(OP),
                size,
                ttfb,
                rtt,
            }))

        } else {
            Err(ChumError::new(
                &format!("Writing {} failed: {}", path, code)).into())
        }
    }

    /*
     * Upload an object to the remote endpoint.
     */
    fn s3_upload(&self, client: &mut S3Client)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        /* This should be similar to how muskie generates objectids. */
        let fname = Uuid::new_v4();

        let mut rng = thread_rng();
        let size = *self.distr.choose(&mut rng)
            .expect("choosing file size failed");

        /*
         * The S3 client library that we're using doesn't have simply
         * sync-friendly buffered IO support. Here we just create one giant
         * buffer to send along.
         */
        let mut buf: Vec<u8> = Vec::with_capacity(size as usize);
        let mut bytes_to_go = size;
        while bytes_to_go > 0 {
            if bytes_to_go < self.buf.len() as u64 {
                let tail = &self.buf[0..(bytes_to_go - 1) as usize];
                buf.extend(tail);
                break;
            }
            buf.extend(&self.buf);
            bytes_to_go -= self.buf.len() as u64;
        }

        let pr = PutObjectRequest {
            bucket: DIR.to_string(),
            key: fname.to_string(),
            body: Some(buf.into()),
            ..Default::default()
        };

        let rtt_start = Instant::now();

        /*
         * For the moment we don't have latency stats for S3 requests. Maybe
         * we could grab these from the underlying reqwest structures. Or maybe
         * not.
         */
        match client.put_object(pr).sync() {
            Err(e) => Err(Box::new(e)),
            Ok(_) => {
                self.queue.lock().unwrap().insert(
                    QueueItem{ obj: fname.to_string() }
                );

                let rtt = rtt_start.elapsed().as_millis();
                Ok(Some(WorkerInfo {
                    id: thread::current().id(),
                    op: String::from(OP),
                    size,
                    ttfb: 0, /* not supported */
                    rtt,
                }))
            },
        }
    }

    /* Write a file locally. */
    fn fs_write(&self, fs: &FsClient)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {
        let fname = Uuid::new_v4();
        let mut rng = thread_rng();
        let size = *self.distr.choose(&mut rng)
            .expect("choosing file size failed");

        let file = File::create(&format!("/{}/{}/{}", fs.basedir, DIR, fname))?;
        let mut bw = BufWriter::new(&file);

        let mut buf: Vec<u8> = Vec::with_capacity(size as usize);
        let mut bytes_to_go = size;
        while bytes_to_go > 0 {
            if bytes_to_go < self.buf.len() as u64 {
                let tail = &self.buf[0..(bytes_to_go - 1) as usize];
                buf.extend(tail);
                break;
            }
            buf.extend(&self.buf);
            bytes_to_go -= self.buf.len() as u64;
        }

        let rtt_start = Instant::now();

        /*
         * Write the data to the file and then issue an fsync. fsync is
         * VERY important. I shouldn't have to say that, but many storage
         * systems in the real world do not perform synchronous IO because
         * the implementors feel that speed is more important than durability.
         *
         * Durability is a constraint, not a feature!
         */
        bw.write_all(&buf)?;

        match file.sync_all() {
            Err(e) => Err(e.into()),
            Ok(_) => {
                self.queue.lock().unwrap().insert(
                    QueueItem{ obj: fname.to_string() }
                );

                let rtt = rtt_start.elapsed().as_millis();
                Ok(Some(WorkerInfo {
                    id: thread::current().id(),
                    op: String::from(OP),
                    size,
                    ttfb: 0, /* not supported */
                    rtt,
                }))
            },
        }
    }

    /*
     * Initial setup of S3 environment.
     */
    fn s3_setup(&self, client: &mut S3Client) -> Result<(), Box<dyn Error>> {

        let cbr = CreateBucketRequest {
            bucket: DIR.to_string(),
            ..Default::default()
        };

        /* XXX refactor? */
        match client.create_bucket(cbr).sync() {
            Err(e) => match e {
                RusotoError::Service(_) => {
                    /* bucket already created */
                    Ok(())
                },
                _ => Err(Box::new(ChumError::new(&format!("Creating bucket \
                    failed: {}", e)))),
            },
            Ok(_) => Ok(())
        }?;

        Ok(())
    }

    fn fs_setup(&self, fs: &FsClient) -> Result<(), Box<dyn Error>> {
        match std::fs::create_dir_all(&format!("/{}/{}", fs.basedir, DIR)) {
            Err(_) => {
                Err(ChumError::new("Creating leading paths failed").into())
            }
            Ok(_) => Ok(())
        }
    }
}

impl WorkerTask for &Writer {
    fn work(&mut self, client: &mut WorkerClient)
        -> Result<Option<WorkerInfo>, Box<dyn Error>> {

        match client {
            WorkerClient::WebDav(easy) => self.web_dav_upload(easy),
            WorkerClient::S3(s3) => self.s3_upload(s3),
            WorkerClient::Fs(fs) => self.fs_write(fs),
        }

    }

    fn setup(&self, client: &mut WorkerClient)
        -> Result<(), Box<dyn Error>> {

        match client {
            WorkerClient::S3(s3) => self.s3_setup(s3),
            WorkerClient::Fs(fs) => self.fs_setup(fs),
            _ => Ok(()),
        }
    }

    fn get_type(&self) -> String { String::from(OP) }
}
