/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

extern crate uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::AsByteSliceMut;
use rand::Rng;

use std::env;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use std::vec::Vec;

use rusoto_core::{Region, RusotoError};
use rusoto_credential::EnvironmentProvider;
use rusoto_s3::{
    CreateBucketRequest, DeleteObjectRequest, GetObjectRequest,
    PutObjectRequest, S3Client, S3 as S3Trait,
};

use uuid::Uuid;

use crate::utils::ChumError;
use crate::worker::{Backend, Operation, WorkerInfo, WorkerOptions, DIR};

pub struct S3 {
    buf: Vec<u8>,
    client: S3Client,
    wopts: WorkerOptions,
}

impl S3 {
    pub fn new(wopts: WorkerOptions) -> S3 {
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

        /*
         * Users may supply access keys in environment variables. We use
         * the minio defaults if keys are not provided.
         */
        if env::var("AWS_ACCESS_KEY_ID").is_err() {
            env::set_var("AWS_ACCESS_KEY_ID", "minioadmin");
        }
        if env::var("AWS_SECRET_ACCESS_KEY").is_err() {
            env::set_var("AWS_SECRET_ACCESS_KEY", "minioadmin");
        }

        let region = Region::Custom {
            name: "chum-s3".to_owned(),
            endpoint: format!("http://{}", wopts.target),
        };

        let client: S3Client = S3Client::new_with(
            rusoto_core::request::HttpClient::new()
                .expect("failed to create S3 HTTP client"),
            EnvironmentProvider::default(),
            region,
        );

        let mut s3 = S3 {
            buf: vec,
            client,
            wopts,
        };

        s3.setup();

        s3
    }

    fn setup(&mut self) {
        let cbr = CreateBucketRequest {
            bucket: DIR.to_string(),
            ..Default::default()
        };

        if let Err(e) = self.client.create_bucket(cbr).sync() {
            match e {
                RusotoError::Service(_) => { /* bucket already created */ }
                _ => panic!("Creating bucket failed: {}", e),
            }
        };
    }

    fn get_path(&self, fname: String) -> PathBuf {
        let first_two = &fname[0..2];
        Path::new(&format!("v2/{}/{}/{}", DIR, first_two, fname)).to_path_buf()
    }
}

impl Backend for S3 {
    fn write(&mut self) -> Result<Option<WorkerInfo>, ChumError> {
        /* This should be similar to how muskie generates objectids. */
        let fname = Uuid::new_v4();

        let mut rng = thread_rng();
        let size = *self
            .wopts
            .distribution
            .choose(&mut rng)
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

        let full_path = self.get_path(fname.to_string());

        let pr = PutObjectRequest {
            bucket: DIR.to_string(),
            key: full_path.to_str().unwrap().to_string(),
            body: Some(buf.into()),
            ..Default::default()
        };

        let rtt_start = Instant::now();

        /*
         * For the moment we don't have latency stats for S3 requests. Maybe
         * we could grab these from the underlying reqwest structures. Or maybe
         * not.
         */
        match self.client.put_object(pr).sync() {
            Err(e) => Err(ChumError::new(&e.to_string())),
            Ok(_) => {
                if self.wopts.read_queue {
                    self.wopts.queue.lock().unwrap().insert(fname.to_string());
                }

                let rtt = rtt_start.elapsed().as_millis();
                Ok(Some(WorkerInfo {
                    id: thread::current().id(),
                    op: Operation::Write,
                    size,
                    ttfb: 0, /* not supported */
                    rtt,
                }))
            }
        }
    }

    fn read(&mut self) -> Result<Option<WorkerInfo>, ChumError> {
        /*
         * Create a scope here to ensure that we don't keep the queue locked
         * for longer than necessary.
         */
        let full_path: String;
        {
            let mut q = self.wopts.queue.lock().unwrap();
            let qi = q.get();
            if qi.is_none() {
                return Ok(None);
            }
            let qi = qi.unwrap();

            let fname = qi.clone();
            full_path = self.get_path(fname).to_str().unwrap().to_string();
        }

        let gr = GetObjectRequest {
            bucket: DIR.to_string(),
            key: full_path.clone(),
            ..Default::default()
        };

        let rtt_start = Instant::now();
        let res = match self.client.get_object(gr).sync() {
            Err(e) => Err(ChumError::new(&format!(
                "failed to read {}: {}",
                full_path, e
            ))),
            Ok(res) => Ok(res),
        }?;

        /*
         * Read the response buffer and throw it away. We don't care about the
         * data.
         */
        if res.body.is_some() {
            let mut stream = res.body.unwrap().into_blocking_read();
            let mut body = Vec::new();
            stream.read_to_end(&mut body).expect(
                "failed to read response \
                 body",
            );
        }

        let size = res.content_length.expect("failed to get content-length");
        let rtt = rtt_start.elapsed().as_millis();

        Ok(Some(WorkerInfo {
            id: thread::current().id(),
            op: Operation::Read,
            size: size as u64,
            ttfb: 0,
            rtt,
        }))
    }

    fn delete(&mut self) -> Result<Option<WorkerInfo>, ChumError> {
        let full_path: String;
        let fname: String;
        {
            let mut q = self.wopts.queue.lock().unwrap();
            let qi = q.remove();
            if qi.is_none() {
                return Ok(None);
            }
            fname = qi.unwrap();

            full_path = self
                .get_path(fname.to_string())
                .to_str()
                .unwrap()
                .to_string();
        }

        let dr = DeleteObjectRequest {
            bucket: DIR.to_string(),
            key: full_path.clone(),
            ..Default::default()
        };

        let rtt_start = Instant::now();

        let res = self.client.delete_object(dr).sync();

        /*
         * Re-insert the object to make it available for future read or delete
         * operations if there was an error during the delete.
         */
        if let Err(e) = res {
            self.wopts.queue.lock().unwrap().insert(fname);

            return Err(ChumError::new(&format!(
                "Deleting {} failed: {}",
                full_path, e
            )));
        }

        let rtt = rtt_start.elapsed().as_millis();

        Ok(Some(WorkerInfo {
            id: thread::current().id(),
            op: Operation::Delete,
            size: 0,
            ttfb: 0,
            rtt,
        }))
    }
}
