/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::sync::{Arc, Mutex};

use crate::worker::{WorkerInfo, Backend, DIR, Operation};
use crate::utils::ChumError;
use crate::queue::{Queue, QueueItem};

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::vec::Vec;
use std::thread;
use std::time::Instant;
use std::fs::File;
use std::io::{Write, Read, BufWriter};

use rand::Rng;
use rand::AsByteSliceMut;

use uuid::Uuid;

pub struct Fs {
    basedir: String,
    distr: Arc<Vec<u64>>,       /* object size distribution */
    queue: Arc<Mutex<Queue>>,
    buf: Vec<u8>,
}

impl Fs {
    pub fn new(basedir: String, distr: Vec<u64>, queue: Arc<Mutex<Queue>>)
        -> Fs {
        let mut rng = thread_rng();

        /*
         * Create a random buffer. This is the data that will be sent
         * to the target.
         */
        let mut buf = [0u8; 65536];
        rng.fill(&mut buf[..]);
        let arr = buf.as_byte_slice_mut();
        let mut vec: Vec<u8> = Vec::new();
        vec.extend_from_slice(arr);

        let fs = Fs {
            basedir,
            distr: Arc::new(distr),
            queue: Arc::clone(&queue),
            buf: vec,
        };

        fs.setup();

        fs
    }

    fn setup(&self) {
        let directory = &format!("/{}/{}/v2/{}",
                self.basedir, DIR, DIR);
        std::fs::create_dir_all(directory).expect("failed to create initial \
            directory layout");
    }
}

impl Backend for Fs {
    fn write(&self) -> Result<Option<WorkerInfo>, ChumError> {
        let fname = Uuid::new_v4();
        let mut rng = thread_rng();
        let size = *self.distr.choose(&mut rng)
            .expect("choosing file size failed");

        let first_two = &fname.to_string()[0..2];
        let directory = &format!("/{}/{}/v2/{}/{}",
                self.basedir, DIR, DIR, first_two);
        let full_path = &format!("{}/{}", directory, fname);

        std::fs::create_dir(directory)?;
        let file = File::create(full_path)?;

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
        bw.flush()?;

        match file.sync_all() {
            Err(e) => Err(ChumError::new(&format!("fsync failed: {}", e))),
            Ok(_) => {
                self.queue.lock().unwrap().insert(
                    QueueItem{ obj: fname.to_string() }
                );

                let rtt = rtt_start.elapsed().as_millis();
                Ok(Some(WorkerInfo {
                    id: thread::current().id(),
                    op: Operation::Write,
                    size,
                    ttfb: 0, /* not supported */
                    rtt,
                }))
            },
        }
    }

    fn read(&self) -> Result<Option<WorkerInfo>, ChumError> {
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

        let rtt_start = Instant::now();

        let first_two = &fname.to_string()[0..2];
        let directory = &format!("/{}/{}/v2/{}/{}",
                self.basedir, DIR, DIR, first_two);
        let full_path = &format!("{}/{}", directory, fname);
    
        let mut buf = Vec::new();
        let mut file =
            File::open(full_path)?;

        let size = file.read_to_end(&mut buf)?;

        let rtt = rtt_start.elapsed().as_millis();

        Ok(Some(WorkerInfo {
            id: thread::current().id(),
            op: Operation::Read,
            size: size as u64,
            ttfb: 0,
            rtt,
        }))
    }

    fn delete(&self) -> Result<Option<WorkerInfo>, ChumError> {
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

        let rtt_start = Instant::now();

        let first_two = &fname.to_string()[0..2];
        let directory = &format!("/{}/{}/v2/{}/{}",
                self.basedir, DIR, DIR, first_two);
        let full_path = &format!("{}/{}", directory, fname);

        let res = std::fs::remove_file(full_path);

        if let Err(e) = res {
            self.queue.lock().unwrap().insert(QueueItem{ obj: fname.clone() });

            return Err(ChumError::new(&format!("Deleting {} \
                failed: {}", fname, e)))
        }

        let rtt = rtt_start.elapsed().as_millis();

        Ok(Some(WorkerInfo {
            id: thread::current().id(),
            op: Operation::Delete,
            size: 0,
            ttfb: 0,
            rtt
        }))
    }
}
