/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use crate::state::State;
use crate::utils::ChumError;
use crate::worker::*;

use rand::seq::SliceRandom;
use rand::thread_rng;
use rand::AsByteSliceMut;
use rand::Rng;

use chrono::{DateTime, Datelike, Utc};

use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::thread;
use std::time::Instant;
use std::vec::Vec;

use uuid::Uuid;

const DEF_MAX_DIRENTS: u64 = 100_000;

pub struct Fs {
    buf: Vec<u8>,
    obj_cnt_dir: u64,
    dir_shard: u32,
    wopts: WorkerOptions,
}

impl Fs {
    pub fn new(wopts: WorkerOptions) -> Fs {
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

        Fs {
            buf: vec,
            obj_cnt_dir: 0,
            dir_shard: 0,
            wopts,
        }
    }

    /* Common function to handle creating filesystem path. */
    fn get_path(&mut self, fname: String) -> PathBuf {
        let today = Utc::today();
        self.obj_cnt_dir += 1;
        if self.obj_cnt_dir > DEF_MAX_DIRENTS {
            self.obj_cnt_dir = 0;
            self.dir_shard += 1;
        }
        Path::new(&format!(
            "{}/{}/{}{}/{}/{}",
            self.wopts.target,
            today.year(),
            today.month(),
            today.day(),
            self.dir_shard,
            fname
        ))
        .to_path_buf()
    }

    #[allow(clippy::single_match)]
    fn send_state(
        &self,
        state: &str,
        begin: DateTime<Utc>,
        end: DateTime<Utc>,
    ) {
        if let Some(c) = &self.wopts.debug_tx {
            match c.send(State {
                host: format!("{:?}", thread::current().id()),
                state: state.to_owned(),
                start_time: begin,
                end_time: end,
            }) {
                Ok(_) => (),
                Err(_) => (),
            }
        }
    }
}

impl Backend for Fs {
    fn write(&mut self) -> Result<Option<WorkerInfo>, ChumError> {
        let fname = Uuid::new_v4();
        let mut rng = thread_rng();
        let size = *self
            .wopts
            .distribution
            .choose(&mut rng)
            .expect("choosing file size failed");

        let full_path = self.get_path(fname.to_string());
        let mut begin: DateTime<Utc>;
        let mut end: DateTime<Utc>;

        begin = Utc::now();
        let rtt_start = Instant::now();
        if let Err(_e) = std::fs::create_dir_all(
            &full_path.parent().expect("couldn't retrieve parent dir"),
        ) {

            /*
             * One of three cases:
             * - lack permission to create directory
             * - parent path doesn't exist (should be handled by Fs::setup)
             * - directory already exists (common case)
             *
             * Unfortunately we don't get a real error type we can parse to see
             * what the error was, so we just do nothing here.
             */
        }
        end = Utc::now();
        self.send_state("write::mkdir", begin, end);

        begin = Utc::now();
        let file = File::create(full_path)?;
        end = Utc::now();
        self.send_state("write::open", begin, end);

        /*
         * Add fallocate support?
         *
         * #[cfg(target_os = "linux")]
         * {
         *     let ret = unsafe {
         *         libc::fallocate(
         *             file.as_raw_fd(), libc::FALLOC_FL_KEEP_SIZE, 0, size);
         *     };
         * }
         */

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

        /*
         * Write the data to the file and then optionally issue an fsync.
         *
         * Durability is a constraint, not a feature, at least in this
         * implementor's opinion.
         */
        begin = Utc::now();
        bw.write_all(&buf)?;
        bw.flush()?;
        end = Utc::now();
        self.send_state("write::write", begin, end);

        if self.wopts.sync {
            begin = Utc::now();
            match file.sync_all() {
                Err(e) => Err(ChumError::new(&format!("fsync failed: {}", e))),
                Ok(_) => {
                    if self.wopts.read_queue {
                        self.wopts
                            .queue
                            .lock()
                            .unwrap()
                            .insert(fname.to_string());
                    }

                    let rtt = rtt_start.elapsed().as_millis();
                    end = Utc::now();
                    self.send_state("write::fsync", begin, end);

                    Ok(Some(WorkerInfo {
                        id: thread::current().id(),
                        op: Operation::Write,
                        size,
                        ttfb: 0, /* not supported */
                        rtt,
                    }))
                }
            }
        } else {
            self.wopts.queue.lock().unwrap().insert(fname.to_string());

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

    fn read(&mut self) -> Result<Option<WorkerInfo>, ChumError> {
        let fname: String;
        {
            let mut q = self.wopts.queue.lock().unwrap();
            let qi = q.get();
            if qi.is_none() {
                return Ok(None);
            }
            let qi = qi.unwrap();

            fname = qi.clone();
        }

        let mut begin: DateTime<Utc>;
        let mut end: DateTime<Utc>;

        let rtt_start = Instant::now();

        let full_path = self.get_path(fname);

        let mut buf = Vec::new();
        begin = Utc::now();
        let mut file = File::open(full_path)?;
        end = Utc::now();
        self.send_state("read::open", begin, end);

        begin = Utc::now();
        let size = file.read_to_end(&mut buf)?;
        end = Utc::now();
        self.send_state("read::read", begin, end);

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
        let fname: String;
        {
            let mut q = self.wopts.queue.lock().unwrap();
            let qi = q.remove();
            if qi.is_none() {
                return Ok(None);
            }
            fname = qi.unwrap();
        }
        let begin: DateTime<Utc>;
        let end: DateTime<Utc>;

        begin = Utc::now();
        let rtt_start = Instant::now();

        let full_path = self.get_path(fname.to_string());

        let res = std::fs::remove_file(full_path);
        end = Utc::now();
        self.send_state("delete::rm", begin, end);

        if let Err(e) = res {
            self.wopts.queue.lock().unwrap().insert(fname.clone());

            return Err(ChumError::new(&format!(
                "Deleting {} \
                 failed: {}",
                fname, e
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
