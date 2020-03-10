/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

extern crate getopts;

mod writer;
mod reader;
mod deleter;
mod worker;
mod queue;
mod utils;

use std::env;
use std::{thread, thread::JoinHandle};
use std::vec::Vec;
use std::sync::{Arc, Mutex, mpsc::channel, mpsc::Sender};
use std::error::Error;

use crate::queue::{Queue, QueueMode};
use crate::worker::Worker;
use crate::utils::*;

use getopts::Options;

/* Default values. */
const DEF_CONCURRENCY: u32 = 1;
const DEF_SLEEP: u64 = 0;
const DEF_DISTR: &str = "128k,256k,512k";
const DEF_INTERVAL: u64 = 2;
const DEF_QUEUE_MODE: QueueMode = QueueMode::Rand;
const DEF_QUEUE_CAP: usize = 1_000_000;
const DEF_WORKLOAD: &str = "r,w";
const DEF_OUTPUT_FORMAT: &str = "h";
const DEF_DATA_CAP: &str = "0";

fn usage(opts: Options, msg: &str) {
    let synopsis = "\
        Upload files to a given WebDAV server as quickly as possible";

    let usg = format!("chum - {}", synopsis);
    println!("{}", opts.usage(&usg));
    println!("{}", msg);
}

fn main() -> Result<(), Box<dyn Error>> {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.reqopt("t",
                "target",
                "target server",
                "[s3|webdav|fs]:IP|PATH");

    opts.optopt("c",
                "concurrency",
                &format!("number of concurrent threads, \
                    default: {}", DEF_CONCURRENCY),
                "NUM");
    opts.optopt("s",
                "sleep",
                &format!("sleep duration in millis between each \
                    upload, default: {}", DEF_SLEEP),
                "NUM");
    opts.optopt("d",
                "distribution",
                &format!("comma-separated distribution of \
                    file sizes to upload, default: {}", DEF_DISTR),
                "NUM:COUNT,NUM:COUNT,...");
    opts.optopt("i",
                "interval",
                &format!("interval in seconds at which to \
                    report stats, default: {}", DEF_INTERVAL),
                "NUM");
    opts.optopt("q",
                "queue-mode",
                &format!("queue mode for read operations, default: {}",
                    DEF_QUEUE_MODE),
                &format!("{}|{}|{}",
                    QueueMode::Lru, QueueMode::Mru, QueueMode::Rand));
    opts.optopt("w",
                "workload",
                &format!("workload of operations, default: {}",
                    DEF_WORKLOAD),
                "OP:COUNT,OP:COUNT");
    opts.optopt("f",
                 "format",
                 &format!("statistics output format, default: {}",
                    DEF_OUTPUT_FORMAT),
                "h|v|t");
    opts.optopt("m",
                "max-data",
                &format!("maximum amount of data to write to the target, \
                    default: {}, '0' disables cap", DEF_DATA_CAP),
                "CAP");
    opts.optopt("r",
                "read-list",
                "path to a file listing files to read from server, \
                    default: none (files are chosen from recent uploads)",
                "FILE");

    opts.optflag("h",
                 "help",
                 "print this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            usage(opts, &f.to_string());
            return Ok(())
        },
    };

    if matches.opt_present("h") {
        usage(opts, "");
        return Ok(());
    }

    /* Handle grabbing defaults if the user didn't provide these flags. */
    let conc = matches.opt_get_default("concurrency", DEF_CONCURRENCY)?;
    let sleep = matches.opt_get_default("sleep", DEF_SLEEP)?;
    let interval =
        matches.opt_get_default("interval", DEF_INTERVAL)?;
    let qmode = matches.opt_get_default("queue-mode", DEF_QUEUE_MODE)?;
    let format = matches.opt_get_default("format",
        String::from(DEF_OUTPUT_FORMAT))?;

    let format = match format.as_str() {
        "h" => OutputFormat::Human,
        "v" => OutputFormat::HumanVerbose,
        "t" => OutputFormat::Tabular,
        _ => {
            usage(opts, &format!("invalid output format '{}'", format));
            return Ok(())
        },
    };

    let ops = if matches.opt_present("workload") {
        matches.opt_str("workload").unwrap()
    } else {
        String::from(DEF_WORKLOAD)
    };
    let ops = expand_distribution(&ops)?;

    let q = Arc::new(Mutex::new(Queue::new(qmode, DEF_QUEUE_CAP)));

    if matches.opt_present("read-list") {
        let readlist = matches.opt_str("read-list").unwrap();
        match populate_queue(q.clone(), readlist) {
            Ok(_) => (),
            Err(e) => {
                usage(opts, &e.to_string());
                return Ok(())
            }
        }
    }

    let target = matches.opt_str("target").unwrap();

    /*
     * Parse the user's size distribution if one was provided, otherwise use
     * our default distr.
     */
    let user_distr = if matches.opt_present("distribution") {
        matches.opt_str("distribution").unwrap()
    } else {
        String::from(DEF_DISTR)
    };

    let distr = match convert_numeric_distribution(
        expand_distribution(&user_distr)?) {
        Ok(d) => d,
        Err(e) => {
            usage(opts, &format!("invalid distribution argument '{}': {}",
                user_distr, e.to_string()));
            return Ok(())
        },
    };

    if conc < 1 {
        usage(opts, "concurrency must be > 1");
        return Ok(())
    }

    let data_cap = parse_human(
        &matches.opt_get_default("max-data", String::from(DEF_DATA_CAP))?)?;

    /*
     * Start the real work. Kick off worker threads and a stat listener.
     */

    let (tx, rx) = channel();

    let mut worker_threads: Vec<JoinHandle<_>> = Vec::new();
    let mut worker_chan: Vec<Sender<_>> = Vec::new();
    for _ in 0..conc {
        let (sender, signal) = channel();
        worker_chan.push(sender);
        let mut worker = Worker::new(signal, tx.clone(), target.clone(),
            distr.clone(), sleep, q.clone(), ops.clone());
        worker_threads.push(thread::spawn(move || { worker.work(); }));
    }

    /* Kick off statistics collection and reporting. */
    let stat_thread = thread::spawn(move || {
        collect_stats(rx, interval, format, data_cap);
    });

    /*
     * When the stat thread exits we know that enough data was written.
     */
    stat_thread.join().expect("failed to join stat thread");

    for sender in worker_chan {
        match sender.send(()) {
            Ok(_) => (),
            Err(e) => println!("problem sending stop signal: {}",
                e.to_string()),
        };
    }

    for hdl in worker_threads {
        hdl.join().expect("failed to join worker thread");
    }

    Ok(())
}
