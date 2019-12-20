/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate getopts;

mod writer;
mod reader;
mod worker;
mod queue;

use std::env;
use std::{thread, thread::JoinHandle};
use std::time;
use std::time::SystemTime;
use std::vec::Vec;
use std::sync::mpsc::{channel, Receiver};
use std::sync::{Arc, Mutex};
use std::collections::HashMap;

use crate::queue::{Queue, QueueMode};
use crate::worker::{Worker, WorkerResult, WorkerStat};

use getopts::Options;

/* Default values. */
const DEF_CONCURRENCY: u32 = 1;
const DEF_SLEEP: u64 = 0;
const DEF_DISTR: &str = "128,256,512";
const DEF_UNIT: &str = "k";
const DEF_INTERVAL: u64 = 2;
const DEF_QUEUE_MODE: &str = "rand";
const DEF_QUEUE_CAP: usize = 1000;
const DEF_WORKLOAD: &str = "r,w";

/*
 * This thread reads results off of the channel. This tracks three sets of
 * stats:
 * - long term aggregate statistics
 * - per tick aggregate statistics
 * - per thread-tick statistics
 *
 * Long term aggregated stats are the stats for the entire program's
 * duration. The throughput stats from each thread are aggregated and added
 * to create a total.
 *
 * Per tick aggregated stats represent the throughput of all of the threads
 * in aggregate for the last 'tick.'
 *
 * Per thread-tick stats represent the throughput of each individual thread
 * for the last tick. This is only printed when the user provides the '-v'
 * flag at the CLI.
 *
 * All stats are separated by operation (e.g. read, write, etc.).
 */
fn collect_stats(rx: Receiver<WorkerResult>, interval: u64, verbose: bool) {

    let mut op_agg = HashMap::new();
    let start_time = SystemTime::now();

    loop {
        thread::sleep(time::Duration::from_secs(interval));

        let mut op_ticks = HashMap::new();
        let mut op_stats = HashMap::new();

        /*
         * Catch up with the results that worker threads sent while this
         * thread was sleeping.
         */
        for res in rx.try_iter() {
            if !op_stats.contains_key(&res.op) {
                op_stats.insert(res.op.clone(), HashMap::new());
            }

            let thread_stats = op_stats.get_mut(&res.op).unwrap();
            thread_stats.entry(res.id).or_insert_with(WorkerStat::new);
            thread_stats.get_mut(&res.id).unwrap().add_result(&res);

            if !op_ticks.contains_key(&res.op) {
                op_ticks.insert(res.op.clone(), WorkerStat::new());
            }
            let tick_totals = op_ticks.get_mut(&res.op).unwrap();
            tick_totals.add_result(&res);

            if !op_agg.contains_key(&res.op) {
                op_agg.insert(res.op.clone(), WorkerStat::new());
            }
            let agg_totals = op_agg.get_mut(&res.op).unwrap();
            agg_totals.add_result(&res);
        }

        /* Print out the stats we gathered. */
        println!("---");
        if verbose {
            let mut i = 0;
            for (op, op_map) in op_stats.iter_mut() {
                println!("Thread ({})", op);
                for (_, worker) in op_map.iter_mut() {
                    if worker.objs == 0 {
                        /*
                         * don't want to divide by zero when there's
                         * no activity
                         */
                        continue;
                    }

                    println!("\t{}: {}", i, worker.serialize_relative());
                    worker.clear();
                    i += 1;
                }
                i = 0;
            }
        }

        for (op, worker) in op_ticks.iter_mut() {
            print!("Tick ({})", op);
            if worker.objs == 0 {
                println!("No activity this tick");
                continue;
            }
            println!("\t{}", worker.serialize_relative());
        }

        for (op, worker) in op_agg.iter_mut() {
            print!("Total ({})", op);
            if worker.objs == 0 {
                println!("No activity this tick");
                continue;
            }
            let elapsed_sec = start_time.elapsed().unwrap().as_secs();
            println!("\t{}", worker.serialize_absolute(elapsed_sec));
        }
    }
}

/*
 * Expand an input string like:
 *   1,2,3
 * into a slice like:
 *   [ 1, 2, 3 ]
 *
 * This allows for a single operator to expand a given entry. For example,
 *   1:3,2,3
 * turns into
 *   [ 1, 1, 1, 2, 3 ]
 *
 * That syntax allows the left-operand to be expanded into right-operand copies.
 * This also works with string prefixes:
 *   r:2,w:2
 * turns into
 *   [ r, r, w, w ]
 */
fn expand_distribution(dstr: String) -> Vec<String> {
    let mut gen_distr = Vec::new();

    for s in dstr.split(',') {
        let tok: Vec<&str> = s.split(':').collect();
        match tok.len() {
            1 => gen_distr.push(tok[0].to_string()),
            2 => {
                for _ in 0..tok[1].parse::<u32>().unwrap() {
                    gen_distr.push(tok[0].to_string());
                }
            },
            _ => println!("too many multiples in token: {:?}... ignoring",
                tok.join(":")),
        };
    }

    gen_distr
}

/*
 * Converts a distribution created by expand_distribution into a Vec of numbers
 * based on the unit size.
 */
fn convert_numeric_distribution(dstr: Vec<String>, unit: &str) -> Vec<u64> {
    let mut gen_distr = Vec::new();

    let unit_multiple = match unit {
        "k" => 1024,
        "m" => 1024 * 1024,
        _ => panic!("unknown capacity unit: {}", unit),
    };

    for s in dstr {
        gen_distr.push(s.parse::<u64>().unwrap() * unit_multiple);
    }

    gen_distr
}

fn usage(opts: Options, msg: &str) {
    let prog = "chum";
    let synopsis = "\
        Upload files to a given file server as quickly as possible";

    let usg = format!("{} - {}", prog, synopsis);
    println!("{}", opts.usage(&usg));
    println!("{}", msg);
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.reqopt("t",
                "target",
                "target server",
                "IP");

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
    opts.optopt("u",
                "unit",
                &format!("capacity unit for upload file \
                    size, default: {}", DEF_UNIT),
                "k|m");
    opts.optopt("i",
                "interval",
                &format!("interval in seconds at which to \
                    report stats, default: {}", DEF_INTERVAL),
                "NUM");
    opts.optopt("q",
                "queue-mode",
                &format!("queue mode for read operations, default: {}",
                    DEF_QUEUE_MODE),
                "lru|mru|rand");
    opts.optopt("w",
                "workload",
                &format!("workload of operations, default: {}",
                    DEF_WORKLOAD),
                "OP:COUNT,OP:COUNT");

    opts.optflag("v",
                 "verbose",
                 "enable per-thread stat reporting");
    opts.optflag("h",
                 "help",
                 "print this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { usage(opts, &f.to_string()); return; }
    };

    if matches.opt_present("h") {
        usage(opts, "");
        return;
    }

    /* Handle grabbing defaults if the user didn't provide these flags. */
    let conc = matches.opt_get_default("concurrency", DEF_CONCURRENCY).unwrap();
    let sleep = matches.opt_get_default("sleep", DEF_SLEEP).unwrap();
    let unit = matches.opt_get_default("unit",
        String::from(DEF_UNIT)).unwrap();
    let interval =
        matches.opt_get_default("interval", DEF_INTERVAL).unwrap();
    let qmode = match matches.opt_get_default("queue-mode",
        String::from(DEF_QUEUE_MODE)).unwrap().as_ref() {

        /* XXX implement FromStr for QueueMode. */
        "lru" => QueueMode::Lru,
        "mru" => QueueMode::Mru,
        "rand" => QueueMode::Rand,
        _ => {
            println!("unknown queue mode, using '{}'", DEF_QUEUE_MODE);
            QueueMode::Lru
        },
    };

    let ops = if matches.opt_present("workload") {
        matches.opt_str("workload").unwrap()
    } else {
        String::from(DEF_WORKLOAD)
    };
    let ops = expand_distribution(ops);

    let target = matches.opt_str("target").unwrap();
    let verbose = matches.opt_present("v");

    /*
     * Parse the user's size distribution if one was provided, otherwise use
     * our default distr.
     */
    let distr = if matches.opt_present("distribution") {
        matches.opt_str("distribution").unwrap()
    } else {
        String::from(DEF_DISTR)
    };
    let distr = expand_distribution(distr);
    let distr = convert_numeric_distribution(distr, &unit);

    if conc < 1 {
        usage(opts, "concurrency must be > 1");
        return;
    }

    /*
     * Start the real work. Kick off worker threads and a stat listener.
     */

    let q = Arc::new(Mutex::new(Queue::new(qmode, DEF_QUEUE_CAP)));
    let (tx, rx) = channel();

    let mut worker_threads: Vec<JoinHandle<_>> = Vec::new();
    for _ in 0..conc {
        let mut worker = Worker::new(tx.clone(), target.clone(),
            distr.clone(), sleep, q.clone(), ops.clone());
        worker_threads.push(thread::spawn(move || { worker.work(); }));
    }

    /* Kick off statistics collection and reporting. */
    let stat_thread = thread::spawn(move || {
        collect_stats(rx, interval, verbose);
    });

    for hdl in worker_threads {
        hdl.join().expect("failed to join worker thread");
    }
    stat_thread.join().expect("failed to join stat thread");
}
