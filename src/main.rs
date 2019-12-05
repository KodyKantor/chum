/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate getopts;

mod writer;
mod worker;

use std::env;
use std::thread;
use std::time;
use std::time::SystemTime;
use std::vec::Vec;
use std::sync::mpsc::{channel, Receiver};

use crate::writer::Writer;
use crate::worker::{WorkerResult, WorkerStat};

use getopts::Options;

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
 */
fn collect_stats(rx: Receiver<WorkerResult>, interval: u64,
    conc: u32, unit: &str, verbose: bool) {

    let mut agg_totals = WorkerStat::new();
    let start_time = SystemTime::now();

    loop {
        thread::sleep(time::Duration::from_secs(interval));

        let mut tick_totals = WorkerStat::new();
        let mut thread_stats: Vec<WorkerStat> = Vec::new();

        /* Initialize a stat structure for each thread. */
        for _ in 0..conc {
            thread_stats.push(WorkerStat::new());
        }

        /*
         * Catch up with the results that worker threads sent while this
         * thread was sleeping.
         */
        for res in rx.try_iter() {
            if let Some(thread) =
                thread_stats.get_mut(res.id as usize) {

                thread.add_result(&res);
                tick_totals.add_result(&res);
                agg_totals.add_result(&res);
            }
        }

        /* Print out the stats we gathered. */
        println!("---");
        for i in 0..conc {
            if let Some(worker) =
                thread_stats.get_mut(i as usize) {

                if verbose {
                    println!("Thread ({}): {} objs, {}{}, {}ms avg ttfb, \
                        {}ms avg e2e", i, worker.objs, worker.data, unit,
                        worker.ttfb / worker.objs as u128,
                        worker.e2e / worker.objs as u128);
                }
                worker.clear();
            }
        }

        let elapsed_sec = start_time.elapsed().unwrap().as_secs();
        println!("Tick ({} threads): {} objs, {}{}, {}ms avg ttfb, \
            {}ms avg e2e",
            conc, tick_totals.objs, tick_totals.data, unit,
            tick_totals.ttfb / tick_totals.objs as u128,
            tick_totals.e2e / tick_totals.objs as u128);
        println!("Total: {} objs, {}{}, {}s, avg {} objs/s, \
            avg {} {}/s", agg_totals.objs,
            agg_totals.data, unit, elapsed_sec, agg_totals.objs / elapsed_sec,
            agg_totals.data / elapsed_sec, unit);
    }
}

/*
 * Expand an input string like:
 *   1,2,3
 * into a slice like:
 *   [ 1, 2, 3 ]
 *
 * This allows for a single operator to expand a given entry. For example,
 *   1x3,2,3
 * turns into
 *   [ 1, 1, 1, 2, 3 ]
 *
 * That syntax allows the left-operand to be expanded into right-operand copies.
 *
 */
fn expand_distribution(dstr: String) -> Vec<u64> {
    let mut gen_distr = Vec::new();

    for s in dstr.split(',') {
        let tok: Vec<&str> = s.split('x').collect();
        match tok.len() {
            1 => gen_distr.push(tok[0].parse::<u64>().unwrap()),
            2 => for _ in 0..tok[1].parse::<u64>().unwrap() {
                gen_distr.push(tok[0].parse::<u64>().unwrap());
            },
            _ => println!("too many multiples in token: {:?}... ignoring",
                tok.join("x")),
        };
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
    let default_conc = 1;
    let default_pause = 0;
    let default_distr = [128, 256, 512];
    let default_unit = "k".to_string();
    let default_interval = 2;

    let args: Vec<String> = env::args().collect();
    let mut opts = Options::new();

    opts.reqopt("t", "target", "target server", "IP");

    opts.optopt("c", "concurrency", format!("number of concurrent threads, \
        default: {}", default_conc).as_ref(), "NUM");
    opts.optopt("p", "pause", format!("pause duration in millis between each \
        upload, default: {}", default_pause).as_ref(), "NUM");
    opts.optopt("d", "distribution", format!("comma-separated distribution of \
        file sizes to upload, default: {:?}", default_distr).as_ref(),
        "NUM,NUM,...");
    opts.optopt("u", "unit", format!("capacity unit for upload file \
        size, default: {}", default_unit).as_ref(), "k|m");
    opts.optopt("i", "interval", format!("interval in seconds at which to \
        report stats, default: {}", default_interval).as_ref(), "NUM");

    opts.optflag("v", "verbose", "enable per-thread stat reporting");
    opts.optflag("h", "help", "print this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { usage(opts, &f.to_string()); return; }
    };

    if matches.opt_present("h") {
        usage(opts, "");
        return;
    }

    let verbose = matches.opt_present("v");

    /* Handle grabbing defaults if the user didn't provide these flags. */
    let conc = matches.opt_get_default("concurrency", default_conc).unwrap();
    let pause = matches.opt_get_default("pause", default_pause).unwrap();
    let user_unit = matches.opt_get_default("unit", default_unit).unwrap();
    let interval =
        matches.opt_get_default("interval", default_interval).unwrap();
    let target = matches.opt_str("target").unwrap();

    /*
     * Parse the user's size distribution if one was provided, otherwise use
     * our default distr.
     */
    let distr = if matches.opt_present("distribution") {
        let user_distr = matches.opt_str("distribution").unwrap();
        expand_distribution(user_distr)
    } else {
        default_distr.to_vec()
    };

    if conc < 1 {
        usage(opts, "concurrency must be > 1");
        return;
    }

    /*
     * Start the real work. Kick off a writer and stat listener.
     */

    let (tx, rx) = channel();
    let writer = Writer::new(conc, tx, target, distr, user_unit.clone(), pause);
    let writer_thread = thread::spawn(move || { writer.run(); });

    /* Kick off statistics collection and reporting. */
    let stat_unit = user_unit.clone();
    let stat_thread = thread::spawn(move || {
        collect_stats(rx, interval, conc, &stat_unit, verbose);
    });

    writer_thread.join().expect("failed to join worker thread");
    stat_thread.join().expect("failed to join stat thread");
}
