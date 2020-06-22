/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

mod fs;
mod queue;
mod s3;
mod state;
mod utils;
mod webdav;
mod worker;

use crate::queue::{Queue, QueueMode};
use crate::utils::*;
use crate::worker::*;

use std::error::Error;
use std::sync::{mpsc::channel, mpsc::Sender, Arc, Mutex};
use std::{thread, thread::JoinHandle};

use clap::{App, Arg, SubCommand};

/* Default values. */
const DEF_CONCURRENCY: &str = "1";
const DEF_SLEEP: &str = "0";
const DEF_DISTR: &str = "128k,256k,512k";
const DEF_INTERVAL: &str = "2";
const DEF_QUEUE_MODE: QueueMode = QueueMode::Rand;
const DEF_WORKLOAD: &str = "r,w";
const DEF_OUTPUT_FORMAT: &str = "h";

/*
 * Arguments specific to the 'fs' worker subcommand.
 */
fn get_fs_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![
        Arg::with_name("percentage")
            .help(
                "fill the target filesystem path to given percentage \
                  capacity",
            )
            .takes_value(true)
            .long("percentage")
            .short("p"),
        Arg::with_name("no-sync")
            .help("disable synchronous writes")
            .long("no-sync"),
    ]
}

fn get_webdav_args<'a, 'b>() -> Vec<Arg<'a, 'b>> {
    vec![Arg::with_name("http2").help("use HTTP/2").long("http2")]
}

fn main() -> Result<(), Box<dyn Error>> {
    /*
     * Options shared by all worker backends.
     */
    let shared_args =
        vec!(

        Arg::with_name("target")
            .help("target address (webdav, s3), or path (fs) for system under \
                  test")
            .takes_value(true)
            .long("target")
            .short("t")
            .required(true),

        Arg::with_name("concurrency")
            .help("number of worker threads, default: 1")
            .long("concurrency")
            .short("c")
            .takes_value(true),

        Arg::with_name("sleep")
            .help("sleep duration in millis between each upload, default: 0")
            .long("sleep")
            .short("s")
            .takes_value(true),

        Arg::with_name("distribution")
            .help("comma-separated distribution of file sizes to upload, \
                    default: 128k,256k,512k")
            .long("distribution")
            .short("d")
            .takes_value(true),

        Arg::with_name("interval")
            .help("interval in seconds at which to report stats, default: 2")
            .long("interval")
            .short("i")
            .takes_value(true),

        Arg::with_name("workload")
            .help("workload of operations, default: r,w")
            .long("workload")
            .short("w")
            .takes_value(true),

        Arg::with_name("format")
            .help("statistics output format, default: h")
            .long("format")
            .short("f")
            .takes_value(true),

        Arg::with_name("max-data")
            .help("maximum amount of data to write to the target, '0' disables \
                  cap, default: none")
            .long("max-data")
            .short("m")
            .takes_value(true),

        Arg::with_name("read-list")
            .help("path to a file listing files to read from server, default: \
                  none (files are chosen from recent uploads)")
            .long("read-list")
            .short("r")
            .takes_value(true),

        Arg::with_name("debug")
            .help("enable verbose statemap tracing (may impact performance) \
                    Must be used with the -m flag")
            .long("debug")
            .short("D")
    );

    let mut worker =
        SubCommand::with_name("worker").about("standalone chum worker");

    let webdav = SubCommand::with_name("webdav")
        .about("webdav mode")
        .args(&shared_args)
        .args(&get_webdav_args());

    let s3 = SubCommand::with_name("s3")
        .about("s3 mode")
        .args(&shared_args);

    let fs = SubCommand::with_name("fs")
        .about("local filesystem mode")
        .args(&shared_args)
        .args(&get_fs_args());

    worker = worker.subcommand(webdav);
    worker = worker.subcommand(s3);
    worker = worker.subcommand(fs);

    let matches = App::new("manta-chum")
        .about("cross-protocol storage testing tool")
        .subcommand(worker)
        /*
         * .subcommand(instructor)
         * .subcommand(pupil)
         */
        .get_matches();

    let (_mode_of_operation, mode_args) = matches.subcommand();

    let mode_args = mode_args.unwrap_or_else(|| {
        println!("no mode of operation specified\n\n{}", matches.usage());
        std::process::exit(1);
    });

    let (protocol_name, protocol_args) = mode_args.subcommand();
    let protocol_args = protocol_args.unwrap_or_else(|| {
        println!("no protocol specified\n\n{}", matches.usage());
        std::process::exit(1);
    });

    /*
     * Get args with sensible defaults.
     */
    let distr = protocol_args.value_of("distribution").unwrap_or(DEF_DISTR);
    let workload = protocol_args.value_of("workload").unwrap_or(DEF_WORKLOAD);
    let format: OutputFormat = protocol_args
        .value_of("format")
        .unwrap_or(DEF_OUTPUT_FORMAT)
        .parse()?;

    let conc = protocol_args
        .value_of("concurrency")
        .unwrap_or(DEF_CONCURRENCY)
        .parse::<u32>()
        .expect("concurrency should be a positive number");
    let interval = protocol_args
        .value_of("interval")
        .unwrap_or(DEF_INTERVAL)
        .parse::<u64>()
        .expect("interval should be a positive number");
    let sleep = protocol_args
        .value_of("sleep")
        .unwrap_or(DEF_SLEEP)
        .parse::<u64>()
        .expect("sleep should be a positive number");

    /*
     * Get args with no defaults.
     */
    let target = protocol_args.value_of("target").unwrap();

    let readlist = protocol_args.value_of("read-list");

    /* Some arguments require more advanced parsing. */
    /* Prefer percentage data cap, otherwise use the bytes-written data cap. */
    let cap = match protocol_args.value_of("percentage") {
        Some(p) => {
            let capnum = p
                .parse::<u32>()
                .expect("percentage should be a positive number");
            Some(DataCap::Percentage(capnum))
        }
        None => match protocol_args.value_of("max-data") {
            Some(m) => {
                let capnum = parse_human(&m)?;
                Some(DataCap::LogicalData(capnum))
            }
            None => None,
        },
    };

    let distr = convert_numeric_distribution(expand_distribution(&distr)?)?;
    let ops = convert_operation_distribution(expand_distribution(&workload)?)?;

    let q: Arc<Mutex<Queue<String>>> =
        Arc::new(Mutex::new(Queue::new(DEF_QUEUE_MODE)));
    let sync = !protocol_args.is_present("no-sync");
    let http2 = protocol_args.is_present("http2");

    let targ = target.to_string();
    let proto = protocol_name.to_string();

    if let Some(rl) = readlist {
        populate_queue(q.clone(), rl.to_string())?
    }

    /*
     * Start the real work. Kick off worker threads and a stat listener.
     */

    let mut debug_tx: Option<Sender<state::State>> = None;
    let (tx, rx) = channel();
    let smap_thread = if protocol_args.is_present("debug") {
        /*
         * The statemap format isn't a streaming format, so we need the states
         * to stop coming (i.e. the program ends) at some point. The only ways
         * to end the program are to:
         * - use a data cap
         * - send a signal
         *
         * I don't want to go through the signal handling dance, so a data cap
         * is the only way to end manta-chum in a quiescent manner.
         */
        if !protocol_args.is_present("max-data") {
            println!(
                "--debug must be used with -m flag\n\n{}",
                protocol_args.usage()
            );
            std::process::exit(1);
        }

        debug_tx = Some(tx);
        Some(thread::spawn(move || {
            state::state_listener(rx);
        }))
    } else {
        None
    };

    let (tx, rx) = channel();
    let workeropts = WorkerOptions {
        protocol: protocol_name.to_string(),
        read_queue: ops.contains(&Operation::Read)
            || ops.contains(&Operation::Delete),
        operations: ops,
        distribution: distr,
        target: targ.clone(),
        sleep,
        tx,
        debug_tx: debug_tx.clone(),
        queue: q,
        sync,
        http2,
    };

    let mut worker_threads: Vec<JoinHandle<_>> = Vec::new();
    for _ in 0..conc {
        let wopts = workeropts.clone();
        worker_threads.push(thread::spawn(move || {
            Worker::new(wopts).work();
        }));
    }

    /* Kick off statistics collection and reporting. */
    let stat_thread = thread::spawn(move || {
        collect_stats(rx, interval, format, cap, targ.clone(), proto.clone());
    });

    /*
     * To make sure that the state thread exits when all worker threads exit,
     * drop our copy of the sender channel here.
     *
     * The state collection thread exits when all senders exit. This main
     * thread will live the life of the program and it will not send any states
     * through the channel.
     */
    drop(debug_tx);
    drop(workeropts);

    /*
     * When the stat thread exits we know that enough data was written.
     */
    stat_thread.join().expect("failed to join stat thread");

    for hdl in worker_threads {
        hdl.join().expect("failed to join worker thread");
    }

    if let Some(jh) = smap_thread {
        jh.join().expect("failed to join statemap thread");
    }

    Ok(())
}
