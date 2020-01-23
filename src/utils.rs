/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */
extern crate regex;

use regex::Regex;
use uuid::Uuid;

use std::error::Error;
use std::{thread, thread::ThreadId};
use std::{time, time::SystemTime, time::UNIX_EPOCH};
use std::vec::Vec;
use std::sync::{Arc, Mutex, mpsc::Receiver};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use crate::worker::{WorkerResult, WorkerStat};
use crate::{reader, writer};
use crate::queue::{Queue, QueueItem};

#[derive(PartialEq)]
pub enum OutputFormat {
    Human, /* prose, for humans watching the console. */
    HumanVerbose,
    Tabular, /* tab-separated, for throwing into something like gnuplot. */
}

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
pub fn collect_stats(
    rx: Receiver<WorkerResult>,
    interval: u64,
    format: OutputFormat,
    data_cap: u64) {

    let mut total_bytes_written: u64 = 0;
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
            if res.op == writer::OP {
                total_bytes_written += res.size;
            }

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

        match format {
            OutputFormat::Human | OutputFormat::HumanVerbose => {
                print_human(start_time, &format, op_stats, op_ticks,
                    &mut op_agg)
            },
            OutputFormat::Tabular => {
                print_tabular(start_time, &format, op_stats, op_ticks,
                    &mut op_agg)
            },
        }

        if data_cap > 0 && total_bytes_written >= data_cap {
            return; /* Exit the thread, signalling and end of the program. */
        }

    }
}

fn print_human(
    start_time: SystemTime,
    format: &OutputFormat,
    mut op_stats: HashMap<String, HashMap<ThreadId, WorkerStat>>,
    mut op_ticks: HashMap<String, WorkerStat>,
    op_agg: &mut HashMap<String, WorkerStat>) {

    /* Print out the stats we gathered. */
    println!("---");
    if *format == OutputFormat::HumanVerbose {
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

fn print_tabular(
    _: SystemTime,
    _: &OutputFormat,
    _: HashMap<String, HashMap<ThreadId, WorkerStat>>,
    op_ticks: HashMap<String, WorkerStat>,
    _: &mut HashMap<String, WorkerStat>) {

    let zero_stat = WorkerStat::new();
    let reader_stats = match op_ticks.get(reader::OP) {
        Some(stats) => stats,
        None => &zero_stat,
    };

    let writer_stats = match op_ticks.get(writer::OP) {
        Some(stats) => stats,
        None => &zero_stat,
    };

    let time = match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(time) => format!("{}", time.as_secs()),
        Err(_) => String::from("0"),
    };

    println!("{} {} {} {} {} {} {} {} {}",
        time,
        reader_stats.objs, writer_stats.objs,
        reader_stats.data,  writer_stats.data,
        reader_stats.ttfb, writer_stats.ttfb,
        reader_stats.rtt, writer_stats.rtt);
}

#[derive(Debug)]
pub struct UtilError {
    msg: String,
}
impl UtilError {
    pub fn new(msg: &str) -> Self {
        UtilError { msg: msg.to_string() }
    }
}
impl Error for UtilError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        None
    }
}
impl std::fmt::Display for UtilError {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "{}", self.msg)
        }
}

/* Convert a human-readable string (e.g. '4k') to bytes (e.g. '4096'). */
pub fn parse_human(val: &str) -> Result<u64, UtilError> {
    let k = 1024;
    let m = k * 1024;
    let g = m * 1024;
    let t = g * 1024;

    if val == "0" {
        return Ok(0);
    }
    let mix_re = Regex::new(r"^\d+[KMGTkmgt]$").unwrap();
    if mix_re.is_match(val) {
        let (first, last) = val.split_at(val.len() - 1);
        let val_as_bytes: u64 =
            u64::from_str_radix(first, 10).map_err(|err| {
                UtilError::new(&err.to_string())
            })?;

        match last.to_ascii_lowercase().as_ref() {
            "k" => Ok(val_as_bytes * k),
            "m" => Ok(val_as_bytes * m),
            "g" => Ok(val_as_bytes * g),
            "t" => Ok(val_as_bytes * t),
            _ => Err(UtilError::new("unrecognized unit suffix")),
        }
    } else {
        Err(UtilError::new("provided value must be a number with a unit \
            suffix"))
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
pub fn expand_distribution(dstr: &str) -> Vec<String> {
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
pub fn convert_numeric_distribution(dstr: Vec<String>)
    -> Result<Vec<u64>, UtilError> {

    let mut gen_distr = Vec::new();

    for s in dstr {
        gen_distr.push(parse_human(&s)?);
    }

    Ok(gen_distr)
}

/*
 * The user provided the path to a file. This file contains a listing of objects
 * in the 'chum' namespace that chum should read back.
 *
 * This function pulls each of these file names from the listing file and
 * inserts them into the chum read queue. The read worker will then pull them
 * off the queue as it normally would (using whatever algorithm the user
 * specified).
 *
 * The default errors we get from the OS and the uuid crate are pretty plain, so
 * we wrap them in a more helpful UtilError.
 */
pub fn populate_queue(queue: Arc<Mutex<Queue>>, readlist: String)
    -> Result<(), UtilError> {

    let file = File::open(readlist).map_err(|e| {
        UtilError::new(&format!("failed to open read listing file: {}",
            e.to_string()))
    })?;
    let br = BufReader::new(file);

    let mut q = queue.lock().unwrap();
    for uuidstr in br.lines() {
        let uuidstr: String = match uuidstr {
            Ok(x) => x,
            Err(_) => {
                return Err(UtilError::new(
                    "failed to read line from read listing file"))
            },
        };

        let uuid = match Uuid::parse_str(uuidstr.as_ref()) {
            Ok(x) => x,
            Err(_) => {
                return Err(UtilError::new(&format!("failed to parse '{}' \
                    from listing file as a uuid", uuidstr)));
            },
        };
        q.insert(QueueItem{ uuid });
    }

    Ok(())
}
