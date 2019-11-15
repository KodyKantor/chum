/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

extern crate reqwest;
extern crate getopts;
extern crate uuid;

use rand::seq::SliceRandom;
use rand::thread_rng;
use std::env;
use std::thread;
use std::time;
use std::vec::Vec;
use std::fs::File;
use std::io::Error;

use reqwest::{Client, Body, header::USER_AGENT};
use getopts::Options;
use uuid::Uuid;

fn worker(target: &str, distr: &[u64], unit: &str, pause: u64)
    -> Result<(), Error> {

    let mut rng = thread_rng();

    let dir = "chum"; /* destination directory for uploads */
    let client = Client::new();

    loop {
        let fname = Uuid::new_v4();

        /* Randomly choose a file size from the list. */
        let mut fsize = *distr.choose(&mut rng).unwrap();

        fsize = match unit {
            "k" => fsize * 1024,
            "m" => fsize * 1024 * 1024,
            _ => fsize * 1024,
        };

        /*
         * Obviously sub-optimal having this in the loop. Borrow-checker
         * struggle in the flesh.
         */
        let f = File::open("/dev/urandom")?;
        let body = Body::sized(f, fsize);

        let resp = client.put(
            &format!("http://{}:80/{}/{}", target, dir, fname))
            .header(USER_AGENT, "chum/1.0")
            .body(body)
            .send().expect("request builder failed");

        if !resp.status().is_success() {
            println!("upload not successful: {}", resp.status());
        }

        println!("done");

        if pause > 0 {
            thread::sleep(time::Duration::from_millis(pause));
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

fn main() -> Result<(), Error> {
    let default_conc = 1;
    let default_pause = 0;
    let default_distr = [128, 256, 512];
    let default_unit = "k".to_string();

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

    opts.optflag("h", "help", "print this help message");

    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { usage(opts, &f.to_string()); return Ok(()) }
    };

    if matches.opt_present("h") {
        usage(opts, "");
        return Ok(())
    }

    let conc = matches.opt_get_default("concurrency", default_conc)
        .unwrap();
    let pause = matches.opt_get_default("pause", default_pause).unwrap();
    let user_unit = matches.opt_get_default("unit", default_unit).unwrap();
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
        return Ok(())
    }

    let mut vec = Vec::new();

    for _ in 0..conc {
        let targ = target.clone();
        let unit = user_unit.clone();
        let dist = distr.clone();
        vec.push(thread::spawn(move || {
            worker(&targ, &dist, &unit, pause)
        }));
    }

    /* Wait for all of the threads to exit. */
    while !vec.is_empty() {
        let hdl = vec.pop();
        if hdl.is_some() {
            if let Err(e) = hdl.unwrap().join() {
                println!("failed to join therad: {:?}", e);
            }
        }
    }
    Ok(())
}
