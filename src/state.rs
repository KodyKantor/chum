/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use std::fs::File;
use std::io::Write;
use std::sync::mpsc::Receiver;

use chrono::{DateTime, Utc};

use statemap::Statemap;

pub struct State {
    pub host: String,  /* Host name */
    pub state: String, /* Name of the state, e.g. "putobject" or "fsync" */
    pub start_time: DateTime<Utc>,
    pub end_time: DateTime<Utc>,
}

pub fn state_listener(rx: Receiver<State>) {
    let mut statemap =
        Statemap::new("manta-chum", Some("myhost".to_string()), None);
    statemap.set_state_color("waiting", "white");

    while let Ok(v) = rx.recv() {
        statemap.set_state(&v.host, &v.state, None, v.start_time);
        statemap.set_state(&v.host, "waiting", None, v.end_time);
    }

    let mut f = File::create("states.out").unwrap();
    for state in statemap {
        f.write_all(&format!("{}\n", state).as_bytes()).unwrap();
    }
}
