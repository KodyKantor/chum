/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

/*
 * A 'worker' is something that reads or writes data. It should emit events in
 * the form of WorkerResults for every object read or written.
 */
pub struct WorkerResult {
    pub id: u32,
    pub size: u64, /* either in kb or mb */
    pub ttfb: u128, /* millis */
    pub e2e: u128, /* millis */
}

/*
 * WorkerResults can be aggregated into WorkerStats.
 */
pub struct WorkerStat {
    pub objs: u64,
    pub data: u64,
    pub ttfb: u128,
    pub e2e: u128,
}

impl WorkerStat {
    pub fn new() -> Self {
        WorkerStat {
            objs: 0,
            data: 0,
            ttfb: 0,
            e2e: 0,
        }
    }
    pub fn add_result(&mut self, res: &WorkerResult) {
        self.objs += 1;
        self.data += res.size;
        self.ttfb += res.ttfb;
        self.e2e += res.e2e;
    }

    pub fn clear(&mut self) {
        self.objs = 0;
        self.data = 0;
        self.ttfb = 0;
        self.e2e = 0;
    }
}
