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
    pub size: u64,
}

/*
 * WorkerResults can be aggregated into WorkerStats.
 */
pub struct WorkerStat {
    pub objs: u64,
    pub data: u64, /* either in kb or mb */
}

