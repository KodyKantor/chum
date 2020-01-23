/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2020 Joyent, Inc.
 */

use rand::Rng;

use std::str::FromStr;
use std::error;
use std::fmt;

/*
 * Operating modes that the queue supports. See the block comment above the
 * Queue impl for an explanation.
 */
pub enum QueueMode {
    Lru,
    Mru,
    Rand,
}

#[derive(Debug)]
pub struct QueueModeError;
impl error::Error for QueueModeError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}
impl fmt::Display for QueueModeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "invalid queue mode")
    }
}

/*
 * To make calling code cleaner, let users create the QueueMode from a
 * lowercase str.
 */
impl FromStr for QueueMode {
    type Err = QueueModeError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mode = match s {
            "lru" => Some(QueueMode::Lru),
            "mru" => Some(QueueMode::Mru),
            "rand" => Some(QueueMode::Rand),
            _ => None,
        };

        if mode.is_none() {
            return Err(QueueModeError)
        }
        Ok(mode.unwrap())
    }
}

impl fmt::Display for QueueMode {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let strmode = match self {
            QueueMode::Lru => "lru",
            QueueMode::Mru => "mru",
            QueueMode::Rand => "rand",
        };
        write!(f, "{}", strmode)
    }
}

pub struct QueueItem {
    pub obj: String,
}

pub struct Queue {
    items: Vec<QueueItem>,
    cap: usize,
    mode: QueueMode,
}

/*
 * This is a simple queue data structure. It supports a few different modes of
 * operation.
 *
 * Modes:
 * - Lru (least recently used). Operates like a FIFO queue. When the queue fills
 *   up new items replace the oldest items.
 * - Mru (most recently used). Operates like a LIFO queue (AKA a stack). When
 *   the queue is at capacity the 'bottom' item in the stack is removed and the
 *   new item is added to the top of the stack.
 * - Rand (random). Operates like an array. Random items are returned when using
 *   the accessor function. New items replace a random item.
 */
impl Queue {
    pub fn new(mode: QueueMode, cap: usize) -> Queue {
        Queue {
            items: Vec::with_capacity(cap),
            cap,
            mode,
        }
    }

    /*
     * Inserts an item into the queue.
     * Removes an item if the queue has hit its capacity.
     */
    pub fn insert(&mut self, qi: QueueItem) {
        if self.items.len() < self.cap {
            self.items.push(qi);
            return
        }

        self.remove();
        self.items.push(qi);
    }

    /*
     * Return an item from the queue.
     * Returns None if nothing is in the queue.
     */
    pub fn get(&mut self) -> Option<&QueueItem> {
        if self.items.is_empty() {
            return None
        }

        match self.mode {
            QueueMode::Lru => self.items.get(0),
            QueueMode::Mru => self.items.get(self.items.len()),
            QueueMode::Rand => self.items.get(
                rand::thread_rng().gen_range(0, self.items.len())),
        }
    }

    fn remove(&mut self) {
        if self.items.is_empty() {
            return
        }

        match self.mode {
            QueueMode::Lru => self.items.remove(0),
            QueueMode::Mru => self.items.remove(0),
            QueueMode::Rand => self.items.remove(
                rand::thread_rng().gen_range(0, self.items.len())),
        };
    }
}
