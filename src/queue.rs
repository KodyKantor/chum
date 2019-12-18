/*
 * This Source Code Form is subject to the terms of the Mozilla Public
 * License, v. 2.0. If a copy of the MPL was not distributed with this
 * file, You can obtain one at http://mozilla.org/MPL/2.0/.
 *
 * Copyright 2019 Joyent, Inc.
 */

use uuid::Uuid;
use rand::Rng;

/*
 * Operating modes that the queue supports.
 */
pub enum QueueMode {
    Lru,
    Mru,
    Rand,
}

/*
 * Description of a queue item.
 */
pub struct QueueItem {
    pub uuid: Uuid,    
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

    pub fn add(&mut self, qi: QueueItem) {
        if self.items.len() < self.cap {
            self.items.push(qi);
            return
        }

        match self.mode {
            QueueMode::Lru => (),
            QueueMode::Mru => {
                self.items.remove(0);
                self.items.push(qi);
            },
            QueueMode::Rand => {
                self.get();
                self.items.push(qi);
            },

        }
    }

    /*
     * Return an item from the queue. This will also remove the returned item
     * from the queue.
     *
     * Returns None if nothing is in the queue.
     */
    pub fn get(&mut self) -> Option<QueueItem> {
        if self.items.is_empty() {
            return None
        }

        match self.mode {
            QueueMode::Lru => Some(self.items.remove(0)),
            QueueMode::Mru => self.items.pop(),
            QueueMode::Rand => {
                /*
                 * Use swap_remove to avoid some copying since we don't care
                 * about the order of this data.
                 */
                Some(self.items.swap_remove(
                    rand::thread_rng().gen_range(0, self.items.len())))
            },
        }
    }
}
