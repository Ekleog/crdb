use crate::BinPtr;
use std::{collections::HashMap, sync::Arc};

pub struct BinariesCache {
    data: HashMap<BinPtr, Arc<Vec<u8>>>,
    size: usize,
    // TODO: have fuzzers that assert that `size` stays in-sync with `binaries`
}

impl BinariesCache {
    pub fn new() -> BinariesCache {
        BinariesCache {
            data: HashMap::new(),
            size: 0,
        }
    }

    pub fn clear(&mut self) {
        self.data.retain(|_, v| {
            if Arc::strong_count(v) == 1 {
                self.size -= v.len();
                false
            } else {
                true
            }
        })
    }

    pub fn insert(&mut self, id: BinPtr, value: Arc<Vec<u8>>) {
        self.size += value.len();
        self.data.insert(id, value);
    }

    pub fn get(&self, id: &BinPtr) -> Option<Arc<Vec<u8>>> {
        self.data.get(id).cloned()
    }
}
