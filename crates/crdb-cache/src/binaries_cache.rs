use crdb_core::BinPtr;
use std::{
    collections::HashMap,
    sync::{Arc, Weak},
};

pub struct BinariesCache {
    data: HashMap<BinPtr, Weak<[u8]>>,
}

impl BinariesCache {
    pub fn new() -> BinariesCache {
        BinariesCache {
            data: HashMap::new(),
        }
    }

    pub fn insert(&mut self, id: BinPtr, value: Weak<[u8]>) {
        self.data.insert(id, value);
    }

    pub fn get(&self, binary_id: &BinPtr) -> Option<Arc<[u8]>> {
        self.data.get(binary_id).and_then(|b| b.upgrade())
    }
}

impl Default for BinariesCache {
    fn default() -> BinariesCache {
        BinariesCache::new()
    }
}
