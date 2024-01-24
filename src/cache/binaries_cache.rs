use crate::BinPtr;
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

    pub fn clear(&mut self) {
        self.data.retain(|_, v| Weak::strong_count(v) > 0)
    }

    pub fn insert(&mut self, id: BinPtr, value: Weak<[u8]>) {
        self.data.insert(id, value);
    }

    pub fn get(&self, binary_id: &BinPtr) -> Option<Arc<[u8]>> {
        self.data.get(binary_id).and_then(|b| b.upgrade())
    }
}
