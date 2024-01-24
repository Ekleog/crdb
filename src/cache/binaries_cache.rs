use crate::BinPtr;
use std::{collections::HashMap, sync::Arc};

pub struct BinariesCache {
    data: HashMap<BinPtr, Arc<[u8]>>,
}

impl BinariesCache {
    pub fn new() -> BinariesCache {
        BinariesCache {
            data: HashMap::new(),
        }
    }

    pub fn clear(&mut self) {
        self.data.retain(|_, v| Arc::strong_count(v) != 1)
    }

    pub fn insert(&mut self, id: BinPtr, value: Arc<[u8]>) {
        self.data.insert(id, value);
    }

    pub fn get(&self, binary_id: &BinPtr) -> Option<Arc<[u8]>> {
        self.data.get(binary_id).cloned()
    }
}
