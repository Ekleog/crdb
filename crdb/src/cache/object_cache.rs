use crate::{DynSized, ObjectId};
use crdb_core::SystemTimeExt;
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicI64, Ordering},
        Arc,
    },
};
use web_time::SystemTime;

// TODO(test-med): test this

pub struct ObjectCache {
    watermark: usize,
    // AtomicI64 here is the timestamp (in ms since unix epoch) of the last access to the object
    objects: HashMap<ObjectId, (AtomicI64, Arc<dyn DynSized>)>,
    approx_exclusive_size: usize,
}

impl ObjectCache {
    pub fn new(watermark: usize) -> ObjectCache {
        ObjectCache {
            watermark,
            objects: HashMap::new(),
            approx_exclusive_size: 0,
        }
    }

    fn add_approx_size(&mut self, size: usize) {
        self.approx_exclusive_size = self.approx_exclusive_size.saturating_add(size);
    }

    fn rm_approx_size(&mut self, size: usize) {
        self.approx_exclusive_size = self.approx_exclusive_size.saturating_sub(size);
    }

    fn recompute_exclusive_size(&mut self) {
        self.approx_exclusive_size = 0;
        for (_, v) in self.objects.values() {
            if Arc::strong_count(v) == 1 {
                self.approx_exclusive_size += v.deep_size_of();
            }
        }
    }

    pub fn set(&mut self, object_id: ObjectId, value: Arc<dyn DynSized>) {
        let now = AtomicI64::new(SystemTime::now().ms_since_posix().unwrap());
        self.add_approx_size(value.deep_size_of());
        if let Some(previous) = self.objects.insert(object_id, (now, value)) {
            self.rm_approx_size(previous.1.deep_size_of());
        }
        if self.approx_exclusive_size > self.watermark {
            self.recompute_exclusive_size();
            if self.approx_exclusive_size > self.watermark {
                self.apply_watermark();
            }
        }
    }

    pub fn get(&self, id: &ObjectId) -> Option<Arc<dyn DynSized>> {
        self.objects.get(id).map(|(access, v)| {
            access.store(
                SystemTime::now().ms_since_posix().unwrap(),
                Ordering::Relaxed,
            );
            v.clone()
        })
    }

    pub fn remove(&mut self, object_id: &ObjectId) {
        if let Some(previous) = self.objects.remove(object_id) {
            self.rm_approx_size(previous.1.deep_size_of());
        }
    }

    fn apply_watermark(&mut self) {
        let mut all_entries = self
            .objects
            .iter()
            .map(|(id, (t, v))| (t.load(Ordering::Relaxed), *id, v.clone()))
            .collect::<Vec<_>>();
        all_entries.sort_unstable_by_key(|(t, _, _)| *t);
        for (_, id, v) in all_entries {
            if Arc::strong_count(&v) == 2 {
                // One is `all_entries`, one is the HashMap Arc
                self.objects.remove(&id);
                self.rm_approx_size(v.deep_size_of());
                if self.approx_exclusive_size <= self.watermark / 2 {
                    break;
                }
            }
        }
    }
}
