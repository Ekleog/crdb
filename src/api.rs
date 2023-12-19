use std::{marker::PhantomData, sync::Arc};

use uuid::Uuid;

pub struct User {
    pub id: Uuid,
}

pub trait CanApplyCallbacks {
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
}

pub trait ApplyCallbacks {
    fn force_snapshot(&mut self);
    fn create_subsequent<T: Object>(&mut self, object: T);
    fn submit_subsequent<T: Object>(&mut self, object: DbPtr<T>, event: T::Event);
}

/// Note that due to postgresql limitations reasons, this type MUST NOT include any
/// null byte in the serialized JSON. Including them will result in internal server
/// errors.
pub trait Object: Default + for<'a> serde::Deserialize<'a> + serde::Serialize {
    /// Note that due to postgresql limitations reasons, this type MUST NOT include any
    /// null byte in the serialized JSON. Trying to submit one such event will result
    /// in the event being rejected by the server.
    type Event: for<'a> serde::Deserialize<'a> + serde::Serialize;

    fn uuid() -> &'static Uuid;
    fn snapshot_version() -> u64 {
        0
    }
    #[allow(unused_variables)]
    fn from_old_snapshot(version: u64, data: serde_json::Value) -> Self {
        unimplemented!()
    }

    fn can_apply<C: CanApplyCallbacks>(
        &self,
        user: &User,
        event: &Self::Event,
        db: &C,
    ) -> anyhow::Result<bool>;
    fn apply<C: ApplyCallbacks>(&mut self, event: &Self::Event, db: &mut C) -> anyhow::Result<()>;
    fn is_heavy(&self) -> anyhow::Result<bool>;
}

pub struct DbPtr<T: Object> {
    #[doc(hidden)]
    pub id: Uuid,
    _phantom: PhantomData<T>,
}

impl<T: Object> DbPtr<T> {
    pub fn from_uuid_unchecked(id: Uuid) -> DbPtr<T> {
        DbPtr {
            id,
            _phantom: PhantomData,
        }
    }
}

pub trait Db {
    fn create<T: Object>(&self, object: T) -> anyhow::Result<DbPtr<T>>;
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
    fn submit<T: Object>(&self, object: DbPtr<T>, event: T::Event) -> anyhow::Result<()>;
}
