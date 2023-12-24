use std::{any::Any, marker::PhantomData, sync::Arc};

use ulid::Ulid;

pub struct User {
    pub id: Ulid,
}

#[non_exhaustive]
pub enum JsonPathItem {
    Key(String),
    Id(usize),
}

#[non_exhaustive]
pub enum JsonNumber {
    F64(f64),
    I64(i64),
    U64(u64),
}

#[non_exhaustive]
pub enum Query {
    // Logic operators
    All(Vec<Query>),
    Any(Vec<Query>),
    Not(Box<Query>),

    // JSON tests
    Eq(Vec<JsonPathItem>, serde_json::Value),
    Ne(Vec<JsonPathItem>, serde_json::Value),

    // Integers
    Le(Vec<JsonPathItem>, JsonNumber),
    Lt(Vec<JsonPathItem>, JsonNumber),
    Ge(Vec<JsonPathItem>, JsonNumber),
    Gt(Vec<JsonPathItem>, JsonNumber),

    // Arrays and object subscripting
    Contains(Vec<JsonPathItem>, serde_json::Value),

    // Full text search
    ContainsStr(Vec<JsonPathItem>, String),
}

mod private {
    pub trait Sealed {}
}

pub trait CanDoCallbacks: private::Sealed {
    fn get<T: Object>(&self, ptr: DbPtr<T>) -> anyhow::Result<Arc<T>>;
}

/// Note that due to postgresql limitations reasons, this type MUST NOT include any
/// null byte in the serialized JSON. Including them will result in internal server
/// errors.
pub trait Object:
    Any
    + Clone
    + Default
    + Eq
    + PartialEq
    + Send
    + Sync
    + for<'a> serde::Deserialize<'a>
    + serde::Serialize
{
    /// Note that due to postgresql limitations reasons, this type MUST NOT include any
    /// null byte in the serialized JSON. Trying to submit one such event will result
    /// in the event being rejected by the server.
    type Event: Any + Send + Sync + for<'a> serde::Deserialize<'a> + serde::Serialize;

    fn ulid() -> &'static Ulid;
    fn snapshot_version() -> u64 {
        0
    }
    #[allow(unused_variables)]
    fn from_old_snapshot(version: u64, data: serde_json::Value) -> Self {
        unimplemented!()
    }

    fn can_create<C: CanDoCallbacks>(&self, user: User, db: &C) -> anyhow::Result<bool>;
    /// Note that permissions are always checked with the latest version of the object on the server.
    /// So, due to this, CRDB objects are not strictly speaking a CRDT. However, it is required to do
    /// so for security, because otherwise a user who lost permissions would still be allowed to
    /// submit events antidated to before the permission loss, which would be bad as users could
    /// re-grant themselves permissions.
    fn can_apply<C: CanDoCallbacks>(
        &self,
        user: &User,
        event: &Self::Event,
        db: &C,
    ) -> anyhow::Result<bool>;
    fn users_who_can_read<C: CanDoCallbacks>(&self) -> anyhow::Result<Vec<User>>;
    fn apply(&mut self, event: &Self::Event) -> anyhow::Result<()>;
    fn is_heavy(&self) -> anyhow::Result<bool>;
}

pub struct DbPtr<T: Object> {
    #[doc(hidden)]
    pub id: Ulid,
    _phantom: PhantomData<T>,
}

pub struct BinPtr {
    id: Ulid,
}
