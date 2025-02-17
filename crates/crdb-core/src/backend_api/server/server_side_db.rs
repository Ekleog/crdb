use super::{ObjectManager, ServerQuery, SessionManager, Vacuum};
use crate::backend_api::{BinaryStore, ObjectGet, Reencoder, TestDb};

pub trait ServerSideDb:
    'static
    + waaaa::Send
    + waaaa::Sync
    + BinaryStore
    + ObjectGet
    + Reencoder
    + TestDb
    + ObjectManager
    + ServerQuery
    + Vacuum
    + SessionManager
{
}
