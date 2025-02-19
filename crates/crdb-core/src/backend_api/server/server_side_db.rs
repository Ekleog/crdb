use super::{ObjectManager, ServerQuery, SessionManager, Vacuum};
use crate::backend_api::{BinaryStore, ObjectGet, Reencoder, TestDb};

pub trait ServerSideDb:
    'static
    + Send
    + Sync
    + BinaryStore
    + ObjectGet
    + ObjectManager
    + Reencoder
    + ServerQuery
    + SessionManager
    + TestDb
    + Vacuum
{
}
