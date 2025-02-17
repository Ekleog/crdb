use crate::backend_api::{BinaryStore, ObjectGet, Reencoder, TestDb};

use super::{
    ClientObjectManager, LocalQuery, LoginManager, QueryManager, ServerObjectManager, StorageInfo,
    UploadQueue, Vacuum,
};

pub trait ClientSideDb:
    'static
    + waaaa::Send
    + waaaa::Sync
    + BinaryStore
    + ObjectGet
    + Reencoder
    + TestDb
    + ClientObjectManager
    + QueryManager
    + LocalQuery
    + LoginManager
    + ServerObjectManager
    + StorageInfo
    + UploadQueue
    + Vacuum
{
}
