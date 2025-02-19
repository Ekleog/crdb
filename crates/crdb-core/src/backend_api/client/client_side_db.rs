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
    + ClientObjectManager
    + LocalQuery
    + LoginManager
    + ObjectGet
    + QueryManager
    + Reencoder
    + ServerObjectManager
    + StorageInfo
    + TestDb
    + UploadQueue
    + Vacuum
{
}
