use crate::ClientDb;
use crdb_core::{ClientSideDb, DbPtr, Object};
use educe::Educe;
use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};

#[derive(Educe)]
#[educe(Clone, Debug(bound(T: Debug)))]
pub struct Obj<T: Object, LocalDb: ClientSideDb> {
    ptr: DbPtr<T>,
    data: Arc<T>,

    #[educe(Debug(ignore))]
    db: Arc<ClientDb<LocalDb>>,
}

impl<T: Object, LocalDb: ClientSideDb> Obj<T, LocalDb> {
    pub(crate) fn new(ptr: DbPtr<T>, data: Arc<T>, db: Arc<ClientDb<LocalDb>>) -> Obj<T, LocalDb> {
        Obj { ptr, data, db }
    }

    pub fn ptr(&self) -> DbPtr<T> {
        self.ptr
    }

    pub async fn submit(
        &self,
        event: T::Event,
    ) -> crate::Result<impl Future<Output = crate::Result<()>>> {
        self.db.submit::<T>(self.ptr, event).await
    }
}

impl<T: Object, LocalDb: ClientSideDb> Deref for Obj<T, LocalDb> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}
