use crate::ClientDb;
use crdb_core::{DbPtr, Object};
use educe::Educe;
use std::{fmt::Debug, future::Future, ops::Deref, sync::Arc};

#[derive(Clone, Educe)]
#[educe(Debug(bound(T: Debug)))]
pub struct Obj<T: Object> {
    ptr: DbPtr<T>,
    data: Arc<T>,

    #[educe(Debug(ignore))]
    db: Arc<ClientDb>,
}

impl<T: Object> Obj<T> {
    #[doc(hidden)] // TODO(client-high): make priv once client/config.rs is no longer a big macro
    pub fn new(ptr: DbPtr<T>, data: Arc<T>, db: Arc<ClientDb>) -> Obj<T> {
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

impl<T: Object> Deref for Obj<T> {
    type Target = T;

    fn deref(&self) -> &T {
        &self.data
    }
}
