use super::{ApiDb, Authenticator, LocalDb};
use crate::{
    api::ApiConfig,
    cache::CacheDb,
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    full_object::FullObject,
    BinPtr, CrdbStream, EventId, Object, ObjectId, Query, Timestamp, User,
};
use futures::{future, StreamExt};
use std::sync::Arc;

pub struct ClientDb<A: Authenticator> {
    api: Arc<ApiDb<A>>,
    db: Arc<CacheDb<LocalDb>>,
}

impl<A: Authenticator> ClientDb<A> {
    pub async fn connect<C: ApiConfig>(
        base_url: Arc<String>,
        auth: Arc<A>,
        local_db: &str,
        cache_watermark: usize,
    ) -> anyhow::Result<ClientDb<A>> {
        // TODO: add a concept of "locked" objects and binaries that cannot be vacuumed out of the
        // local db, because they have not been acknowledged by the server yet
        C::check_ulids();
        let api = Arc::new(ApiDb::connect(base_url, auth).await?);
        let db = CacheDb::new::<C>(Arc::new(LocalDb::connect(local_db).await?), cache_watermark);
        db.also_watch_from::<C, _>(&api);
        Ok(ClientDb { api, db })
    }

    pub fn user(&self) -> User {
        self.api.user()
    }

    pub async fn disconnect(&self) -> anyhow::Result<()> {
        self.api.disconnect().await
    }

    pub async fn clear_cache(&self) {
        self.db.clear_cache().await
    }

    pub async fn clear_binaries_cache(&self) {
        self.db.clear_binaries_cache().await
    }

    pub async fn clear_objects_cache(&self) {
        self.db.clear_objects_cache().await
    }

    pub async fn reduce_size_to(&mut self, size: usize) {
        self.db.reduce_size_to(size).await
    }
}

impl<A: Authenticator> Db for ClientDb<A> {
    async fn new_objects(&self) -> impl CrdbStream<Item = DynNewObject> {
        self.api.new_objects().await
    }

    async fn new_events(&self) -> impl CrdbStream<Item = DynNewEvent> {
        self.api.new_events().await
    }

    async fn new_recreations(&self) -> impl CrdbStream<Item = DynNewRecreation> {
        self.api.new_recreations().await
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        self.api.unsubscribe(ptr).await
    }

    async fn create<T: Object, C: crate::CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> crate::Result<()> {
        self.api.create(id, created_at, object.clone(), cb).await?;
        self.db.create(id, created_at, object, cb).await
    }

    async fn submit<T: Object, C: crate::CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<()> {
        self.api
            .submit::<T, _>(object, event_id, event.clone(), cb)
            .await?;
        self.db.submit::<T, _>(object, event_id, event, cb).await
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        match self.db.get::<T>(object_id).await {
            Ok(r) => return Ok(r),
            Err(crate::Error::ObjectDoesNotExist(_)) => (), // fall-through and fetch from API
            Err(e) => return Err(e),
        }
        let res = self.api.get::<T>(object_id).await?;
        self.db.create_all::<T, _>(res.clone(), &*self.db).await?;
        Ok(res)
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        if !include_heavy {
            return self
                .db
                .query::<T>(user, include_heavy, ignore_not_modified_on_server_since, q)
                .await
                .map(future::Either::Left);
        }
        Ok(future::Either::Right(
            self.api
                .query::<T>(user, include_heavy, ignore_not_modified_on_server_since, q)
                .await?
                .then({
                    let db = self.db.clone();
                    move |o| {
                        let db = db.clone();
                        async move {
                            let o = o?;
                            db.create_all::<T, _>(o.clone(), &*db).await?;
                            Ok(o)
                        }
                    }
                }),
        ))
    }

    async fn recreate<T: Object, C: crate::CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        self.db.recreate::<T, C>(time, object, cb).await?;
        self.api.recreate::<T, C>(time, object, cb).await
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        self.db.create_binary(binary_id, data.clone()).await?;
        self.api.create_binary(binary_id, data).await
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.db.get_binary(binary_id).await? {
            return Ok(Some(res));
        }
        let Some(res) = self.api.get_binary(binary_id).await? else {
            return Ok(None);
        };
        self.db.create_binary(binary_id, res.clone()).await?;
        Ok(Some(res))
    }
}
