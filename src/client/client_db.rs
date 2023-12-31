use super::{ApiDb, Authenticator, LocalDb};
use crate::{
    api,
    cache::CacheDb,
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewSnapshot, EventId, ObjectId},
    full_object::FullObject,
    BinPtr, Object, Query, Timestamp, User,
};
use futures::{future, Stream, StreamExt};
use std::sync::Arc;

pub struct ClientDb<A: Authenticator> {
    api: Arc<ApiDb<A>>,
    db: Arc<CacheDb<LocalDb>>,
}

impl<A: Authenticator> ClientDb<A> {
    pub async fn connect<C: api::Config>(
        base_url: Arc<String>,
        auth: Arc<A>,
        cache_watermark: usize,
    ) -> anyhow::Result<ClientDb<A>> {
        C::check_ulids();
        let api = Arc::new(ApiDb::connect(base_url, auth).await?);
        let db = Arc::new(CacheDb::new::<C>(Arc::new(LocalDb::new()), cache_watermark));
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
    async fn new_objects(&self) -> impl Stream<Item = DynNewObject> {
        self.api.new_objects().await
    }

    async fn new_events(&self) -> impl Send + Stream<Item = DynNewEvent> {
        self.api.new_events().await
    }

    async fn new_snapshots(&self) -> impl Send + Stream<Item = DynNewSnapshot> {
        self.api.new_snapshots().await
    }

    async fn unsubscribe(&self, ptr: ObjectId) -> anyhow::Result<()> {
        self.api.unsubscribe(ptr).await
    }

    async fn create<T: Object>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        precomputed_can_read: Option<Vec<User>>,
    ) -> anyhow::Result<()> {
        self.api
            .create(id, created_at, object.clone(), None)
            .await?;
        self.db
            .create(id, created_at, object, precomputed_can_read)
            .await
    }

    async fn submit<T: Object>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
    ) -> anyhow::Result<()> {
        self.api
            .submit::<T>(object, event_id, event.clone())
            .await?;
        self.db.submit::<T>(object, event_id, event).await
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        if let Some(res) = self.db.get::<T>(ptr).await? {
            return Ok(Some(res));
        }
        let Some(o) = self.api.get::<T>(ptr).await? else {
            return Ok(None);
        };
        self.db.create_all::<T>(o.clone()).await?;
        Ok(Some(o))
    }

    async fn query<T: Object>(
        &self,
        user: User,
        include_heavy: bool,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
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
                            db.create_all::<T>(o.clone()).await?;
                            Ok(o)
                        }
                    }
                }),
        ))
    }

    async fn snapshot<T: Object>(&self, time: Timestamp, object: ObjectId) -> anyhow::Result<()> {
        self.db.snapshot::<T>(time, object).await?;
        self.api.snapshot::<T>(time, object).await
    }

    async fn create_binary(&self, id: BinPtr, value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        self.db.create_binary(id, value.clone()).await?;
        self.api.create_binary(id, value).await
    }

    async fn get_binary(&self, ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        if let Some(res) = self.db.get_binary(ptr).await? {
            return Ok(Some(res));
        }
        let Some(res) = self.api.get_binary(ptr).await? else {
            return Ok(None);
        };
        self.db.create_binary(ptr, res.clone()).await?;
        Ok(Some(res))
    }
}
