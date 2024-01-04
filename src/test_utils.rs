#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::{
    db_trait::{
        Db, DbOpError, DynNewEvent, DynNewObject, DynNewRecreation, EventId, ObjectId, TypeId,
    },
    full_object::{DynSized, FullObject},
    BinPtr, CanDoCallbacks, Object, Query, Timestamp, User,
};
use anyhow::{anyhow, Context};
use futures::prelude::Stream;
use std::{any::Any, collections::HashMap, sync::Arc};
use tokio::sync::Mutex;
use ulid::Ulid;

const fn ulid(s: &str) -> Ulid {
    match Ulid::from_string(s) {
        Ok(r) => r,
        Err(_) => panic!("const fn failed"),
    }
}

pub const OBJECT_ID_NULL: ObjectId = ObjectId(ulid("00000000000000000000000000"));
pub const OBJECT_ID_1: ObjectId = ObjectId(ulid("10000000000000000000000010"));
pub const OBJECT_ID_2: ObjectId = ObjectId(ulid("10000000000000000000000020"));
pub const OBJECT_ID_3: ObjectId = ObjectId(ulid("10000000000000000000000030"));
pub const OBJECT_ID_4: ObjectId = ObjectId(ulid("10000000000000000000000040"));
pub const OBJECT_ID_5: ObjectId = ObjectId(ulid("10000000000000000000000050"));

pub const EVENT_ID_NULL: EventId = EventId(ulid("00000000000000000000000000"));
pub const EVENT_ID_1: EventId = EventId(ulid("20000000000000000000000010"));
pub const EVENT_ID_2: EventId = EventId(ulid("20000000000000000000000020"));
pub const EVENT_ID_3: EventId = EventId(ulid("20000000000000000000000030"));
pub const EVENT_ID_4: EventId = EventId(ulid("20000000000000000000000040"));
pub const EVENT_ID_5: EventId = EventId(ulid("20000000000000000000000050"));

pub const TYPE_ID_NULL: TypeId = TypeId(ulid("00000000000000000000000000"));
pub const TYPE_ID_1: TypeId = TypeId(ulid("30000000000000000000000010"));
pub const TYPE_ID_2: TypeId = TypeId(ulid("30000000000000000000000020"));
pub const TYPE_ID_3: TypeId = TypeId(ulid("30000000000000000000000030"));
pub const TYPE_ID_4: TypeId = TypeId(ulid("30000000000000000000000040"));
pub const TYPE_ID_5: TypeId = TypeId(ulid("30000000000000000000000050"));

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    bolero::generator::TypeGenerator,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct TestObject1(
    #[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] pub Vec<u8>,
);

#[derive(
    Clone,
    Debug,
    Eq,
    PartialEq,
    bolero::generator::TypeGenerator,
    deepsize::DeepSizeOf,
    serde::Deserialize,
    serde::Serialize,
)]
pub enum TestEvent1 {
    Set(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Append(#[generator(bolero::generator::gen_with::<Vec<_>>().len(8_usize))] Vec<u8>),
    Clear,
}

impl TestObject1 {
    pub fn new(v: Vec<u8>) -> TestObject1 {
        TestObject1(v)
    }
    pub fn stub_1() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_2() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_3() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_4() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
    pub fn stub_5() -> TestObject1 {
        TestObject1(b"10000001".to_vec())
    }
}

#[allow(unused_variables)] // TODO: remove?
impl Object for TestObject1 {
    type Event = TestEvent1;

    fn type_ulid() -> &'static ulid::Ulid {
        &TYPE_ID_1.0
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        event: &'a Self::Event,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        Ok(Vec::new())
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            TestEvent1::Set(v) => self.0 = v.clone(),
            TestEvent1::Append(v) => self.0.extend(v.iter().cloned()),
            TestEvent1::Clear => self.0.clear(),
        }
    }

    fn is_heavy(&self) -> bool {
        self.0.len() > 10
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl crate::Event for TestEvent1 {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

struct MemDbImpl {
    // Some(e) for a real event, None for a creation snapshot
    events: HashMap<EventId, Option<Arc<dyn DynSized>>>,
    objects: HashMap<ObjectId, FullObject>,
    binaries: HashMap<BinPtr, Arc<Vec<u8>>>,
}

pub struct MemDb(Arc<Mutex<MemDbImpl>>);

impl MemDb {
    pub fn new() -> MemDb {
        MemDb(Arc::new(Mutex::new(MemDbImpl {
            events: HashMap::new(),
            objects: HashMap::new(),
            binaries: HashMap::new(),
        })))
    }
}

fn eq<T: 'static + Any + Send + Sync + Eq>(
    l: &dyn DynSized,
    r: &dyn DynSized,
) -> anyhow::Result<bool> {
    Ok(l.ref_to_any()
        .downcast_ref::<T>()
        .context("downcasting lhs")?
        == r.ref_to_any()
            .downcast_ref::<T>()
            .context("downcasting rhs")?)
}

impl Db for MemDb {
    async fn new_objects(&self) -> impl Send + Stream<Item = DynNewObject> {
        futures::stream::empty()
    }

    async fn new_events(&self) -> impl Send + Stream<Item = DynNewEvent> {
        futures::stream::empty()
    }

    async fn new_recreations(&self) -> impl Send + Stream<Item = DynNewRecreation> {
        futures::stream::empty()
    }

    async fn unsubscribe(&self, _ptr: ObjectId) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        _cb: &C,
    ) -> Result<(), DbOpError> {
        let mut this = self.0.lock().await;

        // First, check for duplicates
        if let Some(o) = this.objects.get(&id) {
            let c = o.creation_info();
            if created_at != c.created_at {
                return Err(DbOpError::Other(anyhow!(
                    "object {id:?} already existed with different creation time"
                )));
            }
            if !eq::<T>(&*c.creation, &*object as _).map_err(DbOpError::Other)? {
                return Err(DbOpError::Other(anyhow!(
                    "object {id:?} already existed with different creation value"
                )));
            }
            return Ok(());
        }
        if let Some(_) = this.events.get(&created_at) {
            return Err(DbOpError::Other(anyhow!(
                "creating object at the same time as an existing event"
            )));
        }

        // This is a new insert, do it
        this.objects
            .insert(id, FullObject::new(id, created_at, object));
        this.events.insert(created_at, None);

        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> Result<(), DbOpError> {
        let mut this = self.0.lock().await;
        if let Some(e) = this.events.get(&event_id) {
            if !eq::<T::Event>(&*e, &*event as _).map_err(DbOpError::Other)? {
                return Err(DbOpError::Other(anyhow!(
                    "event already inserted with different value"
                )));
            }
            return Ok(());
        }
        match this.objects.get(&object) {
            None => Err(DbOpError::Other(anyhow!(
                "object not yet present in database"
            ))),
            Some(o) if o.creation_info().created_at >= event_id => {
                Err(DbOpError::Other(anyhow!("event is too early for object")))
            }
            Some(o) => {
                o.apply::<T>(event_id, event.clone())
                    .map_err(DbOpError::Other)?;
                this.events.insert(event_id, Some(event));
                Ok(())
            }
        }
    }

    async fn get<T: Object>(&self, ptr: ObjectId) -> anyhow::Result<Option<FullObject>> {
        Ok(self.0.lock().await.objects.get(&ptr).cloned())
    }

    async fn query<T: Object>(
        &self,
        _user: User,
        _include_heavy: bool,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: Query,
    ) -> anyhow::Result<impl Stream<Item = anyhow::Result<FullObject>>> {
        // TODO
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        _cb: &C,
    ) -> anyhow::Result<()> {
        let this = self.0.lock().await;
        let Some(o) = this.objects.get(&object) else {
            anyhow::bail!("object does not exist");
        };
        o.recreate_at::<T>(time).context("recreating object")?;
        Ok(())
    }

    async fn create_binary(&self, _id: BinPtr, _value: Arc<Vec<u8>>) -> anyhow::Result<()> {
        unimplemented!()
    }

    async fn get_binary(&self, _ptr: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        unimplemented!()
    }
}
