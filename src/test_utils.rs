#![allow(dead_code)] // test utils can be or not eb used but get copy-pasted anyway

use crate::{
    db_trait::{Db, DynNewEvent, DynNewObject, DynNewRecreation},
    error::ResultExt,
    full_object::{DynSized, FullObject},
    BinPtr, CanDoCallbacks, DbPtr, EventId, Object, ObjectId, Query, Timestamp, TypeId, User,
};
use anyhow::Context;
use futures::prelude::Stream;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    sync::Arc,
};
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

impl Object for TestObject1 {
    type Event = TestEvent1;

    fn type_ulid() -> &'static TypeId {
        &TYPE_ID_1
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
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
pub struct TestObjectPerms(pub User);

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
pub enum TestEventPerms {
    Set(User),
}

impl Object for TestObjectPerms {
    type Event = TestEventPerms;

    fn type_ulid() -> &'static TypeId {
        &TYPE_ID_2
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        Ok(vec![self.0])
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            TestEventPerms::Set(u) => self.0 = *u,
        }
    }

    fn is_heavy(&self) -> bool {
        false
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl crate::Event for TestEventPerms {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

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
pub struct TestObjectDelegatePerms(pub DbPtr<TestObjectPerms>);

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
pub enum TestEventDelegatePerms {
    Set(DbPtr<TestObjectPerms>),
}

impl Object for TestObjectDelegatePerms {
    type Event = TestEventDelegatePerms;

    fn type_ulid() -> &'static TypeId {
        &TYPE_ID_3
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        _user: User,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }

    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        let remote = match db.get(self.0).await {
            Ok(r) => r,
            Err(crate::Error::Other(e)) => panic!("got unexpected error {e:?}"),
            _ => return Ok(Vec::new()), // protocol not respected
        };
        Ok(vec![remote.0])
    }

    fn apply(&mut self, event: &Self::Event) {
        match event {
            TestEventDelegatePerms::Set(p) => self.0 = *p,
        }
    }

    fn is_heavy(&self) -> bool {
        false
    }

    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

impl crate::Event for TestEventDelegatePerms {
    fn required_binaries(&self) -> Vec<BinPtr> {
        Vec::new()
    }
}

crate::db! {
    pub mod db {
        auth: (),
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: ClientDb,
        objects: {
            test1: super::TestObject1,
            perms: super::TestObjectPerms,
            delegate_perms: super::TestObjectDelegatePerms,
        },
    }
}

struct MemDbImpl {
    // Some(e) for a real event, None for a creation snapshot
    events: HashMap<EventId, (ObjectId, Option<Arc<dyn DynSized>>)>,
    objects: HashMap<ObjectId, (TypeId, FullObject)>,
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

    pub async fn recreate_all<T: Object>(&self, time: Timestamp) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        let this = &mut *this; // disable implicit deref+reborrow and get a real &mut, for borrow splitting
        EventId::last_id_at(time)?;
        for (ty, o) in this.objects.values_mut() {
            if ty == T::type_ulid() {
                let mut events_before = o
                    .changes_clone()
                    .into_iter()
                    .map(|(e, _)| e)
                    .collect::<HashSet<EventId>>();
                events_before.insert(o.created_at());
                o.recreate_at::<T>(time).wrap_context("recreating object")?;
                let mut events_after = o
                    .changes_clone()
                    .into_iter()
                    .map(|(e, _)| e)
                    .collect::<HashSet<EventId>>();
                events_after.insert(o.created_at());
                // Discard all removed events from self.events too
                for e in events_before.difference(&events_after) {
                    this.events.remove(&e);
                }
                // And mark the new "creation event" as a creation event
                this.events.get_mut(&o.created_at()).unwrap().1 = None;
            }
        }
        Ok(())
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
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        _cb: &C,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;

        // First, check for duplicates
        if let Some((ty, o)) = this.objects.get(&object_id) {
            let c = o.creation_info();
            if ty != T::type_ulid()
                || created_at != c.created_at
                || !eq::<T>(&*c.creation, &*object as _).unwrap()
            {
                return Err(crate::Error::ObjectAlreadyExists(object_id));
            }
            return Ok(());
        }
        if let Some(_) = this.events.get(&created_at) {
            return Err(crate::Error::EventAlreadyExists(created_at));
        }

        // This is a new insert, do it
        this.objects.insert(
            object_id,
            (
                *T::type_ulid(),
                FullObject::new(object_id, created_at, object),
            ),
        );
        this.events.insert(created_at, (object_id, None));

        Ok(())
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        _cb: &C,
    ) -> crate::Result<()> {
        let mut this = self.0.lock().await;
        match this.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, o)) if o.creation_info().created_at >= event_id => {
                Err(crate::Error::EventTooEarly {
                    object_id,
                    event_id,
                    created_at: o.creation_info().created_at,
                })
            }
            Some((_, o)) => {
                if let Some((o, e)) = this.events.get(&event_id) {
                    let Some(e) = e else {
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    };
                    if *o != object_id || !eq::<T::Event>(&**e, &*event as _).unwrap_or(false) {
                        return Err(crate::Error::EventAlreadyExists(event_id));
                    }
                    return Ok(());
                }
                o.apply::<T>(event_id, event.clone())?;
                this.events.insert(event_id, (object_id, Some(event)));
                Ok(())
            }
        }
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        match self.0.lock().await.objects.get(&object_id) {
            None => Err(crate::Error::ObjectDoesNotExist(object_id)),
            Some((ty, _)) if ty != T::type_ulid() => Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            }),
            Some((_, o)) => Ok(o.clone()),
        }
    }

    async fn query<T: Object>(
        &self,
        _user: User,
        _include_heavy: bool,
        _ignore_not_modified_on_server_since: Option<Timestamp>,
        _q: Query,
    ) -> anyhow::Result<impl Stream<Item = crate::Result<FullObject>>> {
        // TODO
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object_id: ObjectId,
        _cb: &C,
    ) -> crate::Result<()> {
        let this = self.0.lock().await;
        EventId::last_id_at(time)?; // start by checking the timestamp
        let Some((ty, o)) = this.objects.get(&object_id) else {
            return Err(crate::Error::ObjectDoesNotExist(object_id));
        };
        if ty != T::type_ulid() {
            return Err(crate::Error::WrongType {
                object_id,
                expected_type_id: *T::type_ulid(),
                real_type_id: *ty,
            });
        }
        o.recreate_at::<T>(time).wrap_context("recreating object")?;
        Ok(())
    }

    async fn create_binary(&self, _binary_id: BinPtr, _data: Arc<Vec<u8>>) -> crate::Result<()> {
        unimplemented!()
    }

    async fn get_binary(&self, _binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        unimplemented!()
    }
}
