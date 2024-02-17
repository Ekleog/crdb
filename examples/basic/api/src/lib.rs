use std::collections::HashSet;

use crdb::{fts::SearchableString, BinPtr, CanDoCallbacks, DbPtr, ObjectId, TypeId, User};
use ulid::Ulid;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct AuthInfo {
    pub user: User,
    pub pass: String,
}

#[derive(Clone, Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub struct Item {
    owner: User,
    text: SearchableString,
    tags: Vec<DbPtr<Tag>>,
    file: Option<BinPtr>,
}

#[allow(unused_variables)]
impl crdb::Object for Item {
    type Event = ItemEvent;

    fn type_ulid() -> &'static TypeId {
        static ID: TypeId = TypeId(match Ulid::from_string("01HPVWYMX443M3NRJRXF8NBMGT") {
            Ok(id) => id,
            Err(_) => panic!(),
        });
        &ID
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        self_id: ObjectId,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }
    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        self_id: ObjectId,
        event: &'a Self::Event,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        unimplemented!()
    }
    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<HashSet<User>> {
        unimplemented!()
    }

    fn apply(&mut self, self_id: DbPtr<Self>, event: &Self::Event) {
        unimplemented!()
    }

    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        unimplemented!()
    }
}

#[derive(Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub enum ItemEvent {}

impl crdb::Event for ItemEvent {
    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        Vec::new()
    }
}

#[derive(Clone, Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub struct Tag {
    name: String,
    users_who_can_read: HashSet<User>,
    users_who_can_edit: HashSet<User>,
}

#[allow(unused_variables)]
impl crdb::Object for Tag {
    type Event = TagEvent;

    fn type_ulid() -> &'static TypeId {
        static ID: TypeId = TypeId(match Ulid::from_string("01HPVX2YZ0ZXJWWJDN6GYS096H") {
            Ok(id) => id,
            Err(_) => panic!(),
        });
        &ID
    }

    async fn can_create<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        _self_id: ObjectId,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        Ok(self.users_who_can_edit.contains(&user))
    }
    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        _self_id: ObjectId,
        _event: &'a Self::Event,
        _db: &'a C,
    ) -> anyhow::Result<bool> {
        Ok(self.users_who_can_edit.contains(&user))
    }
    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        _db: &'a C,
    ) -> anyhow::Result<HashSet<User>> {
        Ok(self
            .users_who_can_read
            .iter()
            .copied()
            .chain(self.users_who_can_edit.iter().copied())
            .collect())
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            TagEvent::Rename(name) => {
                self.name = name.clone();
            }
            TagEvent::AddReader(user) => {
                self.users_who_can_read.insert(*user);
            }
            TagEvent::RmReader(user) => {
                self.users_who_can_read.remove(user);
            }
            TagEvent::AddEditor(user) => {
                self.users_who_can_edit.insert(*user);
            }
            TagEvent::RmEditor(user) => {
                self.users_who_can_edit.remove(user);
            }
        }
    }

    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        Vec::new()
    }
}

#[derive(Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub enum TagEvent {
    Rename(String),
    AddReader(User),
    RmReader(User),
    AddEditor(User),
    RmEditor(User),
}

impl crdb::Event for TagEvent {
    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        Vec::new()
    }
}

crdb::db! {
    pub mod db {
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: Db,
        objects: {
            item: super::Item,
            tag: super::Tag,
        },
    }
}
