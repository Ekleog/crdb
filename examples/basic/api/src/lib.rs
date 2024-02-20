use anyhow::Context;
use crdb::{fts::SearchableString, BinPtr, CanDoCallbacks, DbPtr, ObjectId, TypeId, User};
use std::collections::{BTreeSet, HashSet};
use ulid::Ulid;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct AuthInfo {
    pub user: User,
    pub pass: String,
}

#[derive(
    Clone, Debug, Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize,
)]
pub struct Item {
    pub owner: User,
    pub text: SearchableString,
    pub tags: BTreeSet<DbPtr<Tag>>,
    pub file: Option<BinPtr>,
}

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
        _self_id: ObjectId,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        if user != self.owner {
            return Ok(false);
        }
        for tag in self.tags.iter() {
            let tag = db.get(*tag).await.context("fetching tag")?;
            if !tag.users_who_can_edit.contains(&user) {
                return Ok(false);
            }
        }
        Ok(true)
    }
    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        _self_id: ObjectId,
        event: &'a Self::Event,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        if user == self.owner {
            return Ok(true);
        }
        if matches!(event, ItemEvent::SetOwner(_)) {
            return Ok(false);
        }
        for tag in self.tags.iter() {
            let tag = db.get(*tag).await.context("fetching tag")?;
            if tag.users_who_can_edit.contains(&user) {
                return Ok(true);
            }
        }
        Ok(false)
    }
    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<HashSet<User>> {
        let mut res = HashSet::new();
        res.insert(self.owner);
        for tag in self.tags.iter() {
            let tag = db.get(*tag).await.context("fetching tag")?;
            res.extend(tag.users_who_can_read.iter().copied());
            res.extend(tag.users_who_can_edit.iter().copied());
        }
        Ok(res)
    }

    fn apply(&mut self, _self_id: DbPtr<Self>, event: &Self::Event) {
        match event {
            ItemEvent::SetOwner(user) => {
                self.owner = *user;
            }
            ItemEvent::SetText(text) => {
                self.text = SearchableString::from(text);
            }
            ItemEvent::AddTag(tag) => {
                self.tags.insert(*tag);
            }
            ItemEvent::RmTag(tag) => {
                self.tags.remove(tag);
            }
            ItemEvent::SetFile(file) => {
                self.file = *file;
            }
        }
    }

    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        if let Some(f) = &self.file {
            vec![*f]
        } else {
            Vec::new()
        }
    }
}

#[derive(Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub enum ItemEvent {
    SetOwner(User),
    SetText(String),
    AddTag(DbPtr<Tag>),
    RmTag(DbPtr<Tag>),
    SetFile(Option<BinPtr>),
}

impl crdb::Event for ItemEvent {
    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        Vec::new()
    }
}

#[derive(Clone, Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub struct Tag {
    name: String,
    users_who_can_read: BTreeSet<User>,
    users_who_can_edit: BTreeSet<User>,
}

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
