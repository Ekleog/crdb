use crdb::{CanDoCallbacks, DbPtr, ObjectId, TypeId, User};
use ulid::Ulid;

#[derive(serde::Deserialize, serde::Serialize)]
pub struct Authenticator;

#[derive(
    Clone, Default, Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize,
)]
pub struct Foo;

#[allow(unused_variables)]
impl crdb::Object for Foo {
    type Event = FooEvent;

    fn type_ulid() -> &'static TypeId {
        static ID: TypeId = TypeId(match Ulid::from_string("01HJFF7CPZH8X0YXG2V0K4M1GA") {
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
        todo!()
    }
    async fn can_apply<'a, C: CanDoCallbacks>(
        &'a self,
        user: User,
        self_id: ObjectId,
        event: &'a Self::Event,
        db: &'a C,
    ) -> anyhow::Result<bool> {
        todo!()
    }
    async fn users_who_can_read<'a, C: CanDoCallbacks>(
        &'a self,
        db: &'a C,
    ) -> anyhow::Result<Vec<User>> {
        todo!()
    }

    fn apply(&mut self, self_id: DbPtr<Self>, event: &Self::Event) {
        todo!()
    }

    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        todo!()
    }
}

#[derive(Eq, PartialEq, deepsize::DeepSizeOf, serde::Deserialize, serde::Serialize)]
pub enum FooEvent {}

impl crdb::Event for FooEvent {
    fn required_binaries(&self) -> Vec<crdb::BinPtr> {
        Vec::new()
    }
}

crdb::db! {
    pub mod db {
        auth: super::Authenticator,
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: Db,
        objects: {
            foo: super::Foo,
        },
    }
}
