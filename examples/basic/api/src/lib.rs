use crdb::{CanDoCallbacks, User};
use ulid::Ulid;

pub struct Authenticator;

#[derive(Clone, Default, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Foo;

#[allow(unused_variables)]
impl crdb::Object for Foo {
    type Event = FooEvent;

    fn ulid() -> &'static Ulid {
        static ID: Ulid = match Ulid::from_string("01HJFF7CPZH8X0YXG2V0K4M1GA") {
            Ok(id) => id,
            Err(_) => panic!(),
        };
        &ID
    }

    fn can_create<C: CanDoCallbacks>(&self, user: User, db: &C) -> anyhow::Result<bool> {
        todo!()
    }
    fn can_apply<C: CanDoCallbacks>(
        &self,
        user: &User,
        event: &Self::Event,
        db: &C,
    ) -> anyhow::Result<bool> {
        todo!()
    }
    fn users_who_can_read<C: CanDoCallbacks>(&self) -> anyhow::Result<Vec<User>> {
        todo!()
    }
    fn apply(&mut self, event: &Self::Event) {
        todo!()
    }
    fn is_heavy(&self) -> anyhow::Result<bool> {
        todo!()
    }
}

#[derive(Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum FooEvent {}

crdb::db! {
    pub mod db {
        auth: super::Authenticator,
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: Db,
        objects: [
            super::Foo,
        ],
    }
}
