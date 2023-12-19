use std::marker::PhantomData;

use uuid::Uuid;

pub struct User {
    pub id: Uuid,
}

pub trait Object: Default + for<'a> serde::Deserialize<'a> + serde::Serialize {
    type Event: for<'a> serde::Deserialize<'a> + serde::Serialize;

    fn uuid() -> &'static Uuid;
    fn can_apply(&self, user: &User, event: &Self::Event) -> anyhow::Result<bool>;
    fn apply(&mut self, event: &Self::Event, force_snapshot: impl Fn()) -> anyhow::Result<()>;
    fn is_heavy(&self) -> anyhow::Result<bool>;
}

pub struct DbPtr<T> {
    #[doc(hidden)]
    pub id: Uuid,
    _phantom: PhantomData<T>,
}
