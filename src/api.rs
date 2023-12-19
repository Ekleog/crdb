use uuid::Uuid;

pub struct User(Uuid);

pub trait Event: for<'a> serde::Deserialize<'a> + serde::Serialize {}

pub trait Object: Default + for<'a> serde::Deserialize<'a> + serde::Serialize {
    type Event: Event;

    fn can_apply(&self, user: &User, event: &Self::Event) -> anyhow::Result<bool>;
    fn apply(&mut self, event: &Self::Event, force_snapshot: impl Fn()) -> anyhow::Result<()>;
    fn is_heavy(&self) -> anyhow::Result<bool>;
}
