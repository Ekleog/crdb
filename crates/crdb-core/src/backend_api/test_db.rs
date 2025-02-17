use crate::Object;

pub trait TestDb: 'static + waaaa::Send + waaaa::Sync {
    fn assert_invariants_generic(&self) -> impl waaaa::Future<Output = ()>;
    fn assert_invariants_for<T: Object>(&self) -> impl waaaa::Future<Output = ()>;
}
