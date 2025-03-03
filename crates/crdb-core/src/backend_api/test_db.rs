use crate::Object;

pub trait TestDb: 'static + waaa::Send + waaa::Sync {
    fn assert_invariants_generic(&self) -> impl waaa::Future<Output = ()>;
    fn assert_invariants_for<T: Object>(&self) -> impl waaa::Future<Output = ()>;
}
