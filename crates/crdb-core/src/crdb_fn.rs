pub trait CrdbFn<Arg>: waaa::Send + Fn(Arg) {}
impl<Arg, F: waaa::Send + Fn(Arg)> CrdbFn<Arg> for F {}

pub trait CrdbSyncFn<Arg>: waaa::Send + waaa::Sync + Fn(Arg) {}
impl<Arg, F: waaa::Send + waaa::Sync + Fn(Arg)> CrdbSyncFn<Arg> for F {}
