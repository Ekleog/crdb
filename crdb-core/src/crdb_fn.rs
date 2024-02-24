pub trait CrdbFn<Arg>: waaaa::Send + Fn(Arg) {}
impl<Arg, F: waaaa::Send + Fn(Arg)> CrdbFn<Arg> for F {}

pub trait CrdbSyncFn<Arg>: waaaa::Send + waaaa::Sync + Fn(Arg) {}
impl<Arg, F: waaaa::Send + waaaa::Sync + Fn(Arg)> CrdbSyncFn<Arg> for F {}
