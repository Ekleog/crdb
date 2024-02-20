use futures::Stream;
use std::{future::Future, pin::Pin};

#[cfg(not(target_arch = "wasm32"))]
pub trait CrdbSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send> CrdbSend for T {}

#[cfg(target_arch = "wasm32")]
pub trait CrdbSend {}
#[cfg(target_arch = "wasm32")]
impl<T> CrdbSend for T {}

#[cfg(not(target_arch = "wasm32"))]
pub trait CrdbSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync> CrdbSync for T {}

#[cfg(target_arch = "wasm32")]
pub trait CrdbSync {}
#[cfg(target_arch = "wasm32")]
impl<T> CrdbSync for T {}

pub trait CrdbFuture: CrdbSend + Future {}
impl<F: CrdbSend + Future> CrdbFuture for F {}

pub trait CrdbStream: CrdbSend + Stream {}
impl<F: CrdbSend + Stream> CrdbStream for F {}

#[cfg(feature = "client")]
pub(crate) trait CrdbFn<Arg>: CrdbSend + Fn(Arg) {}
#[cfg(feature = "client")]
impl<Arg, F: CrdbSend + Fn(Arg)> CrdbFn<Arg> for F {}

pub trait CrdbFutureExt: CrdbFuture {
    fn boxed_crdb<'a>(self) -> Pin<Box<dyn 'a + CrdbFuture<Output = Self::Output>>>
    where
        Self: Sized + 'a,
    {
        Box::pin(self)
    }
}

impl<T: CrdbFuture> CrdbFutureExt for T {}

pub fn spawn<F>(f: F)
where
    F: 'static + CrdbFuture<Output = ()>,
{
    #[cfg(not(target_arch = "wasm32"))]
    tokio::task::spawn(f);

    #[cfg(target_arch = "wasm32")]
    wasm_bindgen_futures::spawn_local(async move {
        f.await;
    });
}
