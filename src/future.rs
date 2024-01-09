use futures::Stream;
use std::{future::Future, pin::Pin};

#[cfg(not(target_arch = "wasm32"))]
pub trait CrdbFuture: Send + Future {}
#[cfg(not(target_arch = "wasm32"))]
impl<F: Send + Future> CrdbFuture for F {}

#[cfg(target_arch = "wasm32")]
pub trait CrdbFuture: Future {}
#[cfg(target_arch = "wasm32")]
impl<F: Future> CrdbFuture for F {}

#[cfg(not(target_arch = "wasm32"))]
pub trait CrdbStream: Send + Stream {}
#[cfg(not(target_arch = "wasm32"))]
impl<F: Send + Stream> CrdbStream for F {}

#[cfg(target_arch = "wasm32")]
pub trait CrdbStream: Stream {}
#[cfg(target_arch = "wasm32")]
impl<F: Stream> CrdbStream for F {}

pub trait CrdbFutureExt: CrdbFuture {
    fn boxed_crdb<'a>(self) -> Pin<Box<dyn 'a + CrdbFuture<Output = Self::Output>>>
    where
        Self: Sized + 'a,
    {
        Box::pin(self)
    }
}

impl<T: CrdbFuture> CrdbFutureExt for T {}

#[cfg(not(target_arch = "wasm32"))]
pub fn spawn<F: 'static + CrdbFuture>(f: F) -> tokio::task::JoinHandle<F::Output>
where
    F::Output: Send,
{
    tokio::task::spawn(f)
}

#[cfg(target_arch = "wasm32")]
pub fn spawn<F: 'static + CrdbFuture>(f: F) -> tokio::task::JoinHandle<F::Output> {
    tokio::task::spawn_local(f)
}
