mod client_server_object_manager;
mod object_permissions;
mod reencoder;
mod server_object_manager;
mod ulid_checker;

pub use client_server_object_manager::*;
pub use object_permissions::*;
pub use reencoder::*;
pub use server_object_manager::*;
pub use ulid_checker::*;

// This module needs to actually be public, because the `db` macro needs to be
// able to implement it. However, making it doc(hidden) makes it look as though
// it is actually sealed in the documentation, which is good because we don't want
// users to rely on any stability guarantees there.
#[doc(hidden)]
pub mod private {
    pub trait Sealed {}
}

pub trait BackendDispatcher:
    'static
    + Send
    + Sync
    + private::Sealed
    + ClientServerObjectManager
    + ObjectPermissions
    + Reencoder
    + ServerObjectManager
    + UlidChecker
{
}
