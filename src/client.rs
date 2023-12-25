#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ( $authenticator:ty | $api_config:ident | $client_db:ident | $($object:ty),* ) => {
        // TODO: generate something like an "impl" of client::Db that just forwards to the crate::Db impl of Cache<IndexedDbCache<Api>>
        // set_new_* MUST be replaced by one function for each object/event type, so that the user can properly handle them.
        // TODO: also have a way to force a server round-trip NOW, for eg. permissions change
        // TODO: use the async_broadcast crate with overflow disabled to fan-out in a blocking manner the new_object/event notifications
    };
}
