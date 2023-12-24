#[doc(hidden)]
#[macro_export]
macro_rules! generate_client {
    ($($t:tt)*) => {
        // TODO: generate something like an "impl" of client::Db that just forwards to the crate::Db impl of Cache<Api>
        // set_new_* MUST be replaced by one function for each object/event type, so that the user can properly handle them.
    };
}
