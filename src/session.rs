use crate::{ids, Timestamp, User};
use ulid::Ulid;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionToken(Ulid);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct SessionRef(Ulid);

ids::impl_for_id!(SessionToken);
ids::impl_for_id!(SessionRef);

impl SessionToken {
    #[cfg(feature = "server")]
    pub fn new() -> SessionToken {
        use rand::Rng;
        SessionToken(Ulid::from_bytes(rand::thread_rng().gen()))
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "_tests", derive(bolero::generator::TypeGenerator))]
pub struct NewSession {
    pub user_id: User,
    pub session_name: String,
    pub expiration_time: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct Session {
    pub user_id: User,
    pub session_ref: SessionRef,
    pub session_name: String,
    pub login_time: Timestamp,
    pub last_active: Timestamp,
    pub expiration_time: Option<Timestamp>,
}

impl Session {
    pub fn new(s: NewSession) -> Session {
        let now = Timestamp::now();
        Session {
            user_id: s.user_id,
            session_ref: SessionRef::now(),
            session_name: s.session_name,
            login_time: now,
            last_active: now,
            expiration_time: s.expiration_time,
        }
    }
}
