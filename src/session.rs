use crate::{SessionRef, SessionToken, Timestamp, User};

impl SessionToken {
    #[cfg(feature = "server")]
    pub fn new() -> SessionToken {
        use rand::Rng;
        SessionToken(ulid::Ulid::from_bytes(rand::thread_rng().gen()))
    }
}

#[derive(Clone, Debug)]
#[cfg_attr(feature = "_tests", derive(arbitrary::Arbitrary))]
pub struct NewSession {
    pub user_id: User,
    pub session_name: String,
    pub expiration_time: Option<Timestamp>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Session {
    pub user_id: User,
    pub session_ref: SessionRef,
    pub session_name: String,
    // TODO(low): get rid of Timestamp struct, these should be replaced with SystemTime and are almost the last remaining uses
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
