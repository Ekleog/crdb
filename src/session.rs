use crate::{SessionRef, SessionToken, User};
use web_time::SystemTime;

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
    #[cfg_attr(feature = "_tests", arbitrary(with = |u: &mut arbitrary::Unstructured| u.arbitrary::<Option<std::time::Duration>>().map(|d| d.map(|d| SystemTime::UNIX_EPOCH + d))))]
    pub expiration_time: Option<SystemTime>,
}

#[derive(Clone, Debug, Eq, Hash, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct Session {
    pub user_id: User,
    pub session_ref: SessionRef,
    pub session_name: String,
    pub login_time: SystemTime,
    pub last_active: SystemTime,
    pub expiration_time: Option<SystemTime>,
}

impl Session {
    pub fn new(s: NewSession) -> Session {
        let now = SystemTime::now();
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
