use crate::{SessionRef, User};
use web_time::SystemTime;

#[cfg(feature = "arbitrary")]
fn any_system_time_opt(u: &mut arbitrary::Unstructured) -> arbitrary::Result<Option<SystemTime>> {
    let d = u.arbitrary::<Option<web_time::Duration>>()?;
    let Some(d) = d else {
        return Ok(None);
    };
    Ok(SystemTime::UNIX_EPOCH.checked_add(d))
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
#[cfg_attr(feature = "arbitrary", derive(arbitrary::Arbitrary))]
pub struct NewSession {
    pub user_id: User,
    pub session_name: String,
    #[cfg_attr(feature = "arbitrary", arbitrary(with = any_system_time_opt))]
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
