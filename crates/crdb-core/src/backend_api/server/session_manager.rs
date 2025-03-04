use web_time::SystemTime;

use crate::{Session, SessionRef, SessionToken, User};

pub trait SessionManager: 'static + waaa::Send + waaa::Sync {
    fn session_login(
        &self,
        session: Session,
    ) -> impl '_ + waaa::Future<Output = crate::Result<(SessionToken, SessionRef)>>;

    fn session_resume(
        &self,
        token: SessionToken,
    ) -> impl '_ + waaa::Future<Output = crate::Result<Session>>;

    fn session_mark_active(
        &self,
        token: SessionToken,
        at: SystemTime,
    ) -> impl '_ + waaa::Future<Output = crate::Result<()>>;

    fn session_rename<'a>(
        &'a self,
        token: SessionToken,
        new_name: &'a str,
    ) -> impl 'a + waaa::Future<Output = crate::Result<()>>;

    fn list_sessions(
        &self,
        user: User,
    ) -> impl '_ + waaa::Future<Output = crate::Result<Vec<Session>>>;

    fn session_disconnect(
        &self,
        user: User,
        session: SessionRef,
    ) -> impl '_ + waaa::Future<Output = crate::Result<()>>;
}
