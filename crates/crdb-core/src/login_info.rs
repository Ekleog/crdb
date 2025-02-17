use crate::{SessionToken, User};
use std::sync::Arc;

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct LoginInfo {
    pub url: Arc<String>,
    pub user: User,
    pub token: SessionToken,
}
