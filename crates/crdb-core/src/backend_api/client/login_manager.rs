use crate::LoginInfo;

pub trait LoginManager: 'static + waaaa::Send + waaaa::Sync {
    fn login_record(&self, info: LoginInfo) -> impl waaaa::Future<Output = crate::Result<()>>;

    fn login_retrieve(&self) -> impl waaaa::Future<Output = crate::Result<Option<LoginInfo>>>;

    fn logout_and_remove_everything(&self) -> impl waaaa::Future<Output = crate::Result<()>>;
}
