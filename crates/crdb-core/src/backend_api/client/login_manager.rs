use crate::LoginInfo;

pub trait LoginManager: 'static + waaa::Send + waaa::Sync {
    fn login_record(&self, info: LoginInfo) -> impl waaa::Future<Output = crate::Result<()>>;

    fn login_retrieve(&self) -> impl waaa::Future<Output = crate::Result<Option<LoginInfo>>>;

    fn logout_and_remove_everything(&self) -> impl waaa::Future<Output = crate::Result<()>>;
}
