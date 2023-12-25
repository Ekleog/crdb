pub struct Authenticator;

pub struct Foo;
pub struct Bar;
pub struct Baz;

crdb::db! {
    pub mod db {
        auth: super::Authenticator,
        api_config: ApiConfig,
        server_config: ServerConfig,
        client_db: Db,
        objects: [
            Foo,
            Bar,
            Baz,
        ],
    }
}
