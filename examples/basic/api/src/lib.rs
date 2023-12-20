struct Authenticator;

struct Foo;
struct Bar;
struct Baz;

crdb::db! {
    mod db {
        auth: super::Authenticator,
        server_config: ServerConfig,
        client_db: Db,
        objects: [
            Foo,
            Bar,
            Baz,
        ],
    }
}
