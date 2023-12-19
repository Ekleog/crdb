struct Authenticator;

struct Foo;
struct Bar;
struct Baz;

crdb::db! {
    mod db auth Authenticator {
        Foo,
        Bar,
        Baz,
    }
}
