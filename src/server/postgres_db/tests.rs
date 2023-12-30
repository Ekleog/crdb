use super::PostgresDb;

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    PostgresDb::connect(db).await.expect("connecting to db");
}
