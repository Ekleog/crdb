use super::SqlDb;

#[sqlx::test]
async fn smoke_test(db: sqlx::PgPool) {
    SqlDb::connect(db).await.expect("connecting to db");
}
