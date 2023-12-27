use super::ObjectCache;
use crate::db_trait::{Db, DynNewEvent, DynNewObject, DynNewSnapshot};
use std::future::Future;

pub trait CacheConfig {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `create` method with the proper type and the fields from `o`.
    fn create(
        cache: &mut ObjectCache,
        o: DynNewObject,
    ) -> impl Send + Future<Output = anyhow::Result<bool>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `submit` method with the proper type and the fields from `o`.
    fn submit<D: Db>(
        db: Option<&D>,
        cache: &mut ObjectCache,
        e: DynNewEvent,
    ) -> impl Send + Future<Output = anyhow::Result<bool>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `snapshot` method with the proper type and the fields from `s`.
    fn snapshot(
        cache: &mut ObjectCache,
        s: DynNewSnapshot,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `create` method with the proper type and the fields from `o`.
    fn create_in_db<D: Db>(
        db: &D,
        o: DynNewObject,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `submit` method with the proper type and the fields from `o`.
    fn submit_in_db<D: Db>(
        db: &D,
        e: DynNewEvent,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;

    /// Auto-generated by `crdb::db!`.
    ///
    /// Calls `cache`'s `snapshot` method with the proper type and the fields from `s`.
    fn snapshot_in_db<D: Db>(
        db: &D,
        s: DynNewSnapshot,
    ) -> impl Send + Future<Output = anyhow::Result<()>>;
}