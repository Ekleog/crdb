use crate::{db_trait::Db, messages::Upload, BinPtr, CrdbFuture, EventId, ObjectId, TypeId};

#[derive(Copy, Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct UploadId(pub i64);

#[derive(serde::Deserialize, serde::Serialize)]
pub enum UploadOrBinPtr {
    Upload(Upload),
    BinPtr(BinPtr),
}

pub trait ApiConfig: crate::private::Sealed {
    /// Auto-generated by `crdb::db!`.
    ///
    /// Panics if there are two types with the same ULID configured
    fn check_ulids();

    fn recreate<D: Db>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        created_at: EventId,
        snapshot_version: i32,
        object: serde_json::Value,
        force_lock: bool,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;

    fn submit<D: Db>(
        db: &D,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: serde_json::Value,
    ) -> impl CrdbFuture<Output = crate::Result<()>>;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_api {
    ( $config:ident | $($object:ty),* ) => {
        pub struct $config;

        impl crdb::private::Sealed for $config {}
        impl crdb::ApiConfig for $config {
            fn check_ulids() {
                let ulids = [$(<$object as crdb::Object>::type_ulid()),*];
                for u in ulids.iter() {
                    if ulids.iter().filter(|i| *i == u).count() != 1 {
                        panic!("Type ULID {u:?} was used multiple times!");
                    }
                }
            }

            async fn recreate<D: crdb::Db>(
                db: &D,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                created_at: crdb::EventId,
                snapshot_version: i32,
                object: crdb::serde_json::Value,
                force_lock: bool,
            ) -> crdb::Result<()> {
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        // TODO(high): actually implement properly, able both to recreate and to create new objects
                        let _ = snapshot_version;
                        let object = crdb::serde_json::from_value::<$object>(object)
                            .wrap_with_context(|| format!("failed deserializing object of {type_id:?}"))?;
                        return db.create::<$object, _>(object_id, created_at, crdb::Arc::new(object), force_lock, db).await.map(|_| ());
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }

            async fn submit<D: crdb::Db>(
                db: &D,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                event_id: crdb::EventId,
                event: crdb::serde_json::Value,
            ) -> crdb::Result<()> {
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let event = crdb::serde_json::from_value::<<$object as crdb::Object>::Event>(event)
                            .wrap_with_context(|| format!("failed deserializing event of {type_id:?}"))?;
                        return db.submit::<$object, _>(object_id, event_id, crdb::Arc::new(event), db).await.map(|_| ());
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }
        }
    };
}
