use super::{
    postgres_db::{ComboLock, PostgresDb},
    ReadPermsChanges, UpdatesWithSnap,
};
use crate::{
    api::ApiConfig, db_trait::Db, CanDoCallbacks, CrdbFuture, EventId, ObjectId, TypeId,
    Updatedness, User,
};
use std::{collections::HashSet, sync::Arc};

/// Note: Implementation of this trait is supposed to be provided by `crdb::db!`
pub trait ServerConfig: 'static + Sized + Send + Sync + crate::private::Sealed {
    type ApiConfig: ApiConfig;

    fn get_users_who_can_read<'a, C: CanDoCallbacks>(
        call_on: &'a PostgresDb<Self>,
        object_id: ObjectId,
        type_id: TypeId,
        snapshot_version: i32,
        snapshot: serde_json::Value,
        cb: &'a C,
    ) -> impl 'a + CrdbFuture<Output = crate::Result<(HashSet<User>, Vec<ObjectId>, Vec<ComboLock<'a>>)>>;

    fn recreate_no_lock<'a, C: Db>(
        call_on: &'a PostgresDb<Self>,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        updatedness: Updatedness,
        cb: &'a C,
    ) -> impl 'a
           + CrdbFuture<
        Output = crate::Result<Option<(EventId, i32, serde_json::Value, HashSet<User>)>>,
    >;

    fn upload_object<'a, C: Db>(
        call_on: &'a PostgresDb<Self>,
        user: User,
        updatedness: Updatedness,
        type_id: TypeId,
        object_id: ObjectId,
        created_at: EventId,
        snapshot_version: i32,
        snapshot: serde_json::Value,
        cb: &'a C,
    ) -> impl 'a
           + CrdbFuture<
        Output = crate::Result<
            Option<(Arc<UpdatesWithSnap>, HashSet<User>, Vec<ReadPermsChanges>)>,
        >,
    >;

    /// The [`Vec<User>`] in return type is the list of users who can read the object both before and after the change. Users who gained or
    /// lost access to `object_id` are returned as part of the `Vec<ReadPermsChanges>`.
    fn upload_event<'a, C: Db>(
        call_on: &'a PostgresDb<Self>,
        user: User,
        updatedness: Updatedness,
        type_id: TypeId,
        object_id: ObjectId,
        event_id: EventId,
        event: Arc<serde_json::Value>,
        cb: &'a C,
    ) -> impl 'a
           + CrdbFuture<
        Output = crate::Result<Option<(Arc<UpdatesWithSnap>, Vec<User>, Vec<ReadPermsChanges>)>>,
    >;
}

#[doc(hidden)]
#[macro_export]
macro_rules! generate_server {
    ( $api_config:ident | $name:ident | $($object:ty),* ) => {
        pub struct $name;

        impl crdb::private::Sealed for $name {}

        impl crdb::ServerConfig for $name {
            type ApiConfig = $api_config;

            async fn get_users_who_can_read<'a, C: crdb::CanDoCallbacks>(
                call_on: &'a crdb::PostgresDb<Self>,
                object_id: crdb::ObjectId,
                type_id: crdb::TypeId,
                snapshot_version: i32,
                snapshot: crdb::serde_json::Value,
                cb: &'a C,
            ) -> crdb::Result<(crdb::HashSet<crdb::User>, Vec<crdb::ObjectId>, Vec<crdb::ComboLock<'a>>)> {
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let snapshot = crdb::parse_snapshot::<$object>(snapshot_version, snapshot)
                            .wrap_with_context(|| format!("parsing snapshot for {object_id:?}"))?;
                        let res = call_on.get_users_who_can_read(&object_id, &snapshot, cb).await
                            .wrap_with_context(|| format!("listing users who can read {object_id:?}"))?;
                        return Ok(res);
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }

            async fn recreate_no_lock<'a, C: crdb::Db>(
                call_on: &'a crdb::PostgresDb<Self>,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                event_id: crdb::EventId,
                updatedness: crdb::Updatedness,
                cb: &'a C,
            ) -> crdb::Result<Option<(crdb::EventId, i32, crdb::serde_json::Value, crdb::HashSet<crdb::User>)>> {
                use crdb::Object;
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let Some((new_created_at, data)) = call_on.recreate_impl::<$object, C>(object_id, event_id, updatedness, cb).await? else {
                            return Ok(None);
                        };
                        let users_who_can_read = cb.get_latest::<$object>(crdb::Lock::NONE, object_id)
                            .await
                            .wrap_context("retrieving latest snapshot after recreation")?
                            .users_who_can_read(cb)
                            .await
                            .wrap_context("figuring out list of users who can read after recreation")?;
                        let data = crdb::serde_json::to_value(data)
                            .wrap_with_context(|| format!("serializing snapshot for {object_id:?}"))?;
                        return Ok(Some((new_created_at, <$object as crdb::Object>::snapshot_version(), data, users_who_can_read)));
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }

            async fn upload_object<'a, C: crdb::Db>(
                call_on: &'a crdb::PostgresDb<Self>,
                user: crdb::User,
                updatedness: crdb::Updatedness,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                created_at: crdb::EventId,
                snapshot_version: i32,
                snapshot: crdb::serde_json::Value,
                cb: &'a C,
            ) -> crdb::Result<Option<(crdb::Arc<crdb::UpdatesWithSnap>, crdb::HashSet<crdb::User>, Vec<crdb::ReadPermsChanges>)>> {
                use crdb::Object as _;
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let object = crdb::Arc::new(crdb::parse_snapshot::<$object>(snapshot_version, snapshot)
                            .wrap_context("parsing uploaded snapshot data")?);
                        let can_create = object.can_create(user, object_id, cb).await.wrap_context("checking whether user can create submitted object")?;
                        if !can_create {
                            return Err(crdb::Error::Forbidden);
                        }
                        if let Some((_, rdeps)) = call_on.create_and_return_rdep_changes::<$object>(object_id, created_at, object.clone(), updatedness).await? {
                            let snapshot_data = crdb::Arc::new(crdb::serde_json::to_value(&*object)
                                .wrap_context("serializing uploaded snapshot data")?);
                            let users_who_can_read = object.users_who_can_read(cb).await
                                .wrap_context("listing users who can read for submitted object")?;
                            let new_update = crdb::Arc::new(crdb::UpdatesWithSnap {
                                updates: vec![crdb::Arc::new(crdb::Update {
                                    object_id,
                                    data: crdb::UpdateData::Creation {
                                        type_id,
                                        created_at,
                                        snapshot_version: <$object as crdb::Object>::snapshot_version(),
                                        data: snapshot_data.clone(),
                                    },
                                })],
                                new_last_snapshot: Some(snapshot_data),
                            });

                            return Ok(Some((new_update, users_who_can_read, rdeps)));
                        } else {
                            return Ok(None);
                        }
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }

            async fn upload_event<'a, C: crdb::Db>(
                call_on: &'a crdb::PostgresDb<Self>,
                user: crdb::User,
                updatedness: crdb::Updatedness,
                type_id: crdb::TypeId,
                object_id: crdb::ObjectId,
                event_id: crdb::EventId,
                event_data: crdb::Arc<crdb::serde_json::Value>,
                cb: &'a C,
            ) -> crdb::Result<Option<(crdb::Arc<crdb::UpdatesWithSnap>, Vec<crdb::User>, Vec<crdb::ReadPermsChanges>)>> {
                use crdb::Object as _;
                $(
                    if type_id == *<$object as crdb::Object>::type_ulid() {
                        let event = crdb::Arc::new(<<$object as crdb::Object>::Event as crdb::serde::Deserialize>::deserialize(&*event_data)
                            .wrap_context("parsing uploaded snapshot data")?);
                        let object = call_on.get_latest::<$object>(crdb::Lock::OBJECT, object_id).await
                            .wrap_context("retrieving requested object id")?;
                        let can_apply = object.can_apply(user, object_id, &event, call_on).await.wrap_context("checking whether user can apply submitted event")?;
                        if !can_apply {
                            return Err(crdb::Error::Forbidden);
                        }
                        let users_who_can_read_before = object.users_who_can_read(cb).await
                            .wrap_context("listing users who can read for the object before submitting the event")?;
                        if let Some((new_last_snapshot, mut rdeps)) = call_on.submit_and_return_rdep_changes::<$object>(object_id, event_id, event.clone(), updatedness).await? {
                            let snapshot_data = crdb::Arc::new(crdb::serde_json::to_value(&*new_last_snapshot)
                                .wrap_context("serializing updated latest snapshot data")?);
                            let users_who_can_read_after = new_last_snapshot.users_who_can_read(cb).await
                                .wrap_context("listing users who can read for submitted object")?;
                            rdeps.push(crdb::ReadPermsChanges {
                                object_id,
                                type_id,
                                lost_read: users_who_can_read_before.iter().filter(|u| !users_who_can_read_after.contains(u)).copied().collect(),
                                gained_read: users_who_can_read_after.iter().filter(|u| !users_who_can_read_before.contains(u)).copied().collect(),
                            });
                            let users_who_can_read_always = users_who_can_read_before.into_iter().filter(|u| users_who_can_read_after.contains(&u)).collect();

                            let new_update = crdb::Arc::new(crdb::UpdatesWithSnap {
                                updates: vec![crdb::Arc::new(crdb::Update {
                                    object_id,
                                    data: crdb::UpdateData::Event {
                                        type_id,
                                        event_id,
                                        data: event_data.clone(),
                                    },
                                })],
                                new_last_snapshot: Some(snapshot_data.clone()),
                            });
                            return Ok(Some((new_update, users_who_can_read_always, rdeps)));
                        } else {
                            return Ok(None);
                        }
                    }
                )*
                Err(crdb::Error::TypeDoesNotExist(type_id))
            }
        }
    };
}
