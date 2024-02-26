pub use crdb_core::*;
pub use crdb_helpers;
pub use serde;
pub use serde_json;
pub use waaaa;

#[macro_export]
macro_rules! db {
    (
        $v:vis struct $typ:ident {
            $( $name:ident : $object:ty, )*
        }
    ) => {
        $v struct $typ;

        impl $crate::private::Sealed for $typ {}
        impl $crate::Config for $typ {
            fn check_ulids() {
                let ulids = [$(<$object as $crate::Object>::type_ulid()),*];
                for u in ulids.iter() {
                    if ulids.iter().filter(|i| *i == u).count() != 1 {
                        panic!("Type ULID {u:?} was used multiple times!");
                    }
                }
            }

            async fn reencode_old_versions<D: $crate::Db>(call_on: &D) -> usize {
                let mut num_errors = 0;
                $(
                    num_errors += call_on.reencode_old_versions::<$object>().await;
                )*
                num_errors
            }

            async fn create<D: $crate::Db>(
                db: &D,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                created_at: $crate::EventId,
                snapshot_version: i32,
                object: &$crate::serde_json::Value,
            ) -> $crate::Result<()> {
                use std::sync::Arc;
                use $crate::{Lock, Object, ResultExt, crdb_helpers::parse_snapshot_ref};

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let object = parse_snapshot_ref::<$object>(snapshot_version, object)
                            .wrap_with_context(|| format!("failed deserializing object of {type_id:?}"))?;
                        db.create::<$object>(object_id, created_at, Arc::new(object), None, Lock::NONE).await?;
                        return Ok(());
                    }
                )*

                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            async fn recreate<D: $crate::ClientSideDb>(
                db: &D,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                created_at: $crate::EventId,
                snapshot_version: i32,
                object: &$crate::serde_json::Value,
                updatedness: Option<$crate::Updatedness>,
                force_lock: $crate::Lock,
            ) -> $crate::Result<Option<$crate::serde_json::Value>> {
                use $crate::{Object, ResultExt, crdb_helpers::parse_snapshot_ref, serde_json};
                use std::sync::Arc;

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let object = parse_snapshot_ref::<$object>(snapshot_version, object)
                            .wrap_with_context(|| format!("failed deserializing object of {type_id:?}"))?;
                        let res = db.recreate::<$object>(object_id, created_at, Arc::new(object), updatedness, force_lock).await?;
                        let Some(res) = res else {
                            return Ok(None);
                        };
                        let res = serde_json::to_value(&res).wrap_context("serializing object")?;
                        return Ok(Some(res));
                    }
                )*

                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            async fn submit<D: $crate::Db>(
                db: &D,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                event_id: $crate::EventId,
                event: &$crate::serde_json::Value,
                updatedness: Option<$crate::Updatedness>,
                force_lock: $crate::Lock,
            ) -> $crate::Result<Option<$crate::serde_json::Value>> {
                use $crate::{Object, ResultExt, serde, serde_json};
                use std::sync::Arc;

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let event = <<$object as Object>::Event as serde::Deserialize>::deserialize(event)
                            .wrap_with_context(|| format!("failed deserializing event of {type_id:?}"))?;
                        let res = db.submit::<$object>(object_id, event_id, Arc::new(event), updatedness, force_lock).await?;
                        let Some(res) = res else {
                            return Ok(None);
                        };
                        let res = serde_json::to_value(&res).wrap_context("serializing object")?;
                        return Ok(Some(res));
                    }
                )*

                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            async fn remove_event<D: $crate::ClientSideDb>(
                db: &D,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                event_id: $crate::EventId,
            ) -> $crate::Result<()> {
                $(
                    if type_id == *<$object as $crate::Object>::type_ulid() {
                        return db.remove_event::<$object>(object_id, event_id).await;
                    }
                )*
                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            fn get_users_who_can_read<'a, D: $crate::ServerSideDb, C: $crate::CanDoCallbacks>(
                call_on: &'a D,
                object_id: $crate::ObjectId,
                type_id: $crate::TypeId,
                snapshot_version: i32,
                snapshot: $crate::serde_json::Value,
                cb: &'a C,
            ) -> impl 'a + $crate::waaaa::Future<Output = $crate::Result<(std::collections::HashSet<$crate::User>, Vec<$crate::ObjectId>, Vec<$crate::ComboLock<'a>>)>> {
                use $crate::{Object, ResultExt, ServerSideDb, crdb_helpers::parse_snapshot};

                Box::pin(async move { // TODO(api-high): remove this pin-box if it doesn't break the build
                    $(
                        if type_id == *<$object as Object>::type_ulid() {
                            let snapshot = parse_snapshot::<$object>(snapshot_version, snapshot)
                                .wrap_with_context(|| format!("parsing snapshot for {object_id:?}"))?;
                            let res = call_on.get_users_who_can_read(object_id, &snapshot, cb).await
                                .wrap_with_context(|| format!("listing users who can read {object_id:?}"))?;
                            return Ok(res);
                        }
                    )*

                    Err($crate::Error::TypeDoesNotExist(type_id))
                })
            }

            async fn recreate_no_lock<'a, D: $crate::ServerSideDb, C: $crate::CanDoCallbacks>(
                call_on: &'a D,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                event_id: $crate::EventId,
                updatedness: $crate::Updatedness,
                cb: &'a C,
            ) -> $crate::Result<Option<($crate::EventId, i32, $crate::serde_json::Value, std::collections::HashSet<$crate::User>)>> {
                use $crate::{Object, ResultExt, DbPtr, ServerSideDb, serde_json};

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let Some((new_created_at, data)) = call_on.recreate_at::<$object, C>(object_id, event_id, updatedness, cb).await? else {
                            return Ok(None);
                        };
                        let users_who_can_read = cb.get::<$object>(DbPtr::from(object_id))
                            .await
                            .wrap_context("retrieving latest snapshot after recreation")?
                            .users_who_can_read(cb)
                            .await
                            .wrap_context("figuring out list of users who can read after recreation")?;
                        let data = serde_json::to_value(data)
                            .wrap_with_context(|| format!("serializing snapshot for {object_id:?}"))?;
                        return Ok(Some((new_created_at, <$object as Object>::snapshot_version(), data, users_who_can_read)));
                    }
                )*

                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            async fn upload_object<'a, D: $crate::ServerSideDb, C: $crate::CanDoCallbacks>(
                call_on: &'a D,
                user: $crate::User,
                updatedness: $crate::Updatedness,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                created_at: $crate::EventId,
                snapshot_version: i32,
                snapshot: std::sync::Arc<$crate::serde_json::Value>,
                cb: &'a C,
            ) -> $crate::Result<Option<(std::sync::Arc<$crate::UpdatesWithSnap>, std::collections::HashSet<$crate::User>, Vec<$crate::ReadPermsChanges>)>> {
                use $crate::{Object, ResultExt, ServerSideDb, crdb_helpers::parse_snapshot_ref, serde_json, UpdatesWithSnap, Update, UpdateData};
                use std::sync::Arc;

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let object = Arc::new(parse_snapshot_ref::<$object>(snapshot_version, &*snapshot)
                            .wrap_context("parsing uploaded snapshot data")?);
                        let can_create = object.can_create(user, object_id, cb).await.wrap_context("checking whether user can create submitted object")?;
                        if !can_create {
                            return Err($crate::Error::Forbidden);
                        }
                        if let Some((_, rdeps)) = call_on.create_and_return_rdep_changes::<$object>(object_id, created_at, object.clone(), updatedness).await? {
                            let snapshot_data = Arc::new(serde_json::to_value(&*object)
                                .wrap_context("serializing uploaded snapshot data")?);
                            let users_who_can_read = object.users_who_can_read(cb).await
                                .wrap_context("listing users who can read for submitted object")?;
                            let new_update = Arc::new(UpdatesWithSnap {
                                updates: vec![Arc::new(Update {
                                    object_id,
                                    data: UpdateData::Creation {
                                        type_id,
                                        created_at,
                                        snapshot_version: <$object as Object>::snapshot_version(),
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
                Err($crate::Error::TypeDoesNotExist(type_id))
            }

            async fn upload_event<'a, D: $crate::ServerSideDb, C: $crate::CanDoCallbacks>(
                call_on: &'a D,
                user: $crate::User,
                updatedness: $crate::Updatedness,
                type_id: $crate::TypeId,
                object_id: $crate::ObjectId,
                event_id: $crate::EventId,
                event_data: std::sync::Arc<$crate::serde_json::Value>,
                cb: &'a C,
            ) -> $crate::Result<Option<(std::sync::Arc<$crate::UpdatesWithSnap>, Vec<$crate::User>, Vec<$crate::ReadPermsChanges>)>> {
                use $crate::{Object, ResultExt, DbPtr, ServerSideDb, ReadPermsChanges, UpdatesWithSnap, Update, UpdateData, serde, serde_json};
                use std::sync::Arc;

                $(
                    if type_id == *<$object as Object>::type_ulid() {
                        let event = Arc::new(<<$object as Object>::Event as serde::Deserialize>::deserialize(&*event_data)
                            .wrap_context("parsing uploaded snapshot data")?);
                        let object = cb.get::<$object>(DbPtr::from(object_id)).await
                            .wrap_context("retrieving requested object id")?;
                        let can_apply = object.can_apply(user, object_id, &event, cb).await.wrap_context("checking whether user can apply submitted event")?;
                        if !can_apply {
                            return Err($crate::Error::Forbidden);
                        }
                        let users_who_can_read_before = object.users_who_can_read(cb).await
                            .wrap_context("listing users who can read for the object before submitting the event")?;
                        if let Some((new_last_snapshot, mut rdeps)) = call_on.submit_and_return_rdep_changes::<$object>(object_id, event_id, event.clone(), updatedness).await? {
                            let snapshot_data = Arc::new(serde_json::to_value(&*new_last_snapshot)
                                .wrap_context("serializing updated latest snapshot data")?);
                            let users_who_can_read_after = new_last_snapshot.users_who_can_read(cb).await
                                .wrap_context("listing users who can read for submitted object")?;
                            rdeps.push(ReadPermsChanges {
                                object_id,
                                type_id,
                                lost_read: users_who_can_read_before.iter().filter(|u| !users_who_can_read_after.contains(u)).copied().collect(),
                                gained_read: users_who_can_read_after.iter().filter(|u| !users_who_can_read_before.contains(u)).copied().collect(),
                            });
                            let users_who_can_read_always = users_who_can_read_before.into_iter().filter(|u| users_who_can_read_after.contains(&u)).collect();

                            let new_update = Arc::new(UpdatesWithSnap {
                                updates: vec![Arc::new(Update {
                                    object_id,
                                    data: UpdateData::Event {
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

                Err($crate::Error::TypeDoesNotExist(type_id))
            }
        }
    }
}
