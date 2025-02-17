use crate::{Object, ResultExt, TypeId};

/// A JSON snapshot that is always on the latest snapshot version for the contained object type
pub struct JsonSnapshot {
    type_id: TypeId,
    snapshot_version: i32,
    data: serde_json::Value,
}

impl JsonSnapshot {
    pub fn new<T: Object>(snapshot_version: i32, data: serde_json::Value) -> JsonSnapshot {
        JsonSnapshot {
            type_id: *T::type_ulid(),
            snapshot_version,
            data,
        }
    }

    pub fn into_latest<T: Object>(self) -> crate::Result<JsonSnapshot> {
        debug_assert_eq!(self.type_id, *T::type_ulid());
        if self.snapshot_version == T::snapshot_version() {
            Ok(self)
        } else {
            let obj =
                T::from_old_snapshot(self.snapshot_version, self.data).wrap_with_context(|| {
                    format!(
                        "deserializing from snapshot version {}",
                        self.snapshot_version
                    )
                })?;
            Ok(JsonSnapshot {
                type_id: self.type_id,
                snapshot_version: T::snapshot_version(),
                data: serde_json::to_value(obj).wrap_context("serializing snapshot")?,
            })
        }
    }

    pub fn into_parsed<T: Object>(self) -> crate::Result<T> {
        debug_assert_eq!(self.type_id, *T::type_ulid());
        if self.snapshot_version == T::snapshot_version() {
            Ok(serde_json::from_value(self.data)
                .wrap_context("deserializing snapshot from latest version")?)
        } else {
            let obj =
                T::from_old_snapshot(self.snapshot_version, self.data).wrap_with_context(|| {
                    format!(
                        "deserializing from snapshot version {}",
                        self.snapshot_version
                    )
                })?;
            Ok(obj)
        }
    }
}
