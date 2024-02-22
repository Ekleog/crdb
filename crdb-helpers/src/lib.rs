use crdb_core::Object;

use anyhow::Context;

pub fn parse_snapshot<T: Object>(
    snapshot_version: i32,
    snapshot_data: serde_json::Value,
) -> anyhow::Result<T> {
    if snapshot_version == T::snapshot_version() {
        Ok(serde_json::from_value(snapshot_data).with_context(|| {
            format!(
                "parsing current snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })?)
    } else {
        T::from_old_snapshot(snapshot_version, snapshot_data).with_context(|| {
            format!(
                "parsing old snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })
    }
}

pub fn parse_snapshot_ref<T: Object>(
    snapshot_version: i32,
    snapshot_data: &serde_json::Value,
) -> anyhow::Result<T> {
    if snapshot_version == T::snapshot_version() {
        Ok(T::deserialize(snapshot_data).with_context(|| {
            format!(
                "parsing current snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })?)
    } else {
        T::from_old_snapshot(snapshot_version, snapshot_data.clone()).with_context(|| {
            format!(
                "parsing old snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })
    }
}

#[cfg(feature = "js")]
pub fn parse_snapshot_js<T: Object>(
    snapshot_version: i32,
    snapshot_data: wasm_bindgen::JsValue,
) -> crdb_core::Result<T> {
    use crdb_core::ResultExt;

    if snapshot_version == T::snapshot_version() {
        Ok(
            serde_wasm_bindgen::from_value(snapshot_data).wrap_with_context(|| {
                format!(
                    "parsing current snapshot version {snapshot_version} for object type {:?}",
                    T::type_ulid()
                )
            })?,
        )
    } else {
        let as_serde_json = serde_wasm_bindgen::from_value::<serde_json::Value>(snapshot_data)
            .wrap_with_context(|| format!("parsing data from IndexedDB as JSON"))?;
        T::from_old_snapshot(snapshot_version, as_serde_json).wrap_with_context(|| {
            format!(
                "parsing old snapshot version {snapshot_version} for object type {:?}",
                T::type_ulid()
            )
        })
    }
}
