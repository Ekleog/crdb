// TODO: switch to eg. `idb` once https://github.com/devashishdxt/idb/issues/7 is fixed

use crate::{
    db_trait::Db, error::ResultExt, full_object::FullObject, BinPtr, CanDoCallbacks, CrdbStream,
    EventId, Object, ObjectId, Query, Timestamp, TypeId, User,
};
use anyhow::anyhow;
use js_sys::Function;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc,
};
use tokio::sync::mpsc;
use wasm_bindgen::{closure::Closure, JsCast, JsValue};
use wasm_bindgen_futures::JsFuture;
use web_sys::{
    IdbDatabase, IdbObjectStoreParameters, IdbOpenDbRequest, IdbRequest, IdbTransaction,
    IdbTransactionMode,
};

// TODO: remove all tracing::info!() before stabilizing

#[derive(Debug, PartialEq, serde::Deserialize, serde::Serialize)]
struct SnapshotMeta {
    snapshot_id: EventId,
    type_id: TypeId,
    object_id: ObjectId,
    is_creation: bool,
    is_latest: bool,
    snapshot_version: i32,
    is_locked: bool,
    upload_succeeded: bool,
}

pub struct IndexedDb {
    is_persistent: bool,
    db: IdbDatabase,
}

macro_rules! other_err {
    ($($t:tt)*) => { crate::Error::Other(anyhow!($($t)*)) }
}

impl IndexedDb {
    pub async fn connect(url: &str) -> anyhow::Result<IndexedDb> {
        let closure_stash = Rc::new(RefCell::new(Vec::new()));
        let window = web_sys::window().ok_or_else(|| anyhow!("not running in a browser"))?;
        let is_persistent = JsFuture::from(window.navigator().storage().persist().wrap_context(
            "failed to request persistence, did the user disable storage altogether?",
        )?)
        .await
        .wrap_context("failed to resolve request for persistence")?
        .as_bool()
        .ok_or_else(|| anyhow!("requesting for persistence did not return a boolean"))?;

        let factory = window
            .indexed_db()
            .wrap_context("getting IndexedDb factory")?
            .ok_or_else(|| anyhow!("IndexedDb seems to be disabled"))?;
        const VERSION: u32 = 1;
        let db_req = factory
            .open_with_u32(url, VERSION)
            .wrap_with_context(|| format!("opening IndexedDb {url:?} at version {VERSION}"))?;

        db_req.set_onupgradeneeded(
            stash_closure(&closure_stash, |evt: web_sys::Event| {
                let target = evt.target().unwrap();
                let db_req = target.dyn_into::<IdbOpenDbRequest>().unwrap();
                let result = db_req.result().unwrap();
                let db = result.dyn_into::<IdbDatabase>().unwrap();
                db.create_object_store("snapshots").unwrap();
                db.create_object_store("events").unwrap();
                db.create_object_store("binaries").unwrap();
                let snapshots_meta = db
                    .create_object_store_with_optional_parameters(
                        "snapshots_meta",
                        IdbObjectStoreParameters::new().key_path(Some(
                            &serde_wasm_bindgen::to_value(&["snapshot_id"]).unwrap(),
                        )),
                    )
                    .unwrap();
                snapshots_meta
                    .create_index_with_str_sequence(
                        "latest_object",
                        &serde_wasm_bindgen::to_value(&["is_latest", "object_id"]).unwrap(),
                    )
                    .unwrap();
                snapshots_meta
                    .create_index_with_str_sequence(
                        "creation_object",
                        &serde_wasm_bindgen::to_value(&["is_creation", "object_id"]).unwrap(),
                    )
                    .unwrap();
                snapshots_meta
                    .create_index_with_str_sequence(
                        "object_snapshot",
                        &serde_wasm_bindgen::to_value(&["object_id", "snapshot_id"]).unwrap(),
                    )
                    .unwrap();
            })
            .as_ref(),
        );

        let (db_tx, mut db_rx) = mpsc::unbounded_channel();
        db_req.set_onsuccess(
            result_notrans_cb(&db_tx, &closure_stash, |evt: web_sys::Event| {
                let target = evt
                    .target()
                    .ok_or_else(|| anyhow!("Failed retrieving on_success target"))?;
                let db_req = target.dyn_into::<IdbOpenDbRequest>().map_err(|_| {
                    anyhow!("Failed interpreting on_success target as IdbOpenDbRequest")
                })?;
                let db = db_req
                    .result()
                    .wrap_context("getting database")?
                    .dyn_into::<IdbDatabase>()
                    .map_err(|_| anyhow!("Failed casting IndexedDb result into IdbDatabase"))?;
                Ok(db)
            })
            .as_ref(),
        );
        db_req.set_onerror(
            result_notrans_cb(&db_tx, &closure_stash, |_| {
                Err(anyhow!("failed to open IndexedDb database"))
            })
            .as_ref(),
        );

        let db = db_rx.recv().await.unwrap()?;
        Ok(IndexedDb { is_persistent, db })
    }

    pub fn is_persistent(&self) -> bool {
        self.is_persistent
    }
}

#[allow(unused_variables)] // TODO: remove
impl Db for IndexedDb {
    async fn create<T: Object, C: CanDoCallbacks>(
        &self,
        object_id: ObjectId,
        created_at: EventId,
        object: Arc<T>,
        cb: &C,
    ) -> crate::Result<()> {
        tracing::info!("create");
        let closure_stash = Rc::new(RefCell::new(Vec::new()));
        let transaction = self
            .db
            .transaction_with_str_sequence_and_mode(
                &serde_wasm_bindgen::to_value(&[
                    "snapshots",
                    "snapshots_meta",
                    "events",
                    "binaries",
                ])
                .unwrap(),
                IdbTransactionMode::Readwrite,
            )
            .wrap_context("starting transaction")?;

        let snapshot_meta = SnapshotMeta {
            snapshot_id: created_at,
            type_id: *T::type_ulid(),
            object_id,
            is_creation: true,
            is_latest: true,
            snapshot_version: T::snapshot_version(),
            is_locked: false, // TODO: allow for atomic create-and-lock
            upload_succeeded: false,
        };
        let snapshot = serde_wasm_bindgen::to_value(&*object)
            .wrap_with_context(|| format!("serializing {object_id:?}"))?;

        // First, try inserting the object
        let add_req = transaction
            .object_store("snapshots_meta")
            .wrap_context("getting 'snapshots_meta' object store")?
            .add(
                &serde_wasm_bindgen::to_value(&snapshot_meta).wrap_with_context(|| {
                    format!("serializing snapshot metadata for {object_id:?} to json")
                })?,
            )
            .wrap_with_context(|| format!("submitting request to add {object_id:?}"))?;

        let (tx, mut rx) = mpsc::unbounded_channel();
        let tx2 = tx.clone();
        let tx3 = tx.clone();
        let tx4 = tx.clone();
        let tx5 = tx.clone();
        let tx6 = tx.clone();
        let object2 = object.clone();
        let closure_stash2 = closure_stash.clone();
        let closure_stash3 = closure_stash.clone();
        let closure_stash4 = closure_stash.clone();
        let closure_stash5 = closure_stash.clone();
        let closure_stash6 = closure_stash.clone();
        tracing::info!("before add_req");
        add_req.set_onerror(collect_errors_cb(&tx, &transaction, &closure_stash, move |evt: web_sys::Event| {
            tracing::info!("add_req errors");
            // It failed. Check that the already-existing value was the same

            let target = evt.target().ok_or_else(|| {
                other_err!("Failed retrieving on_error event target")
            })?;
            let add_req = target.dyn_into::<IdbRequest>()
                .map_err(|_| other_err!("Failed rebuilding an IdbRequest from the target"))?;
            let transaction = add_req.transaction().ok_or_else(|| {
                other_err!(
                    "Failed recovering the transaction from the IdbRequest"
                )
            })?;

            let key = js_sys::Array::new();
            key.push(&JsValue::TRUE);
            key.push(&object_id.to_js_string());
            let get_req = transaction
                .object_store("snapshots_meta")
                .wrap_context("getting 'snapshots_meta' object store")?
                .index("creation_object")
                .wrap_context("getting 'creation_object' index")?
                .get(&key)
                .wrap_with_context(|| format!("requesting snapshot metadata for {object_id:?}"))?;
            let transaction = IdbTransaction::from(transaction);

            get_req.set_onerror(result_cb(&tx2, &transaction, &closure_stash2, move |_| {
                tracing::info!("get_req errors");
                Err(other_err!(
                    "Failed to retrieve pre-existing snapshot metadata for {object_id:?}"
                ))
            }).as_ref());
            get_req.set_onsuccess(collect_errors_cb(&tx2, &transaction, &closure_stash2, move |evt: web_sys::Event| {
                tracing::info!("get_req successes");
                let target = evt.target().ok_or_else(|| {
                    other_err!("Failed retrieving on_error event target")
                })?;
                let get_req = target.dyn_into::<IdbRequest>()
                    .map_err(|_| other_err!("rebuilding an IdbRequest from the target"))?;
                let transaction = get_req.transaction().ok_or_else(|| {
                    other_err!("Failed recovering the transaction from an IdbRequest")
                })?;
                let result = get_req.result().wrap_with_context(|| {
                    format!("getting the result of the request for snapshot metadata of {object_id:?}")
                })?;

                let mut preexisting = serde_wasm_bindgen::from_value::<SnapshotMeta>(result)
                    .wrap_with_context(|| {
                        format!("deserializing pre-existing snapshot metadata for {object_id:?}")
                    })?;
                // Ignore a few fields in comparison below
                preexisting.is_latest = true;
                preexisting.is_locked = false;
                preexisting.upload_succeeded = false;
                tracing::info!("comparing preexisting {preexisting:?} with under-insertion {snapshot_meta:?}");
                if preexisting != snapshot_meta {
                    return Err(crate::Error::ObjectAlreadyExists(object_id));
                }

                // Metadata is the same, still need to check snapshot contents
                let get_req = transaction
                    .object_store("snapshots")
                    .wrap_context("retrieving 'snapshots' object store")?
                    .get(&created_at.to_js_string())
                    .wrap_with_context(|| format!("requesting snapshot data for {created_at:?}"))?;

                get_req.set_onerror(result_cb(&tx3, &transaction, &closure_stash3, move |_| {
                    tracing::info!("get_req errors");
                    Err(other_err!("Failed to retrieve pre-existing snapshot data for {object_id:?}"))
                }).as_ref());
                get_req.set_onsuccess(result_cb(&tx3, &transaction, &closure_stash3, move |evt: web_sys::Event| {
                    tracing::info!("get_req successes");
                    let target = evt.target().ok_or_else(|| other_err!("Failed retrieving on_error event target"))?;
                    let get_req = target.dyn_into::<IdbRequest>()
                        .map_err(|_| other_err!("rebuilding an IdbRequest from the target"))?;
                    let result = get_req.result().wrap_with_context(|| {
                        format!("getting the result of the request for snapshot metadata of {object_id:?}")
                    })?;

                    tracing::info!("pre-existing snapshot is {result:?}");
                    let preexisting = serde_wasm_bindgen::from_value::<T>(result)
                        .wrap_with_context(|| format!("deserializing pre-existing snapshot for {object_id:?}"))?;
                    if preexisting != *object {
                        Err(crate::Error::ObjectAlreadyExists(object_id))
                    } else {
                        Ok(())
                    }
                }).as_ref());

                Ok(())
            }).as_ref());

            Ok(())
        }).as_ref());

        add_req.set_onsuccess(collect_errors_cb(
            &tx,
            &transaction,
            &closure_stash,
            move |evt: web_sys::Event| {
                tracing::info!("add_req successes");
                // Metadata addition succeeded. Now, time to add the data itself.

                let target = evt.target().ok_or_else(|| {
                    other_err!("Failed retrieving on_error event target")
                })?;
                let add_req = target.dyn_into::<IdbRequest>()
                    .map_err(|_| other_err!("rebuilding an IdbRequestRequest from the target"))?;
                let transaction = add_req.transaction().ok_or_else(|| {
                    other_err!("Failed recovering the transaction from an IdbRequest")
                })?;

                let add_req = transaction
                    .object_store("snapshots")
                    .wrap_context("getting 'snapshots' object store")?
                    .add_with_key(
                        &serde_wasm_bindgen::to_value(&*object2).wrap_with_context(|| format!("serializing {object_id:?}"))?,
                        &created_at.to_js_string(),
                    )
                    .wrap_with_context(|| format!("requesting for {object_id:?} snapshot addition"))?;

                add_req.set_onerror(result_cb(
                    &tx4,
                    &transaction,
                    &closure_stash4,
                    |_| Err(other_err!("Failed inserting the snapshot, despite snapshot metadata insertion succeeding"))
                ).as_ref());
                add_req.set_onsuccess(collect_errors_cb(&tx4, &transaction, &closure_stash4, move |evt: web_sys::Event| {
                    // It succeeded. We still need to check for no event conflict and no missing binaries

                    let target = evt.target().ok_or_else(|| {
                        other_err!("Failed retrieving on_error event target")
                    })?;
                    let add_req = target.dyn_into::<IdbRequest>()
                        .map_err(|_| other_err!("rebuilding an IdbRequestRequest from the target"))?;
                    let transaction = add_req.transaction().ok_or_else(|| {
                        other_err!("Failed recovering the transaction from an IdbRequest")
                    })?;

                    let get_req = transaction
                        .object_store("events")
                        .wrap_context("getting 'events' object store")?
                        .get_key(&created_at.to_js_string())
                        .wrap_with_context(|| {
                            format!("requesting events for presumably inexistent {created_at:?}")
                        })?;
                    let transaction = IdbTransaction::from(transaction);

                    tracing::info!("before get_req");
                    get_req.set_onerror(result_cb(&tx5, &transaction, &closure_stash5, move |_| {
                        tracing::info!("get_req errors");
                        Err(other_err!(
                            "Failed to validate absence of pre-existing event {created_at:?}"
                        ))
                    }).as_ref());
                    get_req.set_onsuccess(collect_errors_cb(&tx5, &transaction, &closure_stash5, move |evt: web_sys::Event| {
                        tracing::info!("get_req successes");
                        let target = evt.target().ok_or_else(|| {
                            other_err!("Failed retrieving on_error event target")
                        })?;
                        let get_req = target.dyn_into::<IdbRequest>()
                            .map_err(|_| other_err!("rebuilding an IdbRequest from the target"))?;
                        let transaction = add_req.transaction().ok_or_else(|| {
                            other_err!("Failed recovering the transaction from the IdbRequest")
                        })?;
                        let result = get_req.result().wrap_with_context(|| {
                            format!("getting the result of the request for pre-existing events at {created_at:?}")
                        })?;

                        if !result.is_undefined() {
                            return Err(crate::Error::EventAlreadyExists(created_at));
                        }

                        check_required_binaries(&tx6, transaction, &closure_stash6, object2.required_binaries())
                    }).as_ref());

                    Ok(())
                }).as_ref());

                Ok(())
            },
        ).as_ref());

        tracing::info!("waiting for add_req resp");
        let res = rx.recv().await.unwrap();
        tracing::info!("got add_req resp: {res:?}");
        res
    }

    async fn submit<T: Object, C: CanDoCallbacks>(
        &self,
        object: ObjectId,
        event_id: EventId,
        event: Arc<T::Event>,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn get<T: Object>(&self, object_id: ObjectId) -> crate::Result<FullObject> {
        todo!()
    }

    async fn query<T: Object>(
        &self,
        user: User,
        ignore_not_modified_on_server_since: Option<Timestamp>,
        q: &Query,
    ) -> crate::Result<impl CrdbStream<Item = crate::Result<FullObject>>> {
        // todo!()
        Ok(futures::stream::empty())
    }

    async fn recreate<T: Object, C: CanDoCallbacks>(
        &self,
        time: Timestamp,
        object: ObjectId,
        cb: &C,
    ) -> crate::Result<()> {
        todo!()
    }

    async fn remove(&self, _object_id: ObjectId) -> crate::Result<()> {
        todo!()
    }

    async fn create_binary(&self, binary_id: BinPtr, data: Arc<Vec<u8>>) -> crate::Result<()> {
        todo!()
    }

    async fn get_binary(&self, binary_id: BinPtr) -> anyhow::Result<Option<Arc<Vec<u8>>>> {
        todo!()
    }
}

fn stash_closure(
    closure_stash: &Rc<RefCell<Vec<Closure<dyn FnMut(web_sys::Event)>>>>,
    cb: impl 'static + FnOnce(web_sys::Event),
) -> Option<Function> {
    let res = Closure::once(cb);
    let fun = res.as_ref().dyn_ref::<Function>().unwrap().clone();
    closure_stash.borrow_mut().push(res);
    Some(fun)
}

fn result_notrans_cb<Ret: 'static>(
    tx: &mpsc::UnboundedSender<Ret>,
    closure_stash: &Rc<RefCell<Vec<Closure<dyn FnMut(web_sys::Event)>>>>,
    cb: impl 'static + FnOnce(web_sys::Event) -> Ret,
) -> Option<Function> {
    let tx = tx.clone();
    stash_closure(closure_stash, move |arg| {
        arg.prevent_default();
        tx.send(cb(arg)).unwrap();
    })
}

fn result_cb<Ret: 'static, Err: 'static>(
    tx: &mpsc::UnboundedSender<Result<Ret, Err>>,
    transaction: &IdbTransaction,
    closure_stash: &Rc<RefCell<Vec<Closure<dyn FnMut(web_sys::Event)>>>>,
    cb: impl 'static + FnOnce(web_sys::Event) -> Result<Ret, Err>,
) -> Option<Function> {
    let tx = tx.clone();
    let transaction = transaction.clone();
    stash_closure(closure_stash, move |arg| {
        arg.prevent_default();
        tx.send(cb(arg).map_err(|err| {
            transaction
                .abort()
                .expect("Failed aborting the transaction upon error");
            err
        }))
        .unwrap();
    })
}

fn collect_errors_cb<T: 'static, Ret: 'static>(
    tx: &mpsc::UnboundedSender<Result<T, Ret>>,
    transaction: &IdbTransaction,
    closure_stash: &Rc<RefCell<Vec<Closure<dyn FnMut(web_sys::Event)>>>>,
    cb: impl 'static + FnOnce(web_sys::Event) -> Result<(), Ret>,
) -> Option<Function> {
    let tx = tx.clone();
    let transaction = transaction.clone();
    stash_closure(closure_stash, move |arg| {
        arg.prevent_default();
        if let Err(ret) = cb(arg) {
            transaction
                .abort()
                .expect("Failed aborting the transaction upon error");
            tx.send(Err(ret)).unwrap();
        }
    })
}

fn check_required_binaries(
    tx: &mpsc::UnboundedSender<crate::Result<()>>,
    transaction: IdbTransaction,
    closure_stash: &Rc<RefCell<Vec<Closure<dyn FnMut(web_sys::Event)>>>>,
    binaries: Vec<BinPtr>,
) -> crate::Result<()> {
    tracing::info!("check_required_binaries");
    if binaries.is_empty() {
        // The below code will send a result over tx only if there's at least one binary to check
        tx.send(Ok(())).unwrap();
    }

    let binaries_store = transaction
        .object_store("binaries")
        .wrap_context("opening 'binaries' object store")?;
    let missing_binaries = Rc::new(RefCell::new(Vec::new()));
    let remaining_queries = Rc::new(Cell::new(binaries.len()));
    for b in binaries {
        let get_req = binaries_store
            .get_key(&b.to_js_string())
            .wrap_with_context(|| format!("requesting whether {b:?} is present"))?;

        let remaining_queries2 = remaining_queries.clone();
        let remaining_queries3 = remaining_queries.clone();
        let tx2 = tx.clone();
        let missing_binaries2 = missing_binaries.clone();
        get_req.set_onerror(
            result_cb(&tx, &transaction, &closure_stash, move |_| {
                remaining_queries2.set(0);
                Err(other_err!("Failed checking whether {b:?} is present"))
            })
            .as_ref(),
        );
        get_req.set_onsuccess(
            collect_errors_cb(
                &tx,
                &transaction,
                &closure_stash,
                move |evt: web_sys::Event| {
                    let target = evt
                        .target()
                        .ok_or_else(|| other_err!("Failed retrieving on_success event target"))?;
                    let get_req = target
                        .dyn_into::<IdbRequest>()
                        .map_err(|_| other_err!("rebuilding a StoreRequest from the target"))?;
                    let result = get_req.result().wrap_with_context(|| {
                        format!("getting the result of the request for binary {b:?}")
                    })?;

                    if result.is_undefined() {
                        missing_binaries2.borrow_mut().push(b);
                    }

                    let remains = remaining_queries3.get();
                    if remains == 0 {
                        let missing_binaries = missing_binaries2.borrow();
                        let res = if missing_binaries.is_empty() {
                            Ok(())
                        } else {
                            Err(crate::Error::MissingBinaries(missing_binaries.clone()))
                        };
                        tx2.send(res).unwrap();
                    } else {
                        remaining_queries3.set(remains - 1);
                    }

                    Ok(())
                },
            )
            .as_ref(),
        );
    }
    Ok(())
}
