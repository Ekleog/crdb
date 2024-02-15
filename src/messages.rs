use crate::{
    ids::QueryId, BinPtr, EventId, ObjectId, Query, Session, SessionRef, SessionToken, Timestamp,
    TypeId, Updatedness,
};
use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::Arc,
};

#[derive(
    Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, serde::Deserialize, serde::Serialize,
)]
pub struct RequestId(pub u64);

// TODO(misc-med): review what all the (de)serialized JSON for all the types defined here looks like
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientMessage {
    pub request_id: RequestId,
    pub request: Arc<Request>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Request {
    // TODO(client-high): make sure all these request types are properly exposed to the user.
    SetToken(SessionToken),
    RenameSession(String),
    CurrentSession,
    ListSessions,
    DisconnectSession(SessionRef),
    GetTime,
    // Map from object to the only_updated_since information we want on it
    GetSubscribe(HashMap<ObjectId, Option<Updatedness>>),
    QuerySubscribe {
        query_id: QueryId,
        type_id: TypeId,
        query: Arc<Query>,
        only_updated_since: Option<Updatedness>,
    },
    GetLatest(HashSet<ObjectId>),
    QueryLatest {
        type_id: TypeId,
        query: Arc<Query>,
        only_updated_since: Option<Updatedness>,
    },
    GetBinaries(HashSet<BinPtr>),
    Unsubscribe(HashSet<ObjectId>),
    UnsubscribeQuery(QueryId),
    Upload(Upload),
    UploadBinaries(usize), // There are N binaries is in the N websocket frames of type `Binary` just after this one
}

#[cfg(feature = "client")]
impl Request {
    pub fn is_upload(&self) -> bool {
        match self {
            Request::SetToken(_)
            | Request::RenameSession(_)
            | Request::CurrentSession
            | Request::ListSessions
            | Request::DisconnectSession(_)
            | Request::GetTime
            | Request::Unsubscribe(_)
            | Request::UnsubscribeQuery(_)
            | Request::GetSubscribe(_)
            | Request::QuerySubscribe { .. }
            | Request::GetLatest(_)
            | Request::QueryLatest { .. }
            | Request::GetBinaries(_) => false,
            Request::Upload(_) | Request::UploadBinaries(_) => true,
        }
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Upload {
    Object {
        object_id: ObjectId,
        type_id: TypeId,
        created_at: EventId,
        snapshot_version: i32,
        object: serde_json::Value,
        subscribe: bool,
    },
    Event {
        object_id: ObjectId,
        type_id: TypeId,
        event_id: EventId,
        event: Arc<serde_json::Value>,
        subscribe: bool,
    },
}

/// One ServerMessage is supposed to hold as much data as possible
/// without delaying updates, but still avoiding going too far above
/// than 1M / message, to allow for better resumability.
///
/// If the `last_response` field is set to `true`, then it means that
/// all the previous `ServerMessage`s that answered this `request`,
/// taken together, hold the answer to the request.
///
/// Any subsequent updates, obtained by subscribing to the object or
/// query, will be pushed as `Update`s.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum ServerMessage {
    Response {
        request_id: RequestId,
        response: ResponsePart,
        last_response: bool,
    },
    Updates(Updates),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Updates {
    pub data: Vec<Arc<Update>>,
    // This is the updatedness for all the currently subscribed queries
    pub now_have_all_until: Updatedness,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum ResponsePart {
    Success,
    Error(crate::SerializableError),
    Sessions(Vec<Session>),
    CurrentTime(Timestamp),
    Objects {
        data: Vec<MaybeObject>,
        // Set only in answer to a Query, this is the max of the Updatedness of all the returned objects.
        // This is only set in the last ResponsePart of the query request, to make sure if connection cuts
        // the client will not wrongfully assume having already received everything.
        // TODO(perf-low): Server would have better perf if this were actually the max updatedness it's guaranteed
        // to have answered. This way, clients would ask queries with a higher only_updated_since, and thus
        // postgresql would be able to filter more lines faster.
        now_have_all_until: Option<Updatedness>,
    },
    Snapshots {
        data: Vec<MaybeSnapshot>,
        now_have_all_until: Option<Updatedness>,
    },
    Binaries(usize),
    // Note: the server's answer to GetBinaries is a Binaries(x) message, followed by `x`
    // websocket frames of type Binary. It can be split into multiple parts.
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum MaybeObject {
    AlreadySubscribed(ObjectId),
    NotYetSubscribed(ObjectData),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ObjectData {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    // TODO(misc-med): expose some API to make it easy for client writers to notice they're getting snapshots
    // with versions higher than what their current code version supports, to suggest an upgrade
    pub creation_snapshot: Option<(EventId, i32, Arc<serde_json::Value>)>,
    pub events: BTreeMap<EventId, Arc<serde_json::Value>>,
    pub now_have_all_until: Updatedness,
}

impl ObjectData {
    #[cfg(any(feature = "client", feature = "server"))]
    pub fn into_updates(self) -> Vec<Arc<Update>> {
        let mut res =
            Vec::with_capacity(self.events.len() + self.creation_snapshot.is_some() as usize);
        if let Some((created_at, snapshot_version, data)) = self.creation_snapshot {
            res.push(Arc::new(Update {
                object_id: self.object_id,
                data: UpdateData::Creation {
                    type_id: self.type_id,
                    created_at,
                    snapshot_version,
                    data,
                },
            }));
        }
        for (event_id, data) in self.events.into_iter() {
            res.push(Arc::new(Update {
                object_id: self.object_id,
                data: UpdateData::Event {
                    type_id: self.type_id,
                    event_id,
                    data,
                },
            }));
        }
        res
    }
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum MaybeSnapshot {
    AlreadySubscribed(ObjectId),
    NotSubscribed(SnapshotData),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct SnapshotData {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    pub snapshot_version: i32,
    pub snapshot: Arc<serde_json::Value>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Update {
    pub object_id: ObjectId,
    pub data: UpdateData,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum UpdateData {
    // Also used for re-creation events
    Creation {
        type_id: TypeId,
        created_at: EventId,
        snapshot_version: i32,
        data: Arc<serde_json::Value>,
    },
    Event {
        type_id: TypeId,
        event_id: EventId,
        data: Arc<serde_json::Value>,
    },
    LostReadRights,
}
