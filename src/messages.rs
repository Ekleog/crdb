#![allow(dead_code)] // TODO(api): remove once server-side API is done
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

// TODO(low): review what all the (de)serialized JSON for all the types defined here looks like
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientMessage {
    pub request_id: RequestId,
    pub request: Arc<Request>,
}

pub struct RequestWithSidecar {
    pub request: Arc<Request>,
    pub sidecar: Vec<Arc<[u8]>>,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum Request {
    SetToken(SessionToken),
    RenameSession(String),
    CurrentSession,
    ListSessions,
    DisconnectSession(SessionRef),
    GetTime,
    // TODO(low): add a way to fetch only the new events, when we already have most of one big object?
    Get {
        // Map from object to the only_updated_since information we want on it
        object_ids: HashMap<ObjectId, Option<Updatedness>>,
        subscribe: bool,
    },
    Query {
        query_id: QueryId,
        query: Arc<Query>,
        only_updated_since: Option<Updatedness>,
        subscribe: bool,
    },
    GetBinaries(HashSet<BinPtr>),
    Unsubscribe(HashSet<ObjectId>),
    UnsubscribeQuery(QueryId),
    Upload(Vec<UploadOrBinary>),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum UploadOrBinary {
    Upload(Upload),
    Binary, // If set to `Binary`, then the binary is in the websocket frame of type `Binary` just after this one
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
        event: serde_json::Value,
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
    pub data: Vec<Update>,
    pub now_have_all_until: Updatedness,
}

pub struct ResponsePartWithSidecar {
    pub response: ResponsePart,
    pub sidecar: Vec<Arc<[u8]>>,
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
    // TODO(low): expose some API to make it easy for client writers to notice they're getting snapshots
    // with versions higher than what their current code version supports, to suggest an upgrade
    pub creation_snapshot: Option<(EventId, i32, serde_json::Value)>,
    pub events: BTreeMap<EventId, serde_json::Value>,
    pub now_have_all_until: Updatedness,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Update {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    pub data: UpdateData,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum UpdateData {
    // Also used for re-creation events
    Creation {
        created_at: EventId,
        snapshot_version: i32,
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        data: serde_json::Value,
    },
    LostReadRights,
}
