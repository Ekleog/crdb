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
    },
    Event {
        object_id: ObjectId,
        type_id: TypeId,
        event_id: EventId,
        object: serde_json::Value,
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
    Updates(Vec<Update>),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum ResponsePart {
    Success,
    Error(crate::SerializableError),
    ConnectionLoss,
    Sessions(Vec<Session>),
    CurrentTime(Timestamp),
    Objects {
        data: Vec<MaybeObject>,
        // Set only in answer to a Query
        now_have_all_until: Option<Updatedness>,
    },
    // Note: the server's answer to GetBinaries is a Success message, followed by one
    // websocket frame of type Binary per requested binary.
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
    pub now_have_all_until_for_object: Updatedness,
    pub now_have_all_until_for_queries: HashMap<QueryId, Updatedness>,
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
}
