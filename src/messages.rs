use crate::{
    ids::RequestId, BinPtr, EventId, ObjectId, Query, Session, SessionRef, SessionToken, Timestamp,
    TypeId,
};
use std::collections::{BTreeMap, HashSet};

// TODO(low): review what all the (de)serialized JSON for all the types defined here looks like
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct ClientMessage {
    pub request_id: RequestId,
    pub request: Request,
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
        object_ids: HashSet<ObjectId>,
        only_updated_since: Option<Timestamp>,
        subscribe: bool,
    },
    Query {
        query: Query,
        only_updated_since: Option<Timestamp>,
        subscribe: bool,
    },
    GetBinaries(HashSet<BinPtr>),
    Unsubscribe(HashSet<ObjectId>),
    UnsubscribeQuery(Query),
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
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        type_id: TypeId,
        object_id: ObjectId,
        data: serde_json::Value,
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
        request: RequestId,
        response: ResponsePart,
        last_response: bool,
    },
    Updates(Vec<Update>),
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum ResponsePart {
    Success,
    Error(crate::SerializableError),
    Sessions(Vec<Session>),
    CurrentTime(Timestamp),
    Objects(Vec<MaybeObject>),
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
    pub created_at: EventId,
    pub type_id: TypeId,
    pub creation_snapshot: Option<serde_json::Value>,
    pub events: BTreeMap<EventId, serde_json::Value>,
    pub now_have_all_until: Timestamp,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Update {
    pub object_id: ObjectId,
    pub type_id: TypeId,
    pub data: UpdateData,
    pub now_have_all_until: Timestamp,
}

#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub enum UpdateData {
    Creation {
        created_at: EventId,
        data: serde_json::Value,
    },
    Event {
        event_id: EventId,
        data: serde_json::Value,
    },
    Recreation {
        time: Timestamp,
    },
}
